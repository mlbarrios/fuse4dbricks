"""
Async client for Databricks Unity Catalog & Files API (v2.0).
Handles pagination, robust authentication, and asynchronous streaming for FUSE.
"""

import logging
import os
import random
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import formatdate, parsedate_to_datetime
from enum import Enum
from typing import AsyncGenerator

import httpx
import pyfuse3
import trio

from fuse4dbricks.api.errors import (
    UcBadRequest,
    UcConflict,
    UcError,
    UcNotFound,
    UcPermissionDenied,
    UcPreconditionFailed,
    UcRateLimited,
    UcUnavailable,
)
from fuse4dbricks.auth.provider import AuthProvider

logger = logging.getLogger(__name__)

try:
    from databricks.sdk.errors.platform import (
        TooManyRequests as _SdkTooManyRequests,
        TemporarilyUnavailable as _SdkTemporarilyUnavailable,
        InternalError as _SdkInternalError,
        DeadlineExceeded as _SdkDeadlineExceeded,
    )
    _SDK_TRANSIENT_ERRORS: tuple = (
        _SdkTooManyRequests,
        _SdkTemporarilyUnavailable,
        _SdkInternalError,
        _SdkDeadlineExceeded,
    )
except ImportError:
    _SDK_TRANSIENT_ERRORS = ()

try:
    from databricks.sdk.errors.platform import Unauthenticated as _SdkUnauthenticated
    _SDK_AUTH_ERRORS: tuple = (_SdkUnauthenticated,)
except ImportError:
    _SDK_AUTH_ERRORS = ()

# Transient failures are retried with exponential backoff and jitter:
# 429 (rate limited), 5xx (server unavailable) and connection-level errors.
_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
_DEFAULT_MAX_RETRIES = 4
_BACKOFF_BASE_S = 0.5
_BACKOFF_CAP_S = 20.0


def _parse_retry_after(value: str | None) -> float | None:
    """Parse a Retry-After header into seconds-from-now.

    Per RFC 7231 the header is either a non-negative integer number of seconds
    or an HTTP date. Both forms are honored; the date form is converted to a
    delay relative to now (clamped at 0). Returns None if unparseable.
    """
    if not value:
        return None
    value = value.strip()
    try:
        return float(value)
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    # parsedate_to_datetime may return a naive datetime for a malformed zone;
    # assume UTC in that case so the subtraction is well-defined.
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    delay = (when - datetime.now(timezone.utc)).total_seconds()
    return max(0.0, delay)


def _backoff_delay(attempt: int, retry_after_s: float | None) -> float:
    """Seconds to wait before retry ``attempt`` (0-based).

    Honors a server-provided Retry-After when present, otherwise uses
    exponential backoff with full jitter, capped at ``_BACKOFF_CAP_S``.
    """
    if retry_after_s is not None:
        return retry_after_s
    ceiling = min(_BACKOFF_CAP_S, _BACKOFF_BASE_S * (2**attempt))
    return random.uniform(0.0, ceiling)


class UcNodeType(Enum):
    """Unity Catalog node types, with the root node"""

    ROOT = 0
    CATALOG = 1
    SCHEMA = 2
    VOLUME = 3
    DIRECTORY = 4
    FILE = 5


@dataclass
class UnityCatalogEntry:
    name: str
    uc_path: str
    "The databricks path /Volumes/catalog/schema..."
    entry_type: UcNodeType
    size: int = 0
    ctime: float | None = None
    mtime: float | None = None

    def is_dir(self):
        return self.entry_type != UcNodeType.FILE


class UnityCatalogClient:
    def __init__(self, workspace_url: str, auth_provider: AuthProvider):
        self.base_url = workspace_url.rstrip("/")
        self.auth_provider = auth_provider

        # Connect timeout is short to fail fast, read timeout longer for large chunks
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=30.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
            follow_redirects=True,
        )

    async def close(self):
        await self.client.aclose()

    async def _with_retry(self, operation, *, uc_path: str | None = None, max_retries: int = _DEFAULT_MAX_RETRIES):
        """
        Retry an async callable with exponential backoff and jitter.

        Handles transient errors from both the httpx path (UcRateLimited,
        UcUnavailable) and the databricks-sdk path (TooManyRequests,
        TemporarilyUnavailable, InternalError, DeadlineExceeded).
        """
        attempt = 0
        while True:
            try:
                return await operation()
            except (UcRateLimited, UcUnavailable) as exc:
                if attempt >= max_retries:
                    raise
                retry_after_s = getattr(exc, "retry_after_s", None)
                delay = _backoff_delay(attempt, retry_after_s)
                logger.warning(
                    "Transient error %s (attempt %d/%d), retrying in %.1fs",
                    exc, attempt + 1, max_retries, delay,
                )
                await trio.sleep(delay)
                attempt += 1
            except Exception as exc:
                if _SDK_TRANSIENT_ERRORS and isinstance(exc, _SDK_TRANSIENT_ERRORS):
                    if attempt >= max_retries:
                        raise
                    retry_after_s = getattr(exc, "retry_after_secs", None)
                    delay = _backoff_delay(attempt, retry_after_s)
                    logger.warning(
                        "SDK transient error %s (attempt %d/%d), retrying in %.1fs",
                        exc, attempt + 1, max_retries, delay,
                    )
                    await trio.sleep(delay)
                    attempt += 1
                else:
                    raise

    async def _get_headers(self, ctx: pyfuse3.RequestContext) -> dict[str, str]:
        token = await self.auth_provider.get_access_token(ctx=ctx)
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, application/octet-stream",
        }

    def _raise_for_status(self, resp: httpx.Response, *, uc_path: str | None = None) -> None:
        if resp.status_code < 400:
            return

        request_id = resp.headers.get("x-databricks-request-id") or resp.headers.get("x-request-id")
        url = str(resp.request.url)
        msg = f"Databricks API error {resp.status_code}"

        if resp.status_code in (401, 403):
            # 401 tends to mean token invalid/expired; 403 means forbidden.
            # The auth refresh logic already handles 401; after retry, treat as permission issue.
            raise UcPermissionDenied(msg, status_code=resp.status_code, uc_path=uc_path, url=url, request_id=request_id)

        if resp.status_code == 404:
            raise UcNotFound(msg, status_code=404, uc_path=uc_path, url=url, request_id=request_id)

        if resp.status_code == 409:
            raise UcConflict(msg, status_code=409, uc_path=uc_path, url=url, request_id=request_id)

        if resp.status_code == 400:
            raise UcBadRequest(msg, status_code=400, uc_path=uc_path, url=url, request_id=request_id)

        if resp.status_code == 412:
            # Precondition failed: the file changed under an If-Unmodified-Since
            # read, so the caller's cached size/mtime view is stale.
            raise UcPreconditionFailed(msg, status_code=412, uc_path=uc_path, url=url, request_id=request_id)

        if resp.status_code == 429:
            retry_after_s = _parse_retry_after(resp.headers.get("retry-after"))
            raise UcRateLimited(msg, status_code=429, uc_path=uc_path, url=url, request_id=request_id, retry_after_s=retry_after_s)

        if resp.status_code in (500, 502, 503, 504):
            raise UcUnavailable(msg, status_code=resp.status_code, uc_path=uc_path, url=url, request_id=request_id)

        raise UcError(msg, status_code=resp.status_code, uc_path=uc_path, url=url, request_id=request_id)


    async def _request(
        self, method, url, *, ctx: pyfuse3.RequestContext, params=None, headers=None,
        stream=False, uc_path=None, max_retries: int = _DEFAULT_MAX_RETRIES,
        raise_on_not_found: bool = False,
    ):
        """
        Internal wrapper to handle Authentication and Token Refreshing automatically.

        Transient failures (429 rate limits, 5xx server errors and connection
        errors) are retried up to ``max_retries`` times with exponential backoff,
        honoring a ``Retry-After`` header on 429 responses when present.

        By default a 404 response is returned as ``None`` so that read-path
        callers can treat "not found" as an expected outcome. Pass
        ``raise_on_not_found=True`` for write operations where 404 is an error
        (distinguishes it from a 200 with an empty body, which also returns
        ``None`` by default).
        """
        if headers is None:
            headers = {}

        base_headers = await self._get_headers(ctx=ctx)
        headers.update(base_headers)

        attempt = 0
        while True:
            request = self.client.build_request(method, url, params=params, headers=headers)

            try:
                response = await self.client.send(request, stream=stream)
            except httpx.TransportError as e:
                # Connection-level errors (connect/read timeouts, resets) are transient.
                if attempt < max_retries:
                    delay = _backoff_delay(attempt, None)
                    logger.warning(
                        f"Transport error to {url}: {e}. "
                        f"Retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})."
                    )
                    await trio.sleep(delay)
                    attempt += 1
                    continue
                logger.error(f"Connection error to {url}: {e}")
                raise

            # 401 Retry Logic (Token Expiration)
            if response.status_code == 401:
                logger.warning("Token expired (401). Refreshing and retrying...")
                self.auth_provider.invalidate_access_token(ctx=ctx)
                await self.auth_provider.get_access_token(ctx=ctx)

                headers.update(await self._get_headers(ctx=ctx))
                # For stream=True, we must ensure the first response is closed before retrying
                if stream:
                    await response.aclose()

                request = self.client.build_request(
                    method, url, params=params, headers=headers
                )
                response = await self.client.send(request, stream=stream)

            # Transient server-side failures: back off and retry.
            if response.status_code in _RETRY_STATUS and attempt < max_retries:
                retry_after_s = (
                    _parse_retry_after(response.headers.get("retry-after"))
                    if response.status_code == 429
                    else None
                )
                # Release the connection before sleeping/retrying.
                if stream:
                    await response.aclose()
                delay = _backoff_delay(attempt, retry_after_s)
                logger.warning(
                    f"Transient error {response.status_code} for {url}. "
                    f"Retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})."
                )
                await trio.sleep(delay)
                attempt += 1
                continue

            if not stream:
                if response.status_code == 404:
                    if raise_on_not_found:
                        raise UcNotFound(
                            f"Not found: {uc_path or url}",
                            status_code=404,
                            uc_path=uc_path,
                        )
                    return None
                self._raise_for_status(response, uc_path=uc_path)

                if method == "HEAD":
                    return response
                if response.content:
                    try:
                        return response.json()
                    except Exception:
                        return None
                return None

            # For streams, we return the response object to be used in a context manager
            if response.status_code >= 400:
                await response.aclose()
                self._raise_for_status(response, uc_path=uc_path)
            return response

    # --- Discovery Layer (Pagination Support) ---

    async def _fetch_all_pages(self, endpoint, key, *, ctx: pyfuse3.RequestContext, params=None, uc_path=None):
        if params is None:
            params = {}
        results = []
        next_token = None

        while True:
            if next_token:
                params["page_token"] = next_token

            data = await self._request("GET", endpoint, ctx=ctx, params=params, uc_path=uc_path)
            if not data:
                break

            results.extend(data.get(key, []))
            next_token = data.get("next_page_token")
            if not next_token:
                break
        return results

    async def _get_catalogs(self, ctx: pyfuse3.RequestContext) -> list[UnityCatalogEntry]:
        catalogs = await self._fetch_all_pages(
            "/api/2.1/unity-catalog/catalogs", "catalogs", ctx=ctx,
            uc_path="/Volumes",
        )
        return [
            UnityCatalogEntry(
                name=x["name"],
                uc_path="/Volumes/" + x["name"],
                entry_type=UcNodeType.CATALOG,
                ctime=float(x["created_at"]) / 1000.0,
                mtime=float(x["updated_at"]) / 1000.0,
            )
            for x in catalogs
        ]

    async def get_current_user_info(self, ctx: pyfuse3.RequestContext) -> str:
        endpoint = "/api/2.0/preview/scim/v2/Me"
        resp = await self._request("GET", endpoint, ctx=ctx)
        return resp["userName"]

    async def check_permissions(self, securable: str, securable_type: str,
                                privileges: list[str], principal: str, ctx: pyfuse3.RequestContext) -> bool:
        endpoint = f"/api/2.1/unity-catalog/effective-permissions/{securable_type}/{securable}"
        priv_assignments = await self._fetch_all_pages(
            endpoint, "privilege_assignments", ctx=ctx,
            params={"principal": principal},
            uc_path=securable,
        )
        all_privs = set()
        for priv_assign in priv_assignments:
            for privilege in priv_assign["privileges"]:
                all_privs.add(privilege["privilege"])
        return set(privileges).issubset(all_privs)


    async def _get_catalog(self, catalog_name: str, ctx: pyfuse3.RequestContext) -> UnityCatalogEntry | None:
        endpoint = f"/api/2.1/unity-catalog/catalogs/{catalog_name}"
        catalog = await self._request("GET", endpoint, ctx=ctx, uc_path=f"/Volumes/{catalog_name}")
        if not catalog:
            return None
        return UnityCatalogEntry(
            name=catalog["name"],
            uc_path="/Volumes/" + catalog["name"],
            entry_type=UcNodeType.CATALOG,
            ctime=float(catalog["created_at"]) / 1000.0,
            mtime=float(catalog["updated_at"]) / 1000.0,
        )

    async def _get_schemas(self, catalog_name: str, ctx: pyfuse3.RequestContext) -> list[UnityCatalogEntry]:
        schemas = await self._fetch_all_pages(
            "/api/2.1/unity-catalog/schemas",
            "schemas",
            ctx=ctx,
            params={"catalog_name": catalog_name},
            uc_path=f"/Volumes/{catalog_name}",
        )
        return [
            UnityCatalogEntry(
                name=x["name"],
                uc_path="/Volumes/" + x["catalog_name"] + "/" + x["name"],
                entry_type=UcNodeType.SCHEMA,
                ctime=float(x["created_at"]) / 1000.0,
                mtime=float(x["updated_at"]) / 1000.0,
            )
            for x in schemas
        ]

    async def _get_schema(self, catalog_name, schema_name, ctx: pyfuse3.RequestContext) -> UnityCatalogEntry | None:
        endpoint = f"/api/2.1/unity-catalog/schemas/{catalog_name}.{schema_name}"
        schema = await self._request("GET", endpoint,
            ctx=ctx,
            uc_path=f"/Volumes/{catalog_name}/{schema_name}"
        )

        if not schema:
            return None
        return UnityCatalogEntry(
            name=schema["name"],
            uc_path="/Volumes/" + schema["catalog_name"] + "/" + schema["name"],
            entry_type=UcNodeType.SCHEMA,
            ctime=float(schema["created_at"]) / 1000.0,
            mtime=float(schema["updated_at"]) / 1000.0,
        )

    async def _get_volumes(self, catalog_name, schema_name, ctx: pyfuse3.RequestContext):
        volumes = await self._fetch_all_pages(
            "/api/2.1/unity-catalog/volumes",
            "volumes",
            ctx=ctx,
            params={"catalog_name": catalog_name, "schema_name": schema_name},
            uc_path=f"/Volumes/{catalog_name}/{schema_name}"
        )
        return [
            UnityCatalogEntry(
                name=x["name"],
                uc_path="/Volumes/"
                + x["catalog_name"]
                + "/"
                + x["schema_name"]
                + "/"
                + x["name"],
                entry_type=UcNodeType.VOLUME,
                ctime=float(x["created_at"]) / 1000.0,
                mtime=float(x["updated_at"]) / 1000.0,
            )
            for x in volumes
        ]

    async def _get_volume(
        self, catalog_name, schema_name, volume_name,
        ctx: pyfuse3.RequestContext,
    ) -> UnityCatalogEntry | None:
        endpoint = (
            f"/api/2.1/unity-catalog/volumes/{catalog_name}.{schema_name}.{volume_name}"
        )
        volume = await self._request("GET", endpoint,
            ctx=ctx,
            uc_path=f"/Volumes/{catalog_name}/{schema_name}/{volume_name}",
        )
        if not volume:
            return None
        return UnityCatalogEntry(
            name=volume["name"],
            uc_path="/Volumes/"
            + volume["catalog_name"]
            + "/"
            + volume["schema_name"]
            + "/"
            + volume["name"],
            entry_type=UcNodeType.VOLUME,
            ctime=float(volume["created_at"]) / 1000.0,
            mtime=float(volume["updated_at"]) / 1000.0,
        )

    # --- File System Layer (Files API 2.0) ---

    def _quote_path(self, path):
        if not path.startswith("/"):
            path = "/" + path
        return urllib.parse.quote(path)

    async def _get_file_metadata(self, path: str, ctx: pyfuse3.RequestContext) -> UnityCatalogEntry | None:
        """HEAD request for size/mtime."""
        encoded_path = self._quote_path(path)
        endpoint = f"/api/2.0/fs/files{encoded_path}"

        response = await self._request("HEAD", endpoint, ctx=ctx, uc_path=path)
        if response is None:
            return None

        size = int(response.headers.get("Content-Length", 0))
        last_modified = response.headers.get("Last-Modified")

        mtime = 0.0
        if last_modified:
            try:
                dt = parsedate_to_datetime(last_modified)
                mtime = dt.timestamp()
            except Exception:
                logger.warning(f"Failed to parse Last-Modified: {last_modified}")
                pass
        return UnityCatalogEntry(
            name=os.path.basename(path),
            uc_path=path,
            entry_type=UcNodeType.FILE,
            size=size,
            mtime=mtime,
        )

    async def _get_directory_metadata(self, path: str, ctx: pyfuse3.RequestContext) -> UnityCatalogEntry | None:
        """HEAD request, checks for existance."""
        encoded_path = self._quote_path(path)
        endpoint = f"/api/2.0/fs/directories{encoded_path}"

        response = await self._request("HEAD", endpoint, ctx=ctx, uc_path=path)
        if response is None:
            return None
        return UnityCatalogEntry(
            name=os.path.basename(path),
            uc_path=path,
            entry_type=UcNodeType.DIRECTORY,
        )

    async def get_path_metadata(
        self, uc_path: str, *, ctx: pyfuse3.RequestContext, expected_type: UcNodeType | None = None
    ) -> UnityCatalogEntry | None:
        """
        Gets the catalog entry. None if not found. expected_type is optional

        :param uc_path: The /Volumes/catalog/Schema/volume/path/to/file.txt
        :param expected_type: You may set this to UcNodeType.DIRECTORY if you expect the path to be a
           folder. You save one request if you do it. defaults to None.
        :raises ValueError: When the path is not valid
        :return: A UnityCatalogEntry holding path metadata
        """
        logger.debug(f"uc_client.get_path_metadata for {uc_path}")
        if not uc_path.startswith("/"):
            raise ValueError("uc_path should start with /Volumes")
        parts = uc_path.split("/")
        if len(parts) < 2 or parts[1] != "Volumes":
            raise ValueError("uc_path should start with /Volumes")
        if len(parts) == 2:
            raise ValueError("'/Volumes' is not a unity catalog entry.")
        catalog = parts[2]
        if len(parts) == 3:
            return await self._get_catalog(catalog, ctx=ctx)
        schema = parts[3]
        if len(parts) == 4:
            return await self._get_schema(catalog, schema, ctx=ctx)
        volume = parts[4]
        if len(parts) == 5:
            return await self._get_volume(catalog, schema, volume, ctx=ctx)
        if expected_type == UcNodeType.DIRECTORY:
            entry = await self._get_directory_metadata(uc_path, ctx=ctx)
            if entry is None:
                # File not found, check if that path is now a directory (because
                # someone created a folder with that same name)
                entry = await self._get_file_metadata(uc_path, ctx=ctx)
                # Returns either the folder or None if not found
                return entry
            return entry
        entry = await self._get_file_metadata(uc_path, ctx=ctx)
        if entry is None:
            entry = await self._get_directory_metadata(uc_path, ctx=ctx)
        return entry

    async def _list_directory_contents(
        self, path, *, ctx: pyfuse3.RequestContext, limit=None
    ) -> list[UnityCatalogEntry] | None:
        """Lists directory contents, or None if not a directory"""
        encoded_path = self._quote_path(path)
        endpoint = f"/api/2.0/fs/directories{encoded_path}"

        results = []
        next_token = None
        first_page = True
        while True:
            params: dict[str, str] = {}
            if next_token:
                params["page_token"] = next_token

            data = await self._request("GET", endpoint, params=params, ctx=ctx, uc_path=path)
            if first_page and data is None:
                return None  # Possibly not a directory
            if data is None:
                break  # Possibly an empty directory (or no new files)
            first_page = False
            contents = data.get("contents", [])
            for x in contents:
                results.append(
                    UnityCatalogEntry(
                        name=x["name"],
                        uc_path=x["path"],
                        entry_type=(
                            UcNodeType.DIRECTORY
                            if x["is_directory"]
                            else UcNodeType.FILE
                        ),
                        size=x.get("file_size", 0),
                        mtime=float(x.get("last_modified", 0.0)) / 1000.0,
                    )
                )
            next_token = data.get("next_page_token")
            if not next_token:
                break
            if limit is not None and len(results) >= limit:
                break
        return results

    async def get_path_contents(self, uc_path: str, *, ctx: pyfuse3.RequestContext) -> list[UnityCatalogEntry] | None:
        """
        Lists the path contents, or returns None if not a catalog/schema/volume/directory

        :param uc_path: The unity catalog path /Volumes/catalog/schema/...
        :raises ValueError: path is malformed
        :return: A list of unity catalog entries with the metadata of the path contents
        """
        logger.debug(f"uc_client.get_path_contents for {uc_path}")
        if not uc_path.startswith("/"):
            raise ValueError("path should start with /Volumes")
        parts = uc_path.split("/")
        if len(parts) < 2 or parts[1] != "Volumes":
            raise ValueError("path should start with /Volumes")
        if len(parts) == 2:
            return await self._get_catalogs(ctx=ctx)
        catalog = parts[2]
        if len(parts) == 3:
            return await self._get_schemas(catalog_name=catalog, ctx=ctx)
        schema = parts[3]
        if len(parts) == 4:
            return await self._get_volumes(catalog, schema, ctx=ctx)
        return await self._list_directory_contents(uc_path, ctx=ctx)

    # --- Write Layer (Files API 2.0 + SDK for multipart) ---

    async def upload_file(
        self,
        uc_path: str,
        local_path: str,
        *,
        overwrite: bool = True,
        ctx: pyfuse3.RequestContext,
    ) -> None:
        """Upload a local file to Unity Catalog via the Databricks SDK.

        Uses databricks-sdk so multipart upload is engaged automatically for
        files above the SDK's internal threshold (>5 GB). WorkspaceClient is
        cheap to construct: it makes no API calls at instantiation time.
        """
        base_url = self.base_url

        async def _upload_with_token(token: str):
            def _do_upload():
                from databricks.sdk import WorkspaceClient
                from databricks.sdk.config import Config
                w = WorkspaceClient(config=Config(host=base_url, token=token))
                with open(local_path, "rb") as f:
                    w.files.upload(file_path=uc_path, contents=f, overwrite=overwrite)
            await trio.to_thread.run_sync(_do_upload)

        async def _attempt():
            # Resolve the token inside the attempt so a refresh takes effect on
            # retry. The SDK path doesn't go through _request's 401 handling, so
            # mirror it here: on an expired token, invalidate, re-resolve, retry
            # once. (Genuine 403s surface as PermissionDenied and propagate.)
            token = await self.auth_provider.get_access_token(ctx=ctx)
            try:
                await _upload_with_token(token)
            except _SDK_AUTH_ERRORS:
                self.auth_provider.invalidate_access_token(ctx=ctx)
                fresh = await self.auth_provider.get_access_token(ctx=ctx)
                await _upload_with_token(fresh)

        await self._with_retry(_attempt, uc_path=uc_path)

    async def delete_file(self, uc_path: str, *, ctx: pyfuse3.RequestContext) -> None:
        encoded_path = self._quote_path(uc_path)
        await self._request(
            "DELETE", f"/api/2.0/fs/files{encoded_path}",
            ctx=ctx, uc_path=uc_path, raise_on_not_found=True,
        )

    async def delete_directory(self, uc_path: str, *, ctx: pyfuse3.RequestContext) -> None:
        encoded_path = self._quote_path(uc_path)
        await self._request(
            "DELETE", f"/api/2.0/fs/directories{encoded_path}",
            ctx=ctx, uc_path=uc_path, raise_on_not_found=True,
        )

    async def create_directory(self, uc_path: str, *, ctx: pyfuse3.RequestContext) -> None:
        encoded_path = self._quote_path(uc_path)
        await self._request(
            "PUT", f"/api/2.0/fs/directories{encoded_path}", ctx=ctx, uc_path=uc_path
        )

    async def download_chunk_stream(
        self,
        path: str,
        offset: int,
        length: int,
        *,
        ctx: pyfuse3.RequestContext,
        if_unmodified_since: float | None = None,
    ) -> AsyncGenerator[bytes, None]:
        """
        Asynchronously streams binary data from Unity Catalog.
        Yields bytes in 64KB chunks to keep memory usage constant.
        """
        logger.debug(f"uc_client.download_chunk_stream for {path}")
        encoded_path = self._quote_path(path)
        endpoint = f"/api/2.0/fs/files{encoded_path}"

        headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
        if if_unmodified_since:
            headers["If-Unmodified-Since"] = formatdate(
                if_unmodified_since, usegmt=True
            )

        # We use _request with stream=True
        response = await self._request("GET", endpoint, ctx=ctx, headers=headers, stream=True, uc_path=path)

        try:
            # Yield data in increments of 64KB
            async for chunk in response.aiter_bytes(chunk_size=65536):
                yield chunk
        finally:
            # Crucial: close the stream to release the connection back to the pool
            await response.aclose()
