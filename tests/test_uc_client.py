from datetime import datetime, timezone
import logging
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import urllib3

import pyfuse3

from fuse4dbricks.api.uc_client import PresignedDownloadUrl, UnityCatalogClient, _parse_retry_after
from fuse4dbricks.api.errors import (
    UcBadRequest,
    UcError,
    UcNotFound,
    UcPermissionDenied,
    UcPreconditionFailed,
    UcRateLimited,
    UcUnavailable,
)

# --- FIXTURES ---


@pytest.fixture
def mock_auth_provider():
    """Mocks the EntraIDAuthProvider."""
    provider = MagicMock()
    provider.get_access_token = AsyncMock(return_value="fake_token_123")
    return provider


@pytest.fixture
def client(mock_auth_provider):
    """
    Creates the UnityCatalogClient with a mocked httpx.AsyncClient.
    """
    uc_client = UnityCatalogClient("https://test-workspace.net", mock_auth_provider)

    uc_client.client = AsyncMock(spec=httpx.AsyncClient)

    mock_request = MagicMock(spec=httpx.Request)
    uc_client.client.build_request.return_value = mock_request

    return uc_client


# --- TESTS: AUTHENTICATION & RETRIES ---


@pytest.mark.trio
async def test_request_success_200(client, mock_auth_provider):
    """Verify standard request flow: Auth header injection + Success."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"key": "value"}
    client.client.send.return_value = mock_response

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    result = await client._request("GET", "/test", ctx=ctx)

    assert result == {"key": "value"}
    mock_auth_provider.get_access_token.assert_awaited_with(ctx=ctx)
    # Verify headers
    call_kwargs = client.client.build_request.call_args[1]
    assert call_kwargs["headers"]["Authorization"] == "Bearer fake_token_123"


@pytest.mark.trio
async def test_request_401_retry_success(client, mock_auth_provider):
    """Verify that a 401 triggers a token refresh and a retry."""
    response_401 = MagicMock(spec=httpx.Response)
    response_401.status_code = 401

    response_200 = MagicMock(spec=httpx.Response)
    response_200.status_code = 200
    response_200.json.return_value = {"success": True}

    client.client.send.side_effect = [response_401, response_200]

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    result = await client._request("GET", "/test", ctx=ctx)

    assert result == {"success": True}
    assert client.client.send.call_count == 2
    mock_auth_provider.get_access_token.assert_any_call(ctx=ctx)

# --- TESTS: PATH & METADATA ---


def test_quote_path_logic(client):
    """Ensure paths are URL-encoded and normalized."""
    assert client._quote_path("folder/file.txt") == "/folder/file.txt"
    assert client._quote_path("space name") == "/space%20name"


@pytest.mark.trio
async def test_get_file_metadata_parsing(client):
    """Verify HEAD request and RFC 7231 Date Parsing."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.headers = {
        "Content-Length": "1024",
        "Last-Modified": "Sun, 01 Feb 2026 12:00:00 GMT",
    }
    client.client.send.return_value = mock_response

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    meta = await client._get_file_metadata("/data.csv", ctx=ctx)

    assert meta.size == 1024
    expected_dt = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert meta.mtime == expected_dt.timestamp()


@pytest.mark.trio
async def test_get_file_metadata_404(client):
    """Verify 404 returns None."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 404
    client.client.send.return_value = mock_response

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    result = await client._get_file_metadata("/missing", ctx=ctx)
    assert result is None


# --- TESTS: DOWNLOAD STREAM & CONSISTENCY ---


@pytest.mark.trio
async def test_download_chunk_stream_headers(client):
    """Verify Range header injection in streaming mode."""
    mock_response = AsyncMock(spec=httpx.Response)
    mock_response.status_code = 206

    async def mock_iter_bytes(chunk_size=None):
        yield b"chunk1"
        yield b"chunk2"

    mock_response.aiter_bytes = mock_iter_bytes

    client.client.send.return_value = mock_response

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    chunks = []
    async for chunk in client.download_chunk_stream("/file.bin", offset=0, length=100, ctx=ctx):
        chunks.append(chunk)

    assert b"".join(chunks) == b"chunk1chunk2"

    last_build_call = client.client.build_request.call_args_list[-1]
    headers = last_build_call[1]["headers"]
    assert headers["Range"] == "bytes=0-99"


@pytest.mark.trio
async def test_download_chunk_stream_412_precondition_failed(client):
    """If the file changed under the If-Unmodified-Since read (412), raise the
    specific UcPreconditionFailed so it can be mapped to ESTALE upstream."""
    mock_response = AsyncMock(spec=httpx.Response)
    mock_response.status_code = 412

    # UnityCatalogClient._raise_for_status reads these fields
    mock_response.headers = {}
    mock_response.request = MagicMock(spec=httpx.Request)
    mock_response.request.url = "https://test-workspace.net/api/2.0/fs/files/file.bin"

    # For stream=True, _request() will aclose() the response on error
    mock_response.aclose = AsyncMock()

    client.client.send.return_value = mock_response

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    with pytest.raises(UcPreconditionFailed) as excinfo:
        async for _ in client.download_chunk_stream("/file.bin", 0, 100, ctx=ctx):
            pass

    assert excinfo.value.status_code == 412
    mock_response.aclose.assert_awaited()


# --- TESTS: TRANSIENT ERROR RETRY / BACKOFF ---


def _error_response(status_code, headers=None):
    """Build a mock httpx.Response for an error status code."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.request = MagicMock(spec=httpx.Request)
    resp.request.url = "https://test-workspace.net/test"
    return resp


@pytest.fixture
def backoff(monkeypatch):
    """Record backoff decisions and collapse the wait to an instant checkpoint.

    Patches the module-level ``_backoff_delay`` (only the retry path calls it) so
    we avoid real sleeps without globally monkeypatching ``trio.sleep``. Each
    entry is the ``retry_after_s`` value passed for that retry.
    """
    retry_afters = []

    def fake_backoff(attempt, retry_after_s):
        retry_afters.append(retry_after_s)
        return 0.0  # trio.sleep(0.0) is an instant checkpoint

    monkeypatch.setattr("fuse4dbricks.api.uc_client._backoff_delay", fake_backoff)
    return retry_afters


@pytest.mark.trio
async def test_request_429_retries_then_succeeds(client, backoff):
    """A 429 is retried with backoff and then succeeds."""
    response_200 = MagicMock(spec=httpx.Response)
    response_200.status_code = 200
    response_200.json.return_value = {"ok": True}

    client.client.send.side_effect = [_error_response(429), response_200]

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    result = await client._request("GET", "/test", ctx=ctx)

    assert result == {"ok": True}
    assert client.client.send.call_count == 2
    assert len(backoff) == 1  # backed off once


@pytest.mark.trio
async def test_request_429_honors_retry_after(client, backoff):
    """A Retry-After header overrides the computed backoff delay."""
    response_200 = MagicMock(spec=httpx.Response)
    response_200.status_code = 200
    response_200.json.return_value = {"ok": True}

    client.client.send.side_effect = [
        _error_response(429, headers={"retry-after": "2"}),
        response_200,
    ]

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    result = await client._request("GET", "/test", ctx=ctx)

    assert result == {"ok": True}
    assert backoff == [2.0]


@pytest.mark.trio
async def test_request_5xx_retries_then_succeeds(client, backoff):
    """A 503 is retried with backoff and then succeeds."""
    response_200 = MagicMock(spec=httpx.Response)
    response_200.status_code = 200
    response_200.json.return_value = {"ok": True}

    client.client.send.side_effect = [_error_response(503), response_200]

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    result = await client._request("GET", "/test", ctx=ctx)

    assert result == {"ok": True}
    assert client.client.send.call_count == 2


@pytest.mark.trio
async def test_request_429_exhausts_retries_raises(client, backoff):
    """When retries are exhausted, the original UcRateLimited is raised."""
    client.client.send.side_effect = [
        _error_response(429, headers={"retry-after": "3"}) for _ in range(3)
    ]

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    with pytest.raises(UcRateLimited) as excinfo:
        await client._request("GET", "/test", ctx=ctx, max_retries=2)

    assert excinfo.value.status_code == 429
    assert excinfo.value.retry_after_s == 3.0
    # initial attempt + 2 retries
    assert client.client.send.call_count == 3
    assert len(backoff) == 2


@pytest.mark.trio
async def test_request_5xx_exhausts_retries_raises(client, backoff):
    """When retries are exhausted, a 5xx surfaces as UcUnavailable."""
    client.client.send.side_effect = [_error_response(500) for _ in range(2)]

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    with pytest.raises(UcUnavailable):
        await client._request("GET", "/test", ctx=ctx, max_retries=1)

    assert client.client.send.call_count == 2


@pytest.mark.trio
async def test_request_transport_error_retries_then_succeeds(client, backoff):
    """Connection-level errors are retried before surfacing."""
    response_200 = MagicMock(spec=httpx.Response)
    response_200.status_code = 200
    response_200.json.return_value = {"ok": True}

    client.client.send.side_effect = [
        httpx.ConnectError("boom"),
        response_200,
    ]

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    result = await client._request("GET", "/test", ctx=ctx)

    assert result == {"ok": True}
    assert client.client.send.call_count == 2


@pytest.mark.trio
async def test_request_transport_error_exhausts_retries_raises(client, backoff):
    """A persistent connection error is re-raised after exhausting retries."""
    client.client.send.side_effect = httpx.ConnectError("boom")

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    with pytest.raises(httpx.ConnectError):
        await client._request("GET", "/test", ctx=ctx, max_retries=2)

    assert client.client.send.call_count == 3


def test_uc_rate_limited_carries_retry_after():
    """Regression: UcRateLimited must accept retry_after_s in its constructor."""
    err = UcRateLimited("rate limited", status_code=429, retry_after_s=5.0)
    assert err.retry_after_s == 5.0
    assert err.status_code == 429


# --- TESTS: WRITE PRIMITIVES ---


@pytest.mark.trio
async def test_delete_file_success(client):
    """DELETE /files/{path} with 200 and empty body → no exception."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.content = b""
    client.client.send.return_value = mock_response

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    # Should not raise
    await client.delete_file("/Volumes/cat/sch/vol/file.txt", ctx=ctx)


@pytest.mark.trio
async def test_delete_file_not_found_raises_ucnotfound(client):
    """DELETE /files/{path} with 404 → _request returns None → UcNotFound."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 404
    client.client.send.return_value = mock_response

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    with pytest.raises(UcNotFound):
        await client.delete_file("/Volumes/cat/sch/vol/missing.txt", ctx=ctx)


@pytest.mark.trio
async def test_delete_directory_success(client):
    """DELETE /directories/{path} with 200 and empty body → no exception."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.content = b""
    client.client.send.return_value = mock_response

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    await client.delete_directory("/Volumes/cat/sch/vol/emptydir", ctx=ctx)


@pytest.mark.trio
async def test_delete_directory_non_empty_raises_ucbadrequest(client):
    """DELETE /directories/{path} on a non-empty directory → 400 → UcBadRequest.

    rmdir() catches UcBadRequest and maps it to ENOTEMPTY.
    """
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 400
    mock_response.headers = {}
    mock_response.request = MagicMock(spec=httpx.Request)
    mock_response.request.url = "https://test-workspace.net/api/2.0/fs/directories/Volumes/cat/sch/vol/nonempty"
    client.client.send.return_value = mock_response

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    with pytest.raises(UcBadRequest):
        await client.delete_directory("/Volumes/cat/sch/vol/nonempty", ctx=ctx)


@pytest.mark.trio
async def test_delete_directory_file_path_raises_ucnotfound(client):
    """DELETE /directories/{path} on a file path → expected 404 → UcNotFound.

    The Databricks API returns 404 when the /directories/ endpoint is called on
    a path that is a file (not a directory). This means rmdir() will see
    UcNotFound and raise ENOENT instead of the POSIX-correct ENOTDIR. This
    test documents the current behavior so any future API change is caught.
    """
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 404
    client.client.send.return_value = mock_response

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    with pytest.raises(UcNotFound):
        await client.delete_directory("/Volumes/cat/sch/vol/actually_a_file.txt", ctx=ctx)


@pytest.mark.trio
async def test_create_directory_success(client):
    """PUT /directories/{path} with 200 and empty body → no exception."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.content = b""
    client.client.send.return_value = mock_response

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    await client.create_directory("/Volumes/cat/sch/vol/newdir", ctx=ctx)


@pytest.mark.trio
async def test_create_directory_path_is_file_raises_ucbadrequest(client):
    """PUT /directories/{path} when path already exists as a file → 400 → UcBadRequest.

    Databricks returns 400 when asked to create a directory at a path that is
    already a file. mkdir() will see UcBadRequest and raise EINVAL. POSIX would
    prefer EEXIST, but that requires a pre-flight HEAD; this test documents the
    current behavior.
    """
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 400
    mock_response.headers = {}
    mock_response.request = MagicMock(spec=httpx.Request)
    mock_response.request.url = "https://test-workspace.net/api/2.0/fs/directories/Volumes/cat/sch/vol/file.txt"
    client.client.send.return_value = mock_response

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    with pytest.raises(UcBadRequest):
        await client.create_directory("/Volumes/cat/sch/vol/file.txt", ctx=ctx)


@pytest.mark.trio
async def test_create_directory_parent_is_file_raises_ucbadrequest(client):
    """PUT /directories/{path} when a parent component is a file → 400 → UcBadRequest.

    e.g. /vol/file.txt/subdir where file.txt is a regular file. Databricks
    returns 400 in this case. mkdir() will see UcBadRequest and raise EINVAL;
    POSIX would prefer ENOTDIR. This test documents the current behavior.
    """
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 400
    mock_response.headers = {}
    mock_response.request = MagicMock(spec=httpx.Request)
    mock_response.request.url = "https://test-workspace.net/api/2.0/fs/directories/Volumes/cat/sch/vol/file.txt/subdir"
    client.client.send.return_value = mock_response

    ctx = pyfuse3.RequestContext(uid=0, pid=1234, gid=0)
    with pytest.raises(UcBadRequest):
        await client.create_directory("/Volumes/cat/sch/vol/file.txt/subdir", ctx=ctx)

# --- TESTS: Retry-After parsing (both RFC 7231 forms) ---


def test_parse_retry_after_seconds():
    assert _parse_retry_after("120") == 120.0
    assert _parse_retry_after("  5 ") == 5.0


def test_parse_retry_after_http_date():
    from datetime import datetime, timedelta, timezone
    from email.utils import format_datetime

    future = datetime.now(timezone.utc) + timedelta(seconds=100)
    val = _parse_retry_after(format_datetime(future, usegmt=True))
    assert val is not None
    # ~100s from now, allowing for the small elapsed time during the test.
    assert 90.0 <= val <= 100.0


def test_parse_retry_after_past_date_clamps_to_zero():
    from datetime import datetime, timedelta, timezone
    from email.utils import format_datetime

    past = datetime.now(timezone.utc) - timedelta(seconds=100)
    assert _parse_retry_after(format_datetime(past, usegmt=True)) == 0.0


def test_parse_retry_after_unparseable_returns_none():
    assert _parse_retry_after(None) is None
    assert _parse_retry_after("") is None
    assert _parse_retry_after("not-a-date") is None


# --- TESTS: upload_file token refresh on 401 ---


@pytest.mark.trio
async def test_upload_file_refreshes_token_on_401(client, mock_auth_provider, tmp_path, monkeypatch):
    """An expired token during the SDK upload (which bypasses _request's 401
    flow) must be invalidated, re-resolved and retried once with a fresh token."""
    from databricks.sdk.errors.platform import Unauthenticated

    mock_auth_provider.get_access_token = AsyncMock(side_effect=["stale", "fresh"])
    mock_auth_provider.invalidate_access_token = MagicMock()

    seen_tokens = []

    class _FakeConfig:
        def __init__(self, host=None, token=None):
            self.host = host
            self.token = token

    class _FakeFiles:
        def __init__(self, token):
            self._token = token

        def upload(self, file_path, contents, overwrite):
            # The stale token is rejected; the refreshed one succeeds.
            if self._token == "stale":
                raise Unauthenticated("token expired")

    class _FakeWorkspaceClient:
        def __init__(self, config=None):
            seen_tokens.append(config.token)
            self.files = _FakeFiles(config.token)

    monkeypatch.setattr("databricks.sdk.config.Config", _FakeConfig)
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", _FakeWorkspaceClient)

    local = tmp_path / "payload.bin"
    local.write_bytes(b"data")
    ctx = pyfuse3.RequestContext(uid=0, pid=1, gid=0)

    await client.upload_file("/Volumes/c/s/v/payload.bin", str(local), ctx=ctx)

    assert seen_tokens == ["stale", "fresh"]
    mock_auth_provider.invalidate_access_token.assert_called_once()
    assert mock_auth_provider.get_access_token.await_count == 2


# --- TESTS: PRESIGNED (DIRECT-TO-STORAGE) DOWNLOADS ---

_SECRET_URL = "https://acct.blob.core.windows.net/c/blob?sv=2021&sig=SECRET-SIGNATURE"
_SECRET_URL_2 = "https://acct.blob.core.windows.net/c/blob?sv=2021&sig=SECRET-SIGNATURE-2"


def _presigned(url=_SECRET_URL, expires_in=600.0):
    return PresignedDownloadUrl(
        url=url,
        headers={"x-ms-version": "2021-04-10"},
        expires_at=time.monotonic() + expires_in,
    )


async def _collect(agen):
    return b"".join([piece async for piece in agen])


def _files_api_fallback(data=b"from-files-api"):
    """Replacement for download_chunk_stream recording its calls."""
    calls = []

    async def fake(path, offset, length, *, ctx, if_unmodified_since=None):
        calls.append((path, offset, length, if_unmodified_since))
        yield data

    return fake, calls


@pytest.mark.trio
async def test_get_download_url_request_and_parsing(client):
    """POST create-download-url with path/expire_time query params (as the SDK
    does) and parse the URL plus required headers."""
    response = MagicMock(spec=httpx.Response)
    response.status_code = 200
    response.json.return_value = {
        "url": _SECRET_URL,
        "headers": [{"name": "x-ms-version", "value": "2021-04-10"}],
    }
    client.client.send.return_value = response

    ctx = pyfuse3.RequestContext(uid=1000, pid=1234, gid=1000)
    presigned = await client.get_download_url("/Volumes/c/s/v/f.bin", ctx=ctx)

    method, endpoint = client.client.build_request.call_args[0]
    params = client.client.build_request.call_args[1]["params"]
    assert (method, endpoint) == ("POST", "/api/2.0/fs/create-download-url")
    assert params["path"] == "/Volumes/c/s/v/f.bin"
    datetime.strptime(params["expire_time"], "%Y-%m-%dT%H:%M:%SZ")
    assert presigned.url == _SECRET_URL
    assert presigned.headers == {"x-ms-version": "2021-04-10"}
    assert presigned.expires_at > time.monotonic()
    assert "SECRET" not in repr(presigned)


@pytest.mark.trio
async def test_get_download_url_rejects_response_without_url(client):
    response = MagicMock(spec=httpx.Response)
    response.status_code = 200
    response.json.return_value = {"headers": []}
    client.client.send.return_value = response

    ctx = pyfuse3.RequestContext(uid=1000, pid=1234, gid=1000)
    with pytest.raises(UcError):
        await client.get_download_url("/Volumes/c/s/v/f.bin", ctx=ctx)


@pytest.mark.trio
async def test_presigned_download_reads_range_from_storage_and_caches_url(client):
    client.get_download_url = AsyncMock(return_value=_presigned())
    client._storage_get_blocking = MagicMock(return_value=(206, b"x" * 100))
    fallback, fallback_calls = _files_api_fallback()
    client.download_chunk_stream = fallback

    ctx = pyfuse3.RequestContext(uid=1000, pid=1234, gid=1000)
    mtime = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    for _ in range(2):
        data = await _collect(client.download_chunk_stream_presigned(
            "/Volumes/c/s/v/f.bin", 8 * 1024 * 1024, 100, ctx=ctx, if_unmodified_since=mtime,
        ))
        assert data == b"x" * 100

    client.get_download_url.assert_awaited_once()  # second chunk reuses the cached URL
    assert fallback_calls == []
    _pool, url, headers, length, accept_200 = client._storage_get_blocking.call_args[0]
    assert url == _SECRET_URL
    assert headers["Range"] == "bytes=8388608-8388707"
    assert headers["If-Unmodified-Since"] == "Sun, 01 Feb 2026 12:00:00 GMT"
    assert headers["x-ms-version"] == "2021-04-10"
    assert "Authorization" not in headers  # the signature is the credential
    assert (length, accept_200) == (100, False)


@pytest.mark.trio
async def test_presigned_url_cache_is_per_uid(client):
    # pyfuse3.RequestContext() ignores its arguments (uid is always 0), so use
    # plain objects: the client only reads ctx.uid / ctx.pid.
    client.get_download_url = AsyncMock(return_value=_presigned())
    client._storage_get_blocking = MagicMock(return_value=(206, b"x" * 10))

    for uid in (1000, 1001, 1000):
        ctx = SimpleNamespace(uid=uid, pid=1234, gid=1000)
        await _collect(client.download_chunk_stream_presigned("/Volumes/c/s/v/f.bin", 0, 10, ctx=ctx))

    assert client.get_download_url.await_count == 2


@pytest.mark.trio
async def test_presigned_url_refreshed_when_expired(client):
    client.get_download_url = AsyncMock(
        side_effect=[_presigned(expires_in=-1.0), _presigned(_SECRET_URL_2)]
    )
    client._storage_get_blocking = MagicMock(return_value=(206, b"x" * 10))

    ctx = pyfuse3.RequestContext(uid=1000, pid=1234, gid=1000)
    for _ in range(2):
        await _collect(client.download_chunk_stream_presigned("/Volumes/c/s/v/f.bin", 0, 10, ctx=ctx))

    assert client.get_download_url.await_count == 2
    assert client._storage_get_blocking.call_args[0][1] == _SECRET_URL_2


@pytest.mark.trio
async def test_presigned_403_refreshes_url_once_then_succeeds(client):
    client.get_download_url = AsyncMock(side_effect=[_presigned(), _presigned(_SECRET_URL_2)])
    client._storage_get_blocking = MagicMock(side_effect=[(403, b""), (206, b"x" * 10)])
    fallback, fallback_calls = _files_api_fallback()
    client.download_chunk_stream = fallback

    ctx = pyfuse3.RequestContext(uid=1000, pid=1234, gid=1000)
    data = await _collect(client.download_chunk_stream_presigned("/Volumes/c/s/v/f.bin", 0, 10, ctx=ctx))

    assert data == b"x" * 10
    assert client.get_download_url.await_count == 2
    assert client._storage_get_blocking.call_args_list[1][0][1] == _SECRET_URL_2
    assert fallback_calls == []


@pytest.mark.trio
async def test_presigned_repeated_403_falls_back_without_logging_url(client, caplog):
    caplog.set_level(logging.DEBUG)
    client.get_download_url = AsyncMock(side_effect=[_presigned(), _presigned(_SECRET_URL_2)])
    client._storage_get_blocking = MagicMock(return_value=(403, b""))
    fallback, fallback_calls = _files_api_fallback()
    client.download_chunk_stream = fallback

    ctx = pyfuse3.RequestContext(uid=1000, pid=1234, gid=1000)
    data = await _collect(client.download_chunk_stream_presigned("/Volumes/c/s/v/f.bin", 0, 10, ctx=ctx))

    assert data == b"from-files-api"
    assert fallback_calls == [("/Volumes/c/s/v/f.bin", 0, 10, None)]
    assert "SECRET" not in caplog.text
    assert "using the Files API" in caplog.text


@pytest.mark.trio
async def test_presigned_falls_back_when_create_download_url_fails(client):
    client.get_download_url = AsyncMock(
        side_effect=UcPermissionDenied("Databricks API error 403", status_code=403)
    )
    client._storage_get_blocking = MagicMock()
    fallback, fallback_calls = _files_api_fallback()
    client.download_chunk_stream = fallback

    ctx = pyfuse3.RequestContext(uid=1000, pid=1234, gid=1000)
    for offset in (0, 10):
        data = await _collect(client.download_chunk_stream_presigned("/Volumes/c/s/v/f.bin", offset, 10, ctx=ctx))
        assert data == b"from-files-api"

    # The failure is remembered: the second chunk goes straight to the Files API.
    client.get_download_url.assert_awaited_once()
    client._storage_get_blocking.assert_not_called()
    assert [call[1] for call in fallback_calls] == [0, 10]


@pytest.mark.trio
async def test_presigned_412_defers_to_files_api(client):
    """Storage's Last-Modified may not match Unity Catalog's mtime exactly, so a
    412 is re-checked through the Files API, which raises UcPreconditionFailed
    if the file really changed."""
    client.get_download_url = AsyncMock(return_value=_presigned())
    client._storage_get_blocking = MagicMock(return_value=(412, b""))
    fallback, fallback_calls = _files_api_fallback()
    client.download_chunk_stream = fallback

    ctx = pyfuse3.RequestContext(uid=1000, pid=1234, gid=1000)
    data = await _collect(client.download_chunk_stream_presigned(
        "/Volumes/c/s/v/f.bin", 0, 10, ctx=ctx, if_unmodified_since=1.0
    ))

    assert data == b"from-files-api"
    assert fallback_calls == [("/Volumes/c/s/v/f.bin", 0, 10, 1.0)]
    # A 412 is about this file version, not about presigned URLs: not remembered.
    assert client._presigned_failures == {}


@pytest.mark.trio
async def test_presigned_retries_transient_errors_and_short_reads(client, backoff):
    client.get_download_url = AsyncMock(return_value=_presigned())
    client._storage_get_blocking = MagicMock(
        side_effect=[
            (503, b""),
            urllib3.exceptions.ReadTimeoutError(None, _SECRET_URL, "read timed out"),
            (206, b"x" * 5),  # short read
            (206, b"x" * 10),
        ]
    )

    ctx = pyfuse3.RequestContext(uid=1000, pid=1234, gid=1000)
    data = await _collect(client.download_chunk_stream_presigned("/Volumes/c/s/v/f.bin", 0, 10, ctx=ctx))

    assert data == b"x" * 10
    assert len(backoff) == 3


@pytest.mark.trio
async def test_presigned_200_only_accepted_at_offset_zero(client):
    """A 200 means storage ignored Range and sent the blob from its start:
    usable for a chunk at offset 0, wrong data for any other chunk."""
    client.get_download_url = AsyncMock(return_value=_presigned())
    client._storage_get_blocking = MagicMock(return_value=(200, b"x" * 10))
    fallback, fallback_calls = _files_api_fallback()
    client.download_chunk_stream = fallback

    ctx = pyfuse3.RequestContext(uid=1000, pid=1234, gid=1000)
    assert await _collect(client.download_chunk_stream_presigned("/Volumes/c/s/v/f.bin", 0, 10, ctx=ctx)) == b"x" * 10

    data = await _collect(client.download_chunk_stream_presigned("/Volumes/c/s/v/f.bin", 10, 10, ctx=ctx))
    assert data == b"from-files-api"
    assert len(fallback_calls) == 1


def _storage_response(status, body=b"", closed=True):
    resp = MagicMock()
    resp.status = status
    resp.read.return_value = body
    resp.closed = closed
    return resp


def test_storage_get_blocking_reads_exactly_the_range():
    pool = MagicMock()
    pool.request.return_value = _storage_response(206, b"x" * 10)

    status, body = UnityCatalogClient._storage_get_blocking(
        pool, _SECRET_URL, {"Range": "bytes=0-9"}, 10, False
    )

    assert (status, body) == (206, b"x" * 10)
    pool.request.return_value.read.assert_called_once_with(10)
    kwargs = pool.request.call_args[1]
    assert kwargs["preload_content"] is False
    assert kwargs["redirect"] is False
    pool.request.return_value.close.assert_not_called()  # connection returned to the pool


def test_storage_get_blocking_drops_unread_error_response():
    pool = MagicMock()
    pool.request.return_value = _storage_response(403, closed=False)

    status, body = UnityCatalogClient._storage_get_blocking(
        pool, _SECRET_URL, {"Range": "bytes=10-19"}, 10, False
    )

    assert (status, body) == (403, b"")
    pool.request.return_value.read.assert_not_called()
    pool.request.return_value.close.assert_called_once()


def test_storage_pool_suppresses_urllib3_debug_request_lines(client):
    pool_logger = logging.getLogger("urllib3.connectionpool")
    previous = pool_logger.level
    pool_logger.setLevel(logging.DEBUG)
    try:
        client._get_storage_pool()
        assert pool_logger.level == logging.INFO
    finally:
        pool_logger.setLevel(previous)
