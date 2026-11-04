# fuse4dbricks

A filesystem in userspace for mounting the Unity Catalog from Databricks.

## Disclaimer

This is not an official databricks package. I, the author of this package, am not affiliated to Databricks. My capacity to support this package is very limited or none. I may review issues and pull requests but I won't commit to timelines or features.

## Features

This filesystem uses the [public databricks API](https://docs.databricks.com/api/azure/workspace/introduction) to retrieve files, directories and access permissions from the Unity Catalog.

**Read and write** access is supported for Unity Catalog Volumes. Users with `WRITE_VOLUME` privilege can create, overwrite and delete files and directories, and can `mv`/rename and truncate files. Files above 5 GB are uploaded via multipart upload handled transparently by the Databricks SDK. Pass `--read-only` to disable all writes to Unity Catalog (useful when mounting a shared volume that should not be modified). `df` reports a (synthetic) capacity so tools that check free space before writing work.

**Restricting visibility** to a subset of securables is supported with `--securable-allowlist` and `--securable-denylist`. Each takes a comma-separated list of securables, written dotted: a catalog (`mycatalog`), a schema (`mycatalog.myschema`) or a volume (`mycatalog.myschema.myvol`). With a denylist, the listed securables (and everything beneath them) are hidden and inaccessible. With an allowlist, only the listed securables are listable and accessible — their parent catalog/schema stay navigable so you can reach them. If both are given, only allowlisted securables that are not also denied are accessible (deny wins). This is a mount-wide visibility filter layered on top of — not a replacement for — Unity Catalog's own per-user permission checks.

To mitigate latency and improve **performance**, file metadata is cached in-memory. Data is cached
to a local cache directory (`--disk-cache-dir`) and partially to RAM as well. Options to control
the sizes of those caches are available.

**Experimental: presigned downloads.** By default, file data is read through the Files API
(`GET /api/2.0/fs/files`), which proxies every byte through the Databricks workspace. With
`--use-presigned-downloads`, fuse4dbricks instead asks for a short-lived presigned URL
(`POST /api/2.0/fs/create-download-url`, the mechanism the Databricks SDK uses for
`files.download()`) and reads byte ranges directly from cloud storage, which can be
substantially faster. URLs are cached per user and file for up to 15 minutes, so revoking a
user's access may take that long to stop reads already in progress. If a presigned URL cannot
be created, or storage refuses a request, the read falls back to the Files API automatically.
This requires the client machine to reach the storage account directly (no firewall or
private-endpoint restriction blocking it).

**Credentials** are stored in RAM while the filesystem is mounted.

`DATABRICKS_TOKEN`, `DATABRICKS_CONFIG_PROFILE` and `DATABRICKS_CONFIG_FILE` are used to find the databricks configuration file and obtain a valid access token. Alternatively, credentials can be passed by writing a
personal access token to a virtual file:

    echo "dapi0000000-2" > /Volumes/.auth/personal_access_token


If fuse (`/etc/fuse.conf`) has `user_allow_other` activated, this driver supports the `--allow-other`,
option so **multiple users** can access it. In this case, the fuse4dbricks process should run from a root account, who should have exclusive access to `--disk-cache-dir`. fuse4dbricks will inspect the environment variables of the requesting process, as well as the requesting user, in order to find the access credentials. This may require reading the `.databrickscfg` file from the requesting user. The cache is shared among all users in this scenario.

On a **single-user machine** you can instead pass `--single-principal`, so one Databricks identity serves every request regardless of the requesting uid. The token is resolved from the fuse4dbricks process's own credentials (its environment and the launching user's `~/.databrickscfg`) or from a token written to `.auth` — never from the requesting process. This is especially useful under WSL: combine `--single-principal` with `--allow-other` so the Windows file explorer (whose requests arrive with an undocumented uid) can browse the mount using that single token, without fuse4dbricks needing root to inspect each requesting process.

When an access token is missing, revoked or expired, the unity catalog is not accessible anymore and only
a virtual `/Volumes/README.txt` file appears, with instructions on how to add the access token manually.

## Requirements

**This package depends on pyfuse3, that requires the `libfuse3-dev` or `fuse3-devel` system package
being installed. The package name depends on your specific linux distribution.**

- Debian / Ubuntu: `sudo apt install libfuse3-dev`
- RedHat / Fedora: `sudo dnf install fuse3-devel`
- SUSE: `sudo zypper install fuse3-devel`


## Quickstart

If you want to see databricks volumes as if you were on Databricks (in /Volumes) you will need admin
privileges to create /Volumes directory on your computer. Otherwise you can use $HOME/Volumes or whatever
directory you have access to.

    pip install "fuse4dbricks"
    mkdir "$HOME/Volumes" # or any other directory
    fuse4dbricks --workspace "https://adb-xxxx.azuredatabricks.net" $HOME/Volumes
    ls $HOME/Volumes
    # Your catalogs will appear

This uses databricks unified authentication (e.g. searches for `$HOME/.databrickscfg` in your home). If no unified auth file
is found, you will find a README file with instructions on how to provide a personal access token.

If you are the only user (e.g. on your laptop) and you have admin permissions, feel free to use `/Volumes` so paths are like on databricks:

    pip install "fuse4dbricks"
    sudo mkdir /Volumes"
	sudo chown $USER /Volumes
    fuse4dbricks --workspace "https://adb-xxxx.azuredatabricks.net" /Volumes
    ls /Volumes
    # Your catalogs will appear on /Volumes

## Quick start on WSL

This section covers mounting Databricks Unity Catalog volumes under
[Windows Subsystem for Linux (WSL)](https://learn.microsoft.com/en-us/windows/wsl/) and
browsing them directly from Windows Explorer.

### 1. Install system dependencies

On a WSL shell:

```bash
sudo apt update
sudo apt install -y fuse3 libfuse3-dev
```

### 2. Install fuse4dbricks

```bash
pip install "fuse4dbricks"
```

### 3. Enable `user_allow_other` in FUSE

Edit `/etc/fuse.conf` and uncomment the `user_allow_other` line:

```bash
sudo nano /etc/fuse.conf
```

Find the line `#user_allow_other` and remove the leading `#` so it reads:

```
user_allow_other
```

Save and close the file.

### 4. Set your Databricks credentials

Export your workspace URL and personal access token as environment variables:

```bash
export DATABRICKS_HOST="https://adb-xxxx.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi0000000-2"
```

Replace the values with your actual workspace URL and
[personal access token](https://docs.databricks.com/en/dev-tools/auth/pat.html).

### 5. Create the `/Volumes` mount point

Create the directory and give your user write access:

```bash
sudo mkdir -p /Volumes
sudo chown "$USER" /Volumes
```

### 6. Mount the filesystem

Run `fuse4dbricks` with `--single-principal` (your token is the only one in use) and
`--allow-other` (required for Windows Explorer to access the mount through WSL):

```bash
fuse4dbricks \
  --workspace "$DATABRICKS_HOST" \
  --single-principal \
  --allow-other \
  /Volumes
```

**Leave this terminal open.** The mount stays active as long as this process is running.

### 7. Verify the mount

Open a **new terminal** and check that your catalogs are visible:

```bash
ls /Volumes
```

You should see your Unity Catalog catalogs listed as directories. You can navigate into
them as you would any local filesystem:

```bash
ls /Volumes/<catalog>/<schema>/
```

### 8. Browse from Windows Explorer

WSL mounts are accessible from Windows as a network drive. In Windows Explorer:

1. Open **Windows Explorer** and look for **Linux** in the left-hand navigation panel
   (under "Network" or directly in the sidebar).
2. Navigate to your WSL distribution (e.g. `\\wsl.localhost\Ubuntu`).
3. Browse to the `Volumes` directory — your Databricks catalogs, schemas, and files
   will appear just like any other folder.

> **Tip:** You can also pin `\\wsl.localhost\Ubuntu\Volumes` to Quick Access or map it
> as a network drive for faster access in the future.



## Multi user setup

If you are on a shared computer (e.g. an HPC), fuse4dbricks can run as a root service and use the access tokens from any user who
aims to access unity catalog volumes.

The databricks token used will depend on the user who asks for the file.

A cache is shared so if many users read the same files the access speed is faster. Users can't access other users files, because each
user has to provide its own access token.

- Create a virtual environment and install fuse4dbricks there:

      # Note that fuse4dbricks requires python>=3.11
	  sudo bash
      mkdir /opt/fuse4dbricks
      chmod 755 /opt/fuse4dbricks
      python3.11 -m venv /opt/fuse4dbricks/venv
      source /opt/fuse4dbricks/venv/bin/activate
      python3 -m pip install fuse4dbricks
      deactivate
	  exit # exit from sudo bash

- Create the mount directory:

      sudo mkdir /Volumes
      sudo chmod 0755 /Volumes

- Create the cache directory:

      sudo mkdir /var/cache/fuse4dbricks
      sudo chmod 0700 /var/cache/fuse4dbricks

- Enable `user_allow_other` support in `/etc/fuse.conf`

    Edit `/etc/fuse.conf` and uncomment the `user_allow_other` line.

- Create a starting script and make it executable:

    Please replace whatever you need here

      cat << EOF | sudo tee /opt/fuse4dbricks/fuse4dbricks_start.sh
      #!/bin/bash

      source /opt/fuse4dbricks/venv/bin/activate
      fuse4dbricks \
        --workspace "https://adb-xxxx.azuredatabricks.net" \
        --disk-cache-dir /var/cache/fuse4dbricks \
        --allow-other \
        --ram-cache-mb 512 \
        --disk-cache-gb 1024 \
        --disk-cache-max-days 30 \
        /Volumes
      EOF
      sudo chmod +x /opt/fuse4dbricks/fuse4dbricks_start.sh

- Create a systemd unit

      cat << EOF | sudo tee /etc/systemd/system/fuse4dbricks.service
      [Unit]
      Description=fuse4dbricks
      After=network.target

      [Service]
      Type=simple
      User=root
      WorkingDirectory=/opt/fuse4dbricks
      ExecStart=/opt/fuse4dbricks/fuse4dbricks_start.sh
      # On stop, fuse4dbricks catches SIGTERM and unmounts itself. Give it time
      # to finish before systemd escalates to SIGKILL (which cannot be caught
      # and would leave a stale mount).
      TimeoutStopSec=30
      # Belt-and-suspenders: if the process was hard-killed and left a stale
      # mount, force-unmount it. '-' ignores failure when nothing is mounted.
      ExecStopPost=-/bin/fusermount3 -uz /Volumes
      Restart=on-failure
      RestartSec=5

      [Install]
      WantedBy=multi-user.target
      EOF

  If your mountpoint is not `/Volumes`, update it in `ExecStopPost` to match.
  On systems that ship FUSE 2 rather than FUSE 3, use the absolute path to
  `fusermount` (e.g. `/usr/bin/fusermount`) instead of `fusermount3`.


- Reload the daemon lists

      sudo systemctl daemon-reload

- Enable and start the service

      sudo systemctl enable fuse4dbricks
      sudo systemctl start fuse4dbricks

## Known limitations

These are intentional design trade-offs in the current implementation. We are open to
discussing them, so feel free to open an issue if any of them is a problem for your use case.

- **The attribute cache is global.** File and directory metadata (names, sizes and
  modification times) is cached in memory and shared across all users of a single mount.
  As a consequence, names, sizes and modification times of files may leak between users,
  even between users who do not have access to read them. **File contents never leak**:
  reading data always goes through a per-user permission check against Unity Catalog using
  that user's own token. The metadata cache is shared for performance reasons, to avoid
  repeating Unity Catalog API calls for objects that several users access.

- **Users are mapped to tokens 1-to-1.** fuse4dbricks associates a single access token with
  each user (uid). It is therefore not possible for two processes belonging to the same user
  to access different Unity Catalog principals at the same time (for example by setting a
  different `DATABRICKS_CONFIG_PROFILE` per process). The first token resolved for a user is
  reused for all of that user's processes.

- **File owner and group are meaningless.** The user and group reported for a file or
  directory (`st_uid`/`st_gid`, what `ls -l` shows in the owner and group columns) are simply
  whoever happened to populate the shared metadata cache for that entry first, so the same
  file may appear owned by different users at different times. These values are irrelevant:
  access is never decided from POSIX ownership, only from a per-user Unity Catalog permission
  check. Do not rely on the reported owner or group for anything.

- **Not-found results are briefly cached.** When a path is looked up and does not exist, that
  "not found" answer is cached per user for a short time (`--metadata-cache-ttl-negative-sec`,
  5 seconds by default) to avoid repeatedly asking Unity Catalog about missing paths. As a
  result, a file created (or that becomes visible to you) just after you looked for it may not
  appear until this short TTL expires. Unlike file metadata, these negative results are *not*
  shared between users.

- **Revoked permissions can stay effective for a short time.** Catalog and
  schema access permissions are cached using `--metadata-cache-ttl-catalog-sec`,
  set to 10 minutes (600 seconds) by default. Volume access permissions are
  cached using `--metadata-cache-ttl-sec`, set to 30 seconds by default. This
  reduces the request load on the Unity Catalog API, improving performance.

- **Writes are object-store semantics, not POSIX semantics.** The Databricks Files API is a
  full-replace store: a file is not visible to other processes until the writing process closes
  it (`release`). Concurrent writers follow last-write-wins — there is no locking. Deleting a
  file while another process has it open will cause that process to receive an I/O error on its
  next network fetch. These are intentional trade-offs for performance on object storage.

- **Opening a large file O_RDWR without O_TRUNC downloads the entire file first.** Because
  writes are buffered locally and uploaded on close, the full existing content must be
  downloaded into a temporary file before the open returns. For very large files (multi-GB)
  this can be slow. Use `O_WRONLY` or `O_RDWR | O_TRUNC` when you intend to overwrite the
  file completely.

- **Renaming a file copies it.** The Databricks Files API has no server-side move, so renaming
  a file downloads it and re-uploads it under the new name, then deletes the original. This is
  cheap for the write-temp-then-rename pattern editors use, but expensive for large files.
  Renaming a *directory* is not supported and returns `EXDEV`, which makes `mv` fall back to a
  recursive copy.

- **Securable allow/deny is a mount-wide visibility filter, not a per-user boundary.**
  `--securable-allowlist`/`--securable-denylist` hide securables for everyone using the mount;
  they are not per-user and do not replace Unity Catalog permissions. A user still needs the
  relevant Unity Catalog privileges to read data that the filter allows.
