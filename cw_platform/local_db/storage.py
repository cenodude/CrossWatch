# cw_platform/local_db/storage.py
# CrossWatch - Filesystem detection for the local database
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

NETWORK_FILESYSTEMS = frozenset(
    {
        "afs",
        "ceph",
        "cifs",
        "fuse.glusterfs",
        "fuse.rclone",
        "fuse.sshfs",
        "glusterfs",
        "ncpfs",
        "nfs",
        "nfs4",
        "smb3",
        "smbfs",
    }
)


def _unescape(value: str) -> str:
    return value.replace("\\040", " ").replace("\\011", "\t").replace("\\012", "\n").replace("\\134", "\\")


def storage_info(path: str | os.PathLike[str], mounts_file: str | os.PathLike[str] = "/proc/mounts") -> dict[str, Any]:
    parent = Path(path).parent
    target = (Path(os.path.realpath(parent)) if os.name == "posix" else parent).as_posix()
    try:
        lines = Path(mounts_file).read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return {"filesystem": None, "mount_point": None, "network": False}
    best: tuple[str, str] | None = None
    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue
        mount_point, fstype = _unescape(parts[1]), parts[2]
        prefix = mount_point.rstrip("/")
        if mount_point == "/" or target == mount_point or target.startswith(prefix + "/"):
            if best is None or len(mount_point) >= len(best[0]):
                best = (mount_point, fstype)
    if best is None:
        return {"filesystem": None, "mount_point": None, "network": False}
    return {"filesystem": best[1], "mount_point": best[0], "network": best[1] in NETWORK_FILESYSTEMS}
