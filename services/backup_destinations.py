# services/backup_destinations.py
# CrossWatch - Local, WebDAV and cloud backup copies through rclone
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path, PurePosixPath
from typing import Any, Callable
from urllib.parse import urlsplit

_HEALTH_LOCK = threading.Lock()
_HEALTH: dict[str, Any] = {}


class RcloneUnavailable(ValueError):
    pass


def runtime_status() -> dict[str, Any]:
    path = shutil.which("rclone")
    if not path:
        return {"available": False, "error": "rclone is unavailable. Use the CW image with backup support or install rclone."}
    try:
        stat = Path(path).stat()
        signature = (path, stat.st_mtime_ns, stat.st_size)
        with _HEALTH_LOCK:
            if _HEALTH.get("signature") == signature and time.monotonic() < _HEALTH.get("until", 0):
                return dict(_HEALTH["status"])
            env = {k: v for k, v in os.environ.items() if not k.upper().lstrip("_").startswith("RCLONE_")}
            result = subprocess.run([path, "version"], capture_output=True, text=True, timeout=5, env=env)
            match = re.match(r"rclone v(\d+)\.(\d+)\.(\d+)", result.stdout)
            if result.returncode or not match:
                status = {"available": False, "error": "rclone could not start. Reinstall it or use the CW image with backup support."}
            elif tuple(map(int, match.groups())) < (1, 76, 0):
                status = {"available": False, "error": "rclone 1.76.0 or newer is required. Update rclone or your CW image."}
            else:
                status = {"available": True, "version": match.group(0)}
            _HEALTH.update(signature=signature, until=time.monotonic() + 30, status=status)
            return dict(status)
    except (OSError, subprocess.SubprocessError):
        return {"available": False, "error": "rclone could not start. Check its installation, permissions and system compatibility."}


def safe_remote_path(value: str) -> str:
    path = PurePosixPath(value)
    if not value or path.is_absolute() or any(p in {".", ".."} for p in value.split("/")) or any(c in value for c in "\\:\r\n\x00"):
        raise ValueError("Use a relative backup folder without parent paths.")
    return path.as_posix()


def validate_destination(raw: dict[str, Any], config_dir: Path) -> dict[str, Any]:
    kind = str(raw.get("kind") or "none")
    if kind == "none":
        return {"kind": "none"}
    if kind in {"onedrive", "dropbox"}:
        from services.backup_cloud import connection

        ident = str(raw.get("connection") or "")
        connection(ident, kind)
        return {"kind": kind, "connection": ident, "folder": safe_remote_path(str(raw.get("folder") or "CrossWatch"))}
    if kind == "folder":
        target = Path(str(raw.get("path") or ""))
        if not target.is_absolute() or target.resolve().is_relative_to(config_dir.resolve()):
            raise ValueError("Choose an absolute mounted folder outside the CW config directory.")
        if not target.is_dir():
            raise ValueError("The destination folder does not exist inside this container.")
        return {"kind": kind, "path": str(target.resolve())}
    if kind != "webdav":
        raise ValueError("Choose OneDrive, Dropbox, Nextcloud / WebDAV or a mounted folder.")
    url = str(raw.get("url") or "").strip()
    parsed = urlsplit(url)
    if parsed.scheme not in {"https", "http"} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("Enter a WebDAV server URL without embedded credentials.")
    if parsed.scheme == "http" and not raw.get("allow_http"):
        raise ValueError("Use HTTPS or explicitly allow HTTP for your local server.")
    user = str(raw.get("username") or "").strip()
    password = str(raw.get("password") or "")
    if not user or not password or any(c in user + password + url for c in "\r\n\x00"):
        raise ValueError("Enter a username and an app password without line breaks.")
    vendor = str(raw.get("vendor") or "other")
    if vendor not in {"nextcloud", "owncloud", "other"}:
        raise ValueError("Unknown WebDAV server type.")
    return {"kind": kind, "url": url, "username": user, "password": password, "vendor": vendor, "folder": safe_remote_path(str(raw.get("folder") or "CrossWatch")), "allow_http": parsed.scheme == "http"}


def _binary() -> str:
    status = runtime_status()
    if not status["available"]:
        raise RcloneUnavailable(status["error"])
    path = shutil.which("rclone")
    if not path:
        raise RcloneUnavailable("rclone is unavailable. Use the CW image with backup support or install rclone.")
    return path


def _environment(destination: dict[str, Any]) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.upper().lstrip("_").startswith("RCLONE_")}
    if destination["kind"] == "webdav":
        result = subprocess.run([_binary(), "obscure", "-"], input=destination["password"] + "\n", capture_output=True, text=True, timeout=15, env=env)
        if result.returncode:
            raise ValueError("Could not prepare the WebDAV connection.")
        env.update({"RCLONE_CONFIG_CW_TYPE": "webdav", "RCLONE_CONFIG_CW_URL": destination["url"], "RCLONE_CONFIG_CW_VENDOR": destination["vendor"], "RCLONE_CONFIG_CW_USER": destination["username"], "RCLONE_CONFIG_CW_PASS": result.stdout.strip()})
    return env


def remote(destination: dict[str, Any], name: str = "") -> str:
    name = safe_remote_path(name) if name else ""
    if destination["kind"] == "folder":
        root = Path(destination["path"]).resolve()
        path = root.joinpath(*PurePosixPath(name).parts).resolve()
        if not path.is_relative_to(root):
            raise ValueError("Invalid destination path.")
        return str(path)
    return "cw:" + destination["folder"].rstrip("/") + ("/" + name if name else "")


def run(destination: dict[str, Any], args: list[str], progress: Callable[[dict[str, Any]], None] | None = None) -> str:
    if destination["kind"] in {"onedrive", "dropbox"}:
        from services.backup_cloud import transfer

        with transfer(destination) as (config, env):
            return _run(args, progress, config, env)
    return _run(args, progress, "NUL" if os.name == "nt" else os.devnull, _environment(destination))


def _run(args: list[str], progress: Callable[[dict[str, Any]], None] | None, config: str, env: dict[str, str]) -> str:
    command = [_binary(), *args, "--config", config, "--retries", "2", "--low-level-retries", "2", "--contimeout", "15s", "--timeout", "60s", "--max-duration", "30m"]
    if progress is None:
        result = subprocess.run(command, capture_output=True, text=True, env=env, timeout=1850)
        if result.returncode:
            raise ValueError("The destination could not be reached or the operation was refused. Check its address, credentials, free space and permissions.")
        return result.stdout
    command.extend(["--use-json-log", "--stats", "1s", "--stats-log-level", "NOTICE"])
    with subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, env=env) as proc:
        try:
            assert proc.stderr is not None
            for line in proc.stderr:
                try:
                    record = json.loads(line)
                    stats = record.get("stats") or {}
                    if stats:
                        progress({"bytes": int(stats.get("bytes") or 0), "total_bytes": int(stats.get("totalBytes") or 0)})
                except (ValueError, TypeError):
                    continue
            if proc.wait(timeout=10):
                raise ValueError("The remote copy failed. Check the connection, credentials and available storage.")
        except BaseException:
            proc.kill()
            proc.wait()
            raise
    return ""


def digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def upload(destination: dict[str, Any], source: Path, name: str, progress: Callable[[dict[str, Any]], None]) -> None:
    run(destination, ["copyto", str(source), remote(destination, name), "--ignore-times"], progress)


def verify(destination: dict[str, Any], source: Path, name: str) -> None:
    with tempfile.TemporaryDirectory(prefix="cw-remote-check-", dir=source.parent) as folder:
        downloaded = Path(folder) / "copy"
        run(destination, ["copyto", remote(destination, name), str(downloaded)])
        if digest(source) != digest(downloaded):
            raise ValueError("The remote copy failed its checksum check. Your local backup is still available.")


def test_connection(destination: dict[str, Any], config_dir: Path) -> None:
    name = "cw-connection-test-" + uuid.uuid4().hex + ".probe"
    with tempfile.TemporaryDirectory(prefix="cw-probe-", dir=config_dir) as folder:
        source = Path(folder) / "probe"
        source.write_bytes(os.urandom(128))
        try:
            upload(destination, source, name, lambda _: None)
            verify(destination, source, name)
        finally:
            run(destination, ["deletefile", remote(destination, name)])


def list_remote(destination: dict[str, Any]) -> list[dict[str, Any]]:
    data = json.loads(run(destination, ["lsjson", remote(destination), "--recursive", "--files-only", "--include", "*.cwbackup", "--include", "*.zip"]))
    result = []
    for item in data:
        path = safe_remote_path(str(item.get("Path") or ""))
        if Path(path).name.startswith("crosswatch-backup-"):
            result.append({"path": path, "size": int(item.get("Size") or 0), "created_at": item.get("ModTime"), "encrypted": path.endswith(".cwbackup")})
    return sorted(result, key=lambda row: str(row["created_at"]), reverse=True)
