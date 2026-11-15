# tests/test_backup_destinations.py
# CrossWatch - Backup destination validation and real rclone round trips
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from types import SimpleNamespace
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import pytest

from services import backup_destinations as destinations, backups


def test_missing_rclone_is_reported_before_transfer(monkeypatch):
    monkeypatch.setattr(destinations.shutil, "which", lambda _: None)
    assert not destinations.runtime_status()["available"]
    with pytest.raises(destinations.RcloneUnavailable, match="unavailable"):
        destinations._binary()


@pytest.mark.parametrize("output,code,available", [("rclone v1.76.0\n", 0, True), ("rclone v1.75.0\n", 0, False), ("broken", 0, False), ("rclone v1.76.0\n", 1, False)])
def test_rclone_health_runs_binary_and_checks_version(tmp_path, monkeypatch, output, code, available):
    binary = tmp_path / "rclone"
    binary.write_text("test")
    monkeypatch.setattr(destinations.shutil, "which", lambda _: str(binary))
    monkeypatch.setattr(destinations.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=code, stdout=output))
    assert destinations.runtime_status()["available"] is available


@pytest.mark.parametrize("error", [OSError("incompatible binary"), subprocess.TimeoutExpired("rclone", 5)])
def test_rclone_health_handles_unusable_binary(tmp_path, monkeypatch, error):
    binary = tmp_path / "rclone"
    binary.write_text("test")
    monkeypatch.setattr(destinations.shutil, "which", lambda _: str(binary))
    monkeypatch.setattr(destinations.subprocess, "run", lambda *a, **kw: (_ for _ in ()).throw(error))
    assert not destinations.runtime_status()["available"]


@pytest.mark.parametrize("value", ["../escape", "/absolute", "a/../b", "C:/drive", "a\\b", "a\nflag", ""])
def test_remote_paths_reject_escape_and_option_injection(value):
    with pytest.raises(ValueError):
        destinations.safe_remote_path(value)


def test_webdav_requires_explicit_insecure_http_and_separate_credentials(tmp_path):
    raw = {"kind": "webdav", "url": "http://localhost/dav", "username": "user", "password": "app-password"}
    with pytest.raises(ValueError, match="HTTPS"):
        destinations.validate_destination(raw, tmp_path)
    assert destinations.validate_destination({**raw, "allow_http": True}, tmp_path)["allow_http"]
    with pytest.raises(ValueError, match="embedded credentials"):
        destinations.validate_destination({**raw, "url": "https://user:pass@localhost/dav"}, tmp_path)


def test_mounted_destination_must_be_outside_config(tmp_path, monkeypatch):
    monkeypatch.setenv("CW_BACKUP_ROOT", str(tmp_path))
    child = tmp_path / "nested"
    child.mkdir()
    with pytest.raises(ValueError, match="outside"):
        destinations.validate_destination({"kind": "folder", "path": str(child)}, tmp_path)


def test_mounted_root_defaults_to_backups(monkeypatch):
    monkeypatch.delenv("CW_BACKUP_ROOT", raising=False)
    assert destinations.mounted_root() == Path("/backups").resolve()


def test_mounted_destinations_are_limited_to_the_configured_root(tmp_path, monkeypatch):
    root = tmp_path / "allowed"
    nested = root / "nested"
    nested.mkdir(parents=True)
    outside = tmp_path / "allowed-other"
    outside.mkdir()
    monkeypatch.setenv("CW_BACKUP_ROOT", str(root))
    assert destinations.validate_destination({"kind": "folder", "path": str(nested)}, tmp_path / "config")["path"] == str(nested.resolve())
    assert destinations.validate_destination({"kind": "folder", "path": str(root)}, tmp_path / "config")["path"] == str(root.resolve())
    for path in (str(outside), str(root / ".." / "allowed-other"), "relative-folder"):
        with pytest.raises(ValueError, match="backup root"):
            destinations.validate_destination({"kind": "folder", "path": path}, tmp_path / "config")
        with pytest.raises(ValueError, match="backup root"):
            destinations.remote({"kind": "folder", "path": path}, "archive.zip")


def test_mounted_transfer_rechecks_changed_root(tmp_path, monkeypatch):
    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    old_root.mkdir()
    new_root.mkdir()
    monkeypatch.setenv("CW_BACKUP_ROOT", str(old_root))
    destination = destinations.validate_destination({"kind": "folder", "path": str(old_root)}, tmp_path / "config")
    monkeypatch.setenv("CW_BACKUP_ROOT", str(new_root))
    with pytest.raises(ValueError, match="backup root"):
        destinations.remote(destination, "archive.zip")


def test_mounted_paths_reject_symlinks_outside_the_root(tmp_path, monkeypatch):
    monkeypatch.setattr(backups, "CONFIG", tmp_path / "config")
    root = tmp_path / "allowed"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    link = root / "linked"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("Creating symlinks is unavailable on this host")
    monkeypatch.setenv("CW_BACKUP_ROOT", str(root))
    with pytest.raises(ValueError, match="outside"):
        destinations.validate_destination({"kind": "folder", "path": str(link)}, tmp_path / "config")
    with pytest.raises(ValueError, match="outside"):
        destinations.remote({"kind": "folder", "path": str(root)}, "linked/archive.zip")


@pytest.fixture
def rclone(monkeypatch):
    binary = os.getenv("RCLONE_TEST_BINARY") or shutil.which("rclone")
    if not binary:
        pytest.skip("Set RCLONE_TEST_BINARY to run real transfer checks")
    monkeypatch.setattr(destinations, "_binary", lambda: binary)
    return binary


@pytest.fixture(params=["folder", "webdav"])
def destination(request, tmp_path, rclone, monkeypatch):
    remote = tmp_path / "remote"
    remote.mkdir()
    monkeypatch.setenv("CW_BACKUP_ROOT", str(remote))
    monkeypatch.setattr(backups, "CONFIG", tmp_path / "config")
    if request.param == "folder":
        yield {"kind": "folder", "path": str(remote)}
        return
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    url = f"http://127.0.0.1:{port}/"
    proc = subprocess.Popen([rclone, "serve", "webdav", str(remote), "--addr", f"127.0.0.1:{port}", "--user", "cw-test", "--pass", "test-app-password", "--config", "NUL" if os.name == "nt" else os.devnull], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            if proc.poll() is not None:
                pytest.fail("rclone WebDAV test server stopped")
            try:
                with urlopen(url, timeout=0.2):
                    break
            except HTTPError:
                break
            except (URLError, TimeoutError):
                time.sleep(0.1)
        else:
            pytest.fail("rclone WebDAV test server did not start")
        yield {"kind": "webdav", "url": url, "username": "cw-test", "password": "test-app-password", "vendor": "other", "folder": "CrossWatch", "allow_http": True}
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_real_connection_upload_progress_verification_browse_download_and_delete(destination, tmp_path):
    destinations.test_connection(destination, tmp_path)
    source = tmp_path / "crosswatch-backup-test.cwbackup"
    source.write_bytes(os.urandom(2 * 1024 * 1024))
    name = "installation/" + source.name
    progress = []
    destinations.upload(destination, source, name, progress.append)
    destinations.verify(destination, source, name)
    assert progress and progress[-1]["bytes"] == source.stat().st_size
    listed = destinations.list_remote(destination)
    assert [r["path"] for r in listed] == [name]
    downloaded = tmp_path / "downloaded"
    destinations.run(destination, ["copyto", destinations.remote(destination, name), str(downloaded)])
    assert downloaded.read_bytes() == source.read_bytes()
    source.write_bytes(b"different data")
    with pytest.raises(ValueError, match="checksum"):
        destinations.verify(destination, source, name)
    destinations.run(destination, ["deletefile", destinations.remote(destination, name)])
    assert destinations.list_remote(destination) == []


@pytest.mark.parametrize("kind", ["onedrive", "dropbox"])
def test_removed_destinations_are_rejected_before_transfer(tmp_path, monkeypatch, kind):
    raw = {"kind": kind, "connection": "old-account", "folder": "CrossWatch"}
    monkeypatch.setattr(destinations, "_binary", lambda: pytest.fail("rclone must not run"))
    with pytest.raises(destinations.UnsupportedDestination):
        destinations.validate_destination(raw, tmp_path)
    with pytest.raises(destinations.UnsupportedDestination):
        destinations.remote(raw)
    with pytest.raises(destinations.UnsupportedDestination):
        destinations.run(raw, ["lsjson", "cw:"])
