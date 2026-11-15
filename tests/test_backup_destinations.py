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

from services import backup_destinations as destinations


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


def test_mounted_destination_must_be_outside_config(tmp_path):
    child = tmp_path / "nested"
    child.mkdir()
    with pytest.raises(ValueError, match="outside"):
        destinations.validate_destination({"kind": "folder", "path": str(child)}, tmp_path)


@pytest.fixture
def rclone(monkeypatch):
    binary = os.getenv("RCLONE_TEST_BINARY") or shutil.which("rclone")
    if not binary:
        pytest.skip("Set RCLONE_TEST_BINARY to run real transfer checks")
    monkeypatch.setattr(destinations, "_binary", lambda: binary)
    return binary


@pytest.fixture(params=["folder", "webdav"])
def destination(request, tmp_path, rclone):
    remote = tmp_path / "remote"
    remote.mkdir()
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
