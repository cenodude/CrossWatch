# tests/test_backup_cloud.py
# CrossWatch - Cloud authorization, encrypted credentials and rclone integration checks
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
from types import SimpleNamespace

import pytest

from cw_platform import config_base
from services import backup_cloud as cloud, backup_destinations as destinations, backup_manager as manager, backups

TOKEN = {"access_token": "fake-test-access", "refresh_token": "fake-test-refresh", "token_type": "Bearer", "expiry": "2030-01-01T00:00:00Z"}


@pytest.fixture
def installation(tmp_path, monkeypatch):
    monkeypatch.setattr(backups, "CONFIG", tmp_path)
    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(manager, "load_config", lambda: {})
    monkeypatch.delenv("CW_CONFIG_KEY", raising=False)
    monkeypatch.delenv("CROSSWATCH_CONFIG_KEY", raising=False)
    return tmp_path


@pytest.fixture
def rclone(monkeypatch):
    binary = os.getenv("RCLONE_TEST_BINARY") or shutil.which("rclone")
    if not binary:
        pytest.skip("Set RCLONE_TEST_BINARY to run real cloud configuration checks")
    monkeypatch.setattr(destinations, "_binary", lambda: binary)
    return binary


@pytest.mark.parametrize("kind", ["onedrive", "dropbox"])
def test_real_rclone_starts_headless_flow_with_encrypted_config(installation, rclone, kind):
    result = cloud.begin(kind)
    assert result["question"] == "config_token" and result["status"] == "connecting"
    record = cloud._record(result["id"])
    assert record["key"].startswith("enc:v1:")
    assert "key" not in result and "state" not in result
    content = cloud._path(result["id"]).read_bytes()
    assert b"RCLONE_ENCRYPT_V0:" in content and b"[cw]" not in content


def test_real_dropbox_configuration_remains_encrypted_and_passes_no_tokens_in_arguments(installation, rclone, monkeypatch):
    result = cloud.begin("dropbox")
    real_run = subprocess.run
    calls = []

    def capture(args, **kwargs):
        calls.append(args)
        return real_run(args, **kwargs)

    monkeypatch.setattr(cloud.subprocess, "run", capture)
    connected = cloud.advance(result["id"], json.dumps(TOKEN))
    assert connected["status"] == "connected"
    assert all("fake-test" not in str(args) for args in calls)
    destination = destinations.validate_destination({"kind": "dropbox", "connection": connected["id"], "folder": "CW backups"}, installation)
    assert destination["folder"] == "CW backups"
    with cloud.transfer(destination) as (path, env):
        dumped = real_run([rclone, "config", "dump", "--config", path], env=env, capture_output=True, text=True, check=True, timeout=10)
        assert json.loads(json.loads(dumped.stdout)["cw"]["token"])["refresh_token"] == TOKEN["refresh_token"]
    for file in manager.private_dir().iterdir():
        assert b"fake-test" not in file.read_bytes()


def test_onedrive_drive_selection_and_confirmation(installation, monkeypatch):
    ident = "a" * 32
    record = {"id": ident, "kind": "onedrive", "key": manager._encrypt_secret("test-key"), "status": "connecting"}
    cloud._progress(record, {"State": "oauth", "Option": {"Name": "config_token"}})
    responses = iter([
        {"State": "choose_type_done", "Option": {"Name": "config_type"}},
        {"State": "driveid_final", "Option": {"Name": "config_driveid", "Examples": [{"Value": "drive1", "Help": "Personal OneDrive"}]}},
        {"State": "driveid_final_end", "Option": {"Name": "config_drive_ok"}},
        {"State": "", "Option": None},
    ])
    answers = []

    def invoke(record, args, answer=None):
        answers.append(answer)
        return next(responses)

    monkeypatch.setattr(cloud, "_invoke", invoke)
    selected = cloud.advance(ident, json.dumps(TOKEN))
    assert selected["question"] == "config_driveid" and answers[1] == "onedrive"
    with pytest.raises(ValueError, match="available"):
        cloud.advance(ident, "unlisted-drive")
    assert cloud.advance(ident, "drive1")["question"] == "config_drive_ok"
    assert cloud.advance(ident, "true")["status"] == "connected"
    assert answers[2:] == ["drive1", "true"]


def test_authorization_accepts_rclone_encoded_result_but_rejects_extra_config():
    encoded = base64.urlsafe_b64encode(json.dumps({"token": json.dumps(TOKEN)}).encode()).decode().rstrip("=")
    assert json.loads(cloud._authorization(encoded)) == TOKEN
    for value in ["invalid", json.dumps({"access_token": "missing-refresh"}), base64.urlsafe_b64encode(json.dumps({"token": json.dumps(TOKEN), "token_url": "https://other.invalid"}).encode()).decode()]:
        with pytest.raises(ValueError, match="authorization"):
            cloud._authorization(value)


def test_expired_or_wrong_provider_connection_is_rejected(installation):
    ident = "b" * 32
    cloud._save({"id": ident, "kind": "onedrive", "status": "connecting", "expires_at": 0})
    with pytest.raises(ValueError, match="expired"):
        cloud.advance(ident, "anything")
    with pytest.raises(ValueError, match="Connect"):
        destinations.validate_destination({"kind": "dropbox", "connection": ident}, installation)


def test_cloud_transfer_uses_writable_encrypted_config_without_environment_overrides(installation, monkeypatch):
    ident = "c" * 32
    cloud._save({"id": ident, "kind": "onedrive", "status": "connected", "key": manager._encrypt_secret("test-config-password")})
    cloud._path(ident).write_bytes(b"RCLONE_ENCRYPT_V0:\nmock")
    monkeypatch.setenv("RCLONE_CONFIG_CW_TOKEN", "external-token")
    monkeypatch.setenv("_RCLONE_CONFIG_KEY_FILE", "external-key-file")
    monkeypatch.setattr(destinations, "_binary", lambda: "rclone")
    seen = []

    def invoke(args, **kwargs):
        seen.append((args, kwargs["env"]))
        return SimpleNamespace(returncode=0, stdout="[]")

    monkeypatch.setattr(destinations.subprocess, "run", invoke)
    assert destinations.list_remote({"kind": "onedrive", "connection": ident, "folder": "CrossWatch"}) == []
    args, env = seen[0]
    assert args[args.index("--config") + 1] == str(cloud._path(ident))
    assert env["RCLONE_CONFIG_PASS"] == "test-config-password"
    assert "RCLONE_CONFIG_CW_TOKEN" not in env and "_RCLONE_CONFIG_KEY_FILE" not in env
    assert "test-config-password" not in args


def test_cloud_api_errors_never_return_raw_rclone_output(installation, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api.backupsAPI import router

    app = FastAPI()
    app.include_router(router)
    monkeypatch.setattr(cloud, "begin", lambda kind: (_ for _ in ()).throw(RuntimeError("secret-token")))
    with TestClient(app) as client:
        response = client.post("/api/backups/cloud/connect", json={"kind": "dropbox"})
        assert response.status_code == 400 and "secret-token" not in response.text
