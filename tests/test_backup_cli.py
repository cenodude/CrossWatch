# tests/test_backup_cli.py
# CrossWatch - CLI backup configuration, cloud and recovery checks
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
from typing import Any

import pytest
import typer

from cli._app import run_isolated
from cli._context import Ctx
from cli._render import Output
from cli._transport import HttpTransport


class BackupTransport(HttpTransport):
    def __init__(self):
        self.calls = []
        self.plan = {"configured": True, "components": ["application", "trackers", "captures"], "trackers": None,
                     "encrypt_local": False, "has_password": True, "destination": {"kind": "none"}, "remote_keep": 30}
        self.routes = {}

    def request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: Any = None) -> Any:
        self.calls.append((method, path, copy.deepcopy(json_body)))
        key = (method, path)
        if key in self.routes:
            value = self.routes[key]
            return value(json_body) if callable(value) else value
        if key == ("GET", "/api/backups/plan"):
            return {"ok": True, "plan": copy.deepcopy(self.plan)}
        if key == ("POST", "/api/backups/plan"):
            plan = copy.deepcopy(json_body)
            plan.pop("password", None)
            plan.pop("password_saved", None)
            plan.get("destination", {}).pop("password", None)
            return {"ok": True, "plan": plan}
        raise AssertionError(f"Unexpected backup request: {method} {path}")


@pytest.fixture
def cli(monkeypatch):
    for name in ("CW_URL", "CW_TOKEN", "CW_OUTPUT", "CW_LOCAL"):
        monkeypatch.delenv(name, raising=False)
    transport = BackupTransport()
    state = Ctx(url="http://cw.test", out=Output("json"), _http=transport)
    return state, transport


def test_configure_changes_contents_and_keeps_saved_destination(cli, capsys):
    state, transport = cli
    transport.plan["destination"] = {"kind": "webdav", "url": "https://dav.test", "username": "cw", "has_password": True}
    assert run_isolated(state, ["backup", "configure", "--component", "trackers", "--component", "captures", "--tracker", "kids", "--encrypt"]) == 0
    body = transport.calls[-1][2]
    assert body["components"] == ["trackers", "captures"]
    assert body["trackers"] == ["kids"]
    assert body["encrypt_local"] is True
    assert body["destination"] == {"kind": "webdav", "url": "https://dav.test", "username": "cw"}
    assert "password" not in body
    assert json.loads(capsys.readouterr().out)["ok"]


def test_configure_secret_files_and_new_destination(cli, tmp_path, capsys):
    state, transport = cli
    password = tmp_path / "backup-secret"
    password.write_text("a long backup secret\n")
    app_password = tmp_path / "dav-secret"
    app_password.write_text("webdav-secret\n")
    assert run_isolated(state, ["backup", "configure", "--destination", "webdav", "--server", "https://dav.test",
                               "--username", "cw", "--app-password-file", str(app_password), "--password-file", str(password),
                               "--password-saved", "--folder", "CW", "--vendor", "nextcloud", "--remote-keep", "7"]) == 0
    body = transport.calls[-1][2]
    assert body["password"] == "a long backup secret"
    assert body["destination"]["password"] == "webdav-secret"
    assert body["remote_keep"] == 7
    assert body["password_saved"] is True
    output = capsys.readouterr().out
    assert "a long backup secret" not in output and "webdav-secret" not in output


@pytest.mark.parametrize("args", [["--tracker", "kids", "--all-trackers"], ["--destination", "folder", "--server", "https://dav.test"],
                                  ["--component", "captures", "--tracker", "kids"]])
def test_invalid_configuration_never_saves(cli, args):
    state, transport = cli
    assert run_isolated(state, ["backup", "configure", *args]) != 0
    assert not any(method == "POST" for method, _, _ in transport.calls)


def test_password_file_requires_saved_confirmation(cli, tmp_path):
    state, transport = cli
    path = tmp_path / "secret"
    path.write_text("a long backup secret")
    assert run_isolated(state, ["backup", "configure", "--password-file", str(path)]) != 0
    assert not any(method == "POST" for method, _, _ in transport.calls)


def test_managed_create_queues_saved_plan(cli, capsys):
    state, transport = cli
    transport.routes[("POST", "/api/backups/create")] = {"ok": True, "job": {"id": "job1", "stage": "queued"}}
    assert run_isolated(state, ["backup", "create", "--managed"]) == 0
    assert transport.calls == [("POST", "/api/backups/create", {"managed": True})]
    assert json.loads(capsys.readouterr().out)["job"]["id"] == "job1"


def test_legacy_create_keeps_label(cli):
    state, transport = cli
    transport.routes[("POST", "/api/backups/create")] = {"ok": True, "backup": {"path": "legacy.zip"}}
    assert run_isolated(state, ["backup", "create", "--note", "before upgrade"]) == 0
    assert transport.calls[-1][2] == {"label": "before upgrade"}


def test_schedule_uses_supported_time_days_and_plan(cli):
    state, transport = cli
    transport.routes[("GET", "/api/backups/schedule")] = {"schedule": {"active": False, "at": "03:00", "days": [1, 2, 3], "include_cache": True}}
    transport.routes[("POST", "/api/backups/schedule")] = {"ok": True}
    assert run_isolated(state, ["backup", "schedule", "--enable", "--managed", "--at", "04:30", "--day", "2", "--day", "6", "--keep", "14"]) == 0
    body = transport.calls[-1][2]
    assert body["enabled"] and body["managed"] and body["include_cache"]
    assert (body["at"], body["days"], body["max_backups"], body["retention_days"]) == ("04:30", [2, 6], 14, 0)


@pytest.mark.parametrize("args", [["--at", "25:00"], ["--day", "8"], ["--every-hours", "4"]])
def test_schedule_rejects_unsupported_values(cli, args):
    state, transport = cli
    assert run_isolated(state, ["backup", "schedule", *args]) != 0
    assert not transport.calls


def test_status_watches_progress_and_retains_error_details(cli, monkeypatch, capsys):
    state, transport = cli
    snapshots = iter([{"jobs": [{"id": "job1", "stage": "uploading", "bytes": 10, "total_bytes": 20}]},
                      {"jobs": [{"id": "job1", "stage": "failed", "error": "Destination full", "retry_at": 0}]}])
    transport.routes[("GET", "/api/backups/status")] = lambda _: next(snapshots)
    monkeypatch.setattr("cli.commands.backup_settings.time.sleep", lambda _: None)
    assert run_isolated(state, ["backup", "status", "--job", "job1", "--watch"]) == 0
    output = capsys.readouterr().out
    assert "uploading" in output and "Destination full" in output
    assert len(transport.calls) == 2


@pytest.mark.parametrize("args,endpoint,body", [(["retry", "job1"], "retry", {"id": "job1"}),
    (["test-destination"], "destination/test", {}), (["remote-download", "install/backup.cwbackup"], "remote/download", {"path": "install/backup.cwbackup"})])
def test_destination_actions_use_saved_settings(cli, args, endpoint, body):
    state, transport = cli
    transport.routes[("POST", "/api/backups/" + endpoint)] = {"ok": True, "backup": {"path": "imported/backup.cwbackup"}}
    assert run_isolated(state, ["backup", *args]) == 0
    assert transport.calls == [("POST", "/api/backups/" + endpoint, body)]


def test_cloud_authorization_steps_and_destination_save(cli, tmp_path, capsys):
    state, transport = cli
    transport.routes[("POST", "/api/backups/cloud/connect")] = {"ok": True, "connection": {"id": "conn1", "kind": "dropbox", "status": "connecting", "question": "config_token"}}
    transport.routes[("POST", "/api/backups/cloud/continue")] = {"ok": True, "connection": {"id": "conn1", "kind": "dropbox", "status": "connected"}}
    assert run_isolated(state, ["backup", "cloud-connect", "dropbox", "--step"]) == 0
    secret = tmp_path / "authorization"
    secret.write_text("private-authorization-result\n")
    assert run_isolated(state, ["backup", "cloud-continue", "conn1", "--answer-file", str(secret)]) == 0
    assert transport.calls[-1][2] == {"id": "conn1", "answer": "private-authorization-result"}
    assert "private-authorization-result" not in capsys.readouterr().out
    assert run_isolated(state, ["backup", "configure", "--destination", "dropbox", "--connection", "conn1"]) == 0
    assert transport.calls[-1][2]["destination"] == {"kind": "dropbox", "connection": "conn1"}


def test_interactive_onedrive_authorization_hides_token_and_selects_drive(cli, monkeypatch):
    state, transport = cli
    state.out = Output("plain")
    questions = iter([{"question": "config_driveid", "choices": [{"value": "drive1", "label": "Personal"}]},
                      {"question": "config_drive_ok"}, {"status": "connected"}])
    transport.routes[("POST", "/api/backups/cloud/connect")] = {"connection": {"id": "conn1", "kind": "onedrive", "status": "connecting", "question": "config_token"}}
    transport.routes[("POST", "/api/backups/cloud/continue")] = lambda _: {"connection": {"id": "conn1", "kind": "onedrive", "status": "connecting", **next(questions)}}
    prompts = []
    answers = iter(["private-token", "drive1"])
    def prompt(label, **kwargs):
        prompts.append(kwargs)
        return next(answers)
    monkeypatch.setattr(typer, "prompt", prompt)
    monkeypatch.setattr(typer, "confirm", lambda *args, **kwargs: True)
    assert run_isolated(state, ["backup", "cloud-connect", "onedrive"]) == 0
    assert prompts[0]["hide_input"] is True
    assert [body["answer"] for _, path, body in transport.calls if path.endswith("continue")] == ["private-token", "drive1", "true"]


def test_tracker_recovery_uses_password_without_replacement_or_restart(cli, tmp_path):
    state, transport = cli
    transport.routes[("POST", "/api/backups/restore")] = {"ok": True, "result": {"message": "Recovered tracker"}}
    secret = tmp_path / "secret"
    secret.write_text("archive-password\n")
    assert run_isolated(state, ["backup", "restore", "saved.cwbackup", "--new-tracker", "--instance", "default", "--label", "Recovered", "--password-file", str(secret)]) == 0
    body = transport.calls[-1][2]
    assert body["mode"] == "new_tracker" and body["restart"] is False
    assert (body["instance"], body["password"], body["label"]) == ("default", "archive-password", "Recovered")


def test_replacement_restore_requires_confirmation(cli, monkeypatch):
    state, transport = cli
    monkeypatch.setattr(typer, "confirm", lambda *args, **kwargs: False)
    assert run_isolated(state, ["backup", "restore", "saved.zip"]) == 0
    assert not transport.calls
    transport.routes[("POST", "/api/backups/restore")] = {"ok": True, "result": {"restart_scheduled": True}}
    assert run_isolated(state, ["backup", "restore", "saved.zip", "--yes"]) == 0
    assert transport.calls[-1][2]["confirm"] and transport.calls[-1][2]["restart"]


@pytest.mark.parametrize("section", ["mappings", "anime_rules", "captures"])
def test_selective_restore_sends_only_requested_section(cli, section):
    state, transport = cli
    transport.routes[("POST", "/api/backups/restore")] = {"ok": True, "result": {"restart_scheduled": True}}
    assert run_isolated(state, ["backup", "restore", "saved.zip", "--only", section, "--yes"]) == 0
    body = transport.calls[-1][2]
    assert body["mode"] == section and body["confirm"] and body["restart"]


@pytest.mark.parametrize("options", [["--only", "unknown"], ["--only", "mappings", "--new-tracker", "--instance", "default"]])
def test_selective_restore_rejects_invalid_cli_options(cli, options):
    state, transport = cli
    assert run_isolated(state, ["backup", "restore", "saved.zip", *options, "--yes"]) != 0
    assert not transport.calls


def test_validation_reports_missing_config_key_and_tracker_ids(cli, capsys):
    state, transport = cli
    state.out = Output("plain")
    transport.routes[("POST", "/api/backups/validate")] = {"ok": True, "validation": {"ok": True, "application_ready": False,
        "application_error": "Original key required", "manifest": {"trackers": [{"id": "kids", "label": "Kids"}]}}}
    assert run_isolated(state, ["backup", "validate", "saved.zip"]) == 0
    output = capsys.readouterr()
    assert "kids" in output.out and "Original key required" in output.err


def test_recovery_key_file_is_not_echoed_or_overwritten(cli, tmp_path, capsys):
    state, transport = cli
    transport.routes[("POST", "/api/backups/recovery-key")] = {"ok": True, "key": "generated-secret-value"}
    file = tmp_path / "key"
    assert run_isolated(state, ["backup", "recovery-key", str(file)]) == 0
    assert file.read_text().strip() == "generated-secret-value"
    assert "generated-secret-value" not in capsys.readouterr().out
    file.write_text("existing-key")
    assert run_isolated(state, ["backup", "recovery-key", str(file)]) != 0
    assert file.read_text() == "existing-key"


def test_rejected_backup_requests_exit_nonzero_in_json_mode(cli):
    state, transport = cli
    transport.routes[("POST", "/api/backups/retry")] = {"ok": False, "error": "Choose a failed job"}
    assert run_isolated(state, ["backup", "retry", "job1"]) != 0


def test_mutations_do_not_fall_back_to_local_mode(cli):
    state, transport = cli
    state.force_local = True
    assert run_isolated(state, ["backup", "configure", "--encrypt"]) != 0
    assert not transport.calls


def test_cli_plan_create_and_validate_with_real_backup_api(cli, tmp_path, monkeypatch, capsys):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import _backup_crypto as crypto
    from api import backupsAPI
    from cw_platform import config_base
    from services import backup_manager as manager, backups

    state, transport = cli
    root = tmp_path / "config"
    root.mkdir()
    cfg = {"crosswatch": {"instances": {}}, "pairs": []}
    (root / "config.json").write_text(json.dumps(cfg))
    monkeypatch.setattr(backups, "CONFIG", root)
    monkeypatch.setattr(config_base, "CONFIG", root)
    monkeypatch.setattr(config_base, "load_config", lambda: copy.deepcopy(cfg))
    monkeypatch.setattr(manager, "load_config", lambda: copy.deepcopy(cfg))
    monkeypatch.setattr(manager, "start", lambda: None)
    monkeypatch.delenv("CW_CONFIG_KEY", raising=False)
    monkeypatch.delenv("CROSSWATCH_CONFIG_KEY", raising=False)
    secret = tmp_path / "password"
    secret.write_text("a separate backup password")
    app = FastAPI()
    app.include_router(backupsAPI.router)
    with TestClient(app) as client:
        transport.routes[("GET", "/api/backups/plan")] = lambda _: client.get("/api/backups/plan").json()
        for endpoint in ("plan", "create", "validate"):
            transport.routes[("POST", "/api/backups/" + endpoint)] = lambda body, endpoint=endpoint: client.post("/api/backups/" + endpoint, json=body).json()
        assert run_isolated(state, ["backup", "configure", "--component", "application", "--encrypt", "--password-file", str(secret), "--password-saved"]) == 0
        assert run_isolated(state, ["backup", "create", "--managed"]) == 0
        manager.run_job(manager._jobs()[0])
        job = manager.status()["jobs"][0]
        assert job["stage"] == "complete"
        assert (root / "backups" / job["path"]).read_bytes().startswith(crypto.MAGIC)
        assert run_isolated(state, ["backup", "validate", job["path"]]) == 0
    assert "a separate backup password" not in capsys.readouterr().out
