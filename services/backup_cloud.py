# services/backup_cloud.py
# CrossWatch - Encrypted rclone cloud connections and headless authorization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import base64
import json
import os
import re
import secrets
import subprocess
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Any

from services import backup_destinations as destinations, backup_manager as manager

PROVIDERS = {"onedrive": "OneDrive", "dropbox": "Dropbox"}
_LOCK = threading.RLock()
_CONNECTION_LOCKS: dict[str, threading.RLock] = {}
_QUESTIONS = {"config_token", "config_type", "config_driveid", "config_drive_ok"}


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{32}", str(value)):
        raise ValueError("Choose a saved cloud connection.")
    return uuid.UUID(hex=value).hex


def _lock(ident: str):
    with _LOCK:
        return _CONNECTION_LOCKS.setdefault(_identifier(ident), threading.RLock())


def _record(ident: str) -> dict[str, Any]:
    value = manager.read("cloud-" + _identifier(ident) + ".json", None)
    if not value or value.get("kind") not in PROVIDERS:
        raise ValueError("This cloud connection is unavailable. Connect the account again.")
    return value


def _save(record: dict[str, Any]) -> None:
    manager.write("cloud-" + _identifier(record["id"]) + ".json", record)


def _path(ident: str):
    base = manager.private_dir().resolve()
    path = base / ("cloud-" + _identifier(ident) + ".conf")
    if path.resolve().parent != base:
        raise ValueError("Invalid cloud connection file.")
    return path


def _environment(record: dict[str, Any]) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.upper().lstrip("_").startswith("RCLONE_")}
    env["RCLONE_CONFIG_PASS"] = manager._decrypt_secret(record["key"])
    env["RCLONE_ASK_PASSWORD"] = "false"
    return env


def _invoke(record: dict[str, Any], args: list[str], answer: str | None = None) -> dict[str, Any]:
    env = _environment(record)
    if answer is not None:
        env["RCLONE_RESULT"] = answer
    try:
        result = subprocess.run([destinations._binary(), "config", *args, "--non-interactive", "--config", str(_path(record["id"])), "--contimeout", "15s", "--timeout", "30s"], env=env, capture_output=True, text=True, timeout=90)
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError("Cloud connection could not finish. Check the connection and try again.") from exc
    if result.returncode:
        raise ValueError("Cloud authorization failed. Check the authorization result and your account permissions, then try again.")
    try:
        data = json.loads(result.stdout)
        if not isinstance(data, dict):
            raise ValueError()
        return data
    except (ValueError, TypeError) as exc:
        raise ValueError("rclone returned an unexpected connection response.") from exc


def _progress(record: dict[str, Any], data: dict[str, Any]) -> dict[str, Any]:
    state = str(data.get("State") or "")
    option = data.get("Option") or {}
    name = str(option.get("Name") or "")
    if data.get("Error"):
        raise ValueError("The account could not be authorized. Check the pasted result or restart the connection.")
    if state and name not in _QUESTIONS:
        raise ValueError("This account needs additional rclone setup. Use a personal or work OneDrive, or a personal Dropbox account.")
    record.update(status="connecting" if state else "connected", state=manager._encrypt_secret(state) if state else "", question=name, expires_at=time.time() + 900 if state else 0)
    examples = option.get("Examples") or []
    record["choices"] = [{"value": str(row["Value"]), "label": str(row.get("Help") or row["Value"])[:300]} for row in examples if isinstance(row, dict) and "Value" in row]
    _save(record)
    return _public(record)


def _public(record: dict[str, Any]) -> dict[str, Any]:
    return {key: record.get(key) for key in ("id", "kind", "status", "question", "choices", "expires_at")}


def connection(ident: str, kind: str) -> dict[str, Any]:
    record = _record(ident)
    if record["kind"] != kind or record["status"] != "connected" or not _path(ident).is_file():
        raise ValueError("Connect this cloud account before saving the destination.")
    return _public(record)


def begin(kind: str) -> dict[str, Any]:
    if kind not in PROVIDERS:
        raise ValueError("Choose OneDrive or Dropbox.")
    binary = destinations._binary()
    ident = uuid.uuid4().hex
    password = secrets.token_urlsafe(48)
    record = {"id": ident, "kind": kind, "key": manager._encrypt_secret(password), "status": "connecting", "expires_at": time.time() + 900}
    with _lock(ident):
        path = _path(ident)
        try:
            with path.open("x", encoding="utf-8"):
                os.chmod(path, 0o600)
            env = {k: v for k, v in os.environ.items() if not k.upper().lstrip("_").startswith("RCLONE_")}
            result = subprocess.run([binary, "config", "encryption", "set", "--config", str(path)], input=password + "\n" + password + "\n", capture_output=True, text=True, env=env, timeout=15)
            if result.returncode or b"RCLONE_ENCRYPT_V0:" not in path.read_bytes():
                raise ValueError("Could not secure the cloud connection settings.")
            data = _invoke(record, ["create", "cw", kind, "config_is_local", "false"])
            return _progress(record, data)
        except Exception:
            path.unlink(missing_ok=True)
            (manager.private_dir() / ("cloud-" + ident + ".json")).unlink(missing_ok=True)
            raise


def _authorization(value: str) -> str:
    if not value or len(value) > 65536:
        raise ValueError("Paste the authorization result from rclone.")
    try:
        if value.startswith("{"):
            token = json.loads(value)
        else:
            decoded = json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))
            if set(decoded) != {"token"}:
                raise ValueError()
            token = json.loads(decoded["token"])
        if not isinstance(token, dict) or not token.get("access_token") or not token.get("refresh_token"):
            raise ValueError()
        allowed = {"access_token", "token_type", "refresh_token", "expiry", "expires_in"}
        return json.dumps({k: v for k, v in token.items() if k in allowed})
    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise ValueError("Paste the complete rclone authorization result, including its refresh token.") from exc


def advance(ident: str, answer: str) -> dict[str, Any]:
    with _lock(ident):
        record = _record(ident)
        if record["status"] != "connecting" or time.time() > record["expires_at"]:
            raise ValueError("This authorization session expired. Start the connection again.")
        question = record["question"]
        if question == "config_token":
            answer = _authorization(answer.strip())
        elif question == "config_type":
            answer = "onedrive"
        elif question == "config_drive_ok":
            if answer != "true":
                raise ValueError("Confirm the selected OneDrive to continue.")
        elif question == "config_driveid":
            if answer not in {choice["value"] for choice in record["choices"]}:
                raise ValueError("Choose one of the available OneDrive accounts.")
        else:
            raise ValueError("Restart the cloud connection.")
        data = _invoke(record, ["update", "cw", "--continue", "--state", manager._decrypt_secret(record["state"])], answer)
        if data.get("Option", {}) and data["Option"].get("Name") == "config_type":
            data = _invoke(record, ["update", "cw", "--continue", "--state", data["State"]], "onedrive")
        return _progress(record, data)


@contextmanager
def transfer(destination: dict[str, Any]):
    ident = str(destination.get("connection") or "")
    with _lock(ident):
        connection(ident, destination["kind"])
        record = _record(ident)
        yield str(_path(ident)), _environment(record)
