from __future__ import annotations

import sys
import types

from providers.sync import _log as sync_log


def _clear_log_env(monkeypatch) -> None:
    for name in (
        "CW_LOG_FORMAT",
        "CW_LOG_LEVEL",
        "CW_SIMKL_LOG_LEVEL",
        "CW_DEBUG",
        "CW_SIMKL_DEBUG",
        "NO_COLOR",
    ):
        monkeypatch.delenv(name, raising=False)


def test_sync_provider_log_keeps_stdout_and_mirrors_to_ui(monkeypatch, capsys) -> None:
    _clear_log_env(monkeypatch)
    monkeypatch.setenv("CW_LOG_COLOR", "0")
    mirrored: list[tuple[str, str]] = []
    fake_crosswatch = types.SimpleNamespace(
        _append_log=lambda tag, line: mirrored.append((tag, line))
    )
    monkeypatch.setitem(sys.modules, "crosswatch", fake_crosswatch)

    sync_log.log("simkl", "history", "info", "fetch_failed", status=500)

    out = capsys.readouterr().out.strip()
    assert out == '[SIMKL:history] INFO fetch_failed status=500'
    assert mirrored == [("SIMKL", out)]


def test_sync_provider_json_log_keeps_stdout_and_mirrors_to_ui(monkeypatch, capsys) -> None:
    _clear_log_env(monkeypatch)
    monkeypatch.setenv("CW_LOG_FORMAT", "json")
    mirrored: list[tuple[str, str]] = []
    fake_crosswatch = types.SimpleNamespace(
        _append_log=lambda tag, line: mirrored.append((tag, line))
    )
    monkeypatch.setitem(sys.modules, "crosswatch", fake_crosswatch)

    sync_log.log("simkl", "history", "warn", "rate_limited", status=429)

    out = capsys.readouterr().out.strip()
    assert '"provider": "SIMKL"' in out
    assert '"feature": "history"' in out
    assert '"level": "WARN"' in out
    assert '"status": 429' in out
    assert mirrored == [("SIMKL", out)]
