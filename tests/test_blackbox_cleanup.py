from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from cw_platform.orchestrator import _blackbox

CFG = {"sync": {"blackbox": {"enabled": True, "promote_after": 2, "pair_scoped": True, "cooldown_days": 30}}}


def _read(p: Path) -> dict[str, Any]:
    return json.loads(p.read_text("utf-8")) if p.exists() else {}


def _write(p: Path, data: dict[str, Any]) -> None:
    p.write_text(json.dumps(data), encoding="utf-8")


def test_promotion_drops_flap_row(monkeypatch: Any, tmp_path: Path) -> None:
    monkeypatch.setattr(_blackbox, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_SCOPE", "cw2_abc")
    for _ in range(2):
        res = _blackbox.record_attempts("SIMKL", "history", ["tmdb:1"], pair="CW2_ABC", cfg=CFG)
    assert res["promoted_keys"] == ["tmdb:1"]
    assert "tmdb:1" in _read(tmp_path / "simkl_history.cw2_abc.blackbox.json")
    assert "tmdb:1" not in _read(tmp_path / "simkl_history.cw2_abc.flap.json")


def test_success_clears_blackbox_and_flap_in_every_scope(monkeypatch: Any, tmp_path: Path) -> None:
    monkeypatch.setattr(_blackbox, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_SCOPE", "cw2_new")
    _write(tmp_path / "simkl_history.cw2_old.blackbox.json", {"tmdb:1": {"reason": "x", "since": 1}, "tmdb:2": {"reason": "x", "since": 1}})
    _write(tmp_path / "simkl_history.crosswatch-simkl.blackbox.json", {"tmdb:1": {"reason": "x", "since": 1}})
    _write(tmp_path / "simkl_history.cw2_old.flap.json", {"tmdb:1": {"consecutive": 3}})
    _write(tmp_path / "simkl_watchlist.cw2_old.blackbox.json", {"tmdb:1": {"reason": "x", "since": 1}})

    res = _blackbox.record_success("SIMKL", "history", ["tmdb:1"], pair="CW2_NEW", cfg=CFG)

    assert res["blackbox_removed"] == 2
    assert _read(tmp_path / "simkl_history.cw2_old.blackbox.json") == {"tmdb:2": {"reason": "x", "since": 1}}
    assert _read(tmp_path / "simkl_history.crosswatch-simkl.blackbox.json") == {}
    assert _read(tmp_path / "simkl_history.cw2_old.flap.json") == {}
    assert "tmdb:1" in _read(tmp_path / "simkl_watchlist.cw2_old.blackbox.json")
    assert not (tmp_path / "simkl_history.cw2_new.flap.json").exists()


def test_prune_drops_idle_stale_and_blackboxed_flap_rows(monkeypatch: Any, tmp_path: Path) -> None:
    monkeypatch.setattr(_blackbox, "STATE_DIR", tmp_path)
    now = int(time.time())
    _write(tmp_path / "simkl_history.cw2_a.blackbox.json", {"tmdb:3": {"reason": "x", "since": now}})
    _write(
        tmp_path / "simkl_history.cw2_a.flap.json",
        {
            "tmdb:1": {"consecutive": 0, "last_success_ts": now},
            "tmdb:2": {"consecutive": 2, "last_attempt_ts": now - 40 * 86400},
            "tmdb:3": {"consecutive": 3, "last_attempt_ts": now},
            "tmdb:4": {"consecutive": 1, "last_attempt_ts": now},
        },
    )

    _blackbox.prune_blackbox(cooldown_days=30)

    assert list(_read(tmp_path / "simkl_history.cw2_a.flap.json")) == ["tmdb:4"]
    assert "tmdb:3" in _read(tmp_path / "simkl_history.cw2_a.blackbox.json")


def test_clear_unresolved_all_scopes(monkeypatch: Any, tmp_path: Path) -> None:
    from cw_platform.orchestrator import _unresolved

    monkeypatch.setattr(_unresolved, "STATE_DIR", tmp_path)
    _write(tmp_path / "simkl_history.unresolved.cw2_a.json", {"tmdb:1": {"hint": "x"}, "tmdb:2": {"hint": "x"}})
    _write(tmp_path / "simkl_history.unresolved.unscoped.json", {"tmdb:1": {"hint": "x"}})
    _write(tmp_path / "simkl_history.unresolved.pending.cw2_a.json", {"keys": ["tmdb:1"], "items": {"tmdb:1": {}}, "hints": {"tmdb:1": "x"}})
    _write(tmp_path / "simkl_watchlist.unresolved.cw2_a.json", {"tmdb:1": {"hint": "x"}})

    res = _unresolved.clear_unresolved_all_scopes("SIMKL", "history", ["tmdb:1"])

    assert res["count"] == 3
    assert list(_read(tmp_path / "simkl_history.unresolved.cw2_a.json")) == ["tmdb:2"]
    assert _read(tmp_path / "simkl_history.unresolved.unscoped.json") == {}
    assert _read(tmp_path / "simkl_history.unresolved.pending.cw2_a.json") == {"keys": [], "items": {}, "hints": {}}
    assert "tmdb:1" in _read(tmp_path / "simkl_watchlist.unresolved.cw2_a.json")


def test_orphaned_unresolved_scope() -> None:
    from services.analyzer import _orphaned_unresolved_scope

    active = {"cw2_a"}
    assert not _orphaned_unresolved_scope({"scope": "cw2_a"}, active)
    assert not _orphaned_unresolved_scope({"scope": "pending.cw2_a"}, active)
    assert not _orphaned_unresolved_scope({}, active)
    assert _orphaned_unresolved_scope({"scope": "cw2_old"}, active)
    assert _orphaned_unresolved_scope({"scope": "pending.unscoped"}, active)
