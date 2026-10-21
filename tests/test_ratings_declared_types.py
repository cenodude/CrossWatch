# tests/test_ratings_declared_types.py
# CrossWatch - Ratings sync limited to the types a destination declares
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from cw_platform.id_map import canonical_key
from cw_platform.orchestrator import Orchestrator
from cw_platform.orchestrator._pairs_oneway import _ratings_filter_index
from cw_platform.orchestrator._pairs_utils import declared_types_for
from test_orchestrator_dry_run_no_side_effects import FakeOps, _cfg, _install

NO_SEASONS = {"movies": True, "shows": True, "seasons": False, "episodes": False}
MOVIE = {"type": "movie", "title": "The Matrix", "year": 1999, "ids": {"tmdb": "603"}, "rating": 8, "rated_at": "2026-10-01T00:00:00Z"}
SEASON = {"type": "season", "title": "Breaking Bad", "show_ids": {"tmdb": "1396"}, "season": 2, "rating": 7, "rated_at": "2026-10-01T00:00:00Z"}


@dataclass
class RatingOps(FakeOps):
    types: dict[str, bool] | None = None
    removed: list[list[dict[str, Any]]] = field(default_factory=list)

    def features(self) -> dict[str, bool]:
        return {"ratings": True}

    def capabilities(self) -> dict[str, Any]:
        ratings: dict[str, Any] = {"index_semantics": "present"}
        if self.types is not None:
            ratings["types"] = self.types
        return {"features": {"ratings": True}, "index_semantics": "present", "ratings": ratings}

    def health(self, cfg: Any, **_: Any) -> dict[str, Any]:
        return {"ok": True, "status": "ok", "features": {"ratings": True}, "api": {}}

    def remove(self, cfg: Any, items: Any, *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        batch = [dict(x) for x in items]
        self.removed.append(batch)
        return {"ok": True, "count": len(batch)}


def _ratings_cfg(mode: str = "one-way") -> dict[str, Any]:
    cfg = _cfg(False)
    pair = cfg["pairs"][0]
    pair.update(mode=mode, feature="ratings", features={"ratings": {"enable": True, "add": True, "remove": False, "types": ["movies", "shows", "seasons", "episodes"], "mode": "all"}})
    return cfg


def _added_types(ops: RatingOps) -> list[str]:
    return sorted(str(it.get("type")) for batch in ops.add_calls for it in batch)


def test_one_way_only_sends_declared_types(config_base: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = RatingOps("SRC", {canonical_key(MOVIE): dict(MOVIE), canonical_key(SEASON): dict(SEASON)})
    dst = RatingOps("DST", {}, types=NO_SEASONS)
    _install(monkeypatch, src, dst, config_base / ".cw_state")

    assert not Orchestrator(_ratings_cfg()).run()["errors"]
    assert _added_types(dst) == ["movie"]


def test_one_way_manual_adds_respect_declared_types(config_base: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = RatingOps("SRC", {canonical_key(MOVIE): dict(MOVIE)})
    dst = RatingOps("DST", {}, types=NO_SEASONS)
    _install(monkeypatch, src, dst, config_base / ".cw_state")
    manual = {canonical_key(SEASON): dict(SEASON)}
    monkeypatch.setattr("cw_platform.orchestrator._pairs_oneway._manual_policy", lambda _state, provider, *_a, **_k: (manual if provider == "SRC" else {}, set()))

    assert not Orchestrator(_ratings_cfg()).run()["errors"]
    assert _added_types(dst) == ["movie"]


def test_two_way_uses_types_both_sides_declare(config_base: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = RatingOps("SRC", {canonical_key(MOVIE): dict(MOVIE), canonical_key(SEASON): dict(SEASON)})
    dst = RatingOps("DST", {}, types=NO_SEASONS)
    _install(monkeypatch, src, dst, config_base / ".cw_state")

    assert not Orchestrator(_ratings_cfg("two-way")).run()["errors"]
    assert _added_types(dst) == ["movie"]
    assert _added_types(src) == []


def test_two_way_remove_retry_skips_hidden_types(config_base: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from cw_platform.orchestrator import _pairs_twoway as twoway

    src = RatingOps("SRC", {canonical_key(MOVIE): dict(MOVIE), canonical_key(SEASON): dict(SEASON)})
    dst = RatingOps("DST", {canonical_key(MOVIE): dict(MOVIE)}, types=NO_SEASONS)
    _install(monkeypatch, src, dst, config_base / ".cw_state")
    pending = [{"key": canonical_key(SEASON), "item": dict(SEASON), "reason": "apply:remove:failed"}]
    monkeypatch.setattr(twoway, "load_unresolved_pending", lambda provider, *_a, **_k: pending if provider == "SRC" else [])
    monkeypatch.setattr(twoway, "is_remove_retry_reason", lambda _reason: True)
    cfg = _ratings_cfg("two-way")
    cfg["pairs"][0]["features"]["ratings"]["remove"] = True

    assert not Orchestrator(cfg).run()["errors"]
    assert [it for batch in src.removed for it in batch] == []


class _Ops:
    def __init__(self, caps: Any) -> None:
        self._caps = caps

    def capabilities(self) -> Any:
        return self._caps


INDEX = {
    "tmdb:155": {"type": "movie", "rating": 8},
    "tmdb:1396#show": {"type": "show", "rating": 9},
    "tmdb:1396#season:2": {"type": "season", "rating": 7},
    "tmdb:1396#s01e01": {"type": "episode", "rating": 6},
    "mal:1": {"type": "anime", "rating": 5},
}


def test_declared_types_reads_dict_and_list_shapes() -> None:
    assert declared_types_for(_Ops({"ratings": {"types": {"movies": True, "shows": True, "seasons": False, "episodes": True}}}), "ratings") == {"movie", "show", "episode"}
    assert declared_types_for(_Ops({"ratings": {"types": ["movie", "show", "season", "episode"]}}), "ratings") == {"movie", "show", "season", "episode"}


def test_undeclared_types_allow_everything() -> None:
    assert declared_types_for(_Ops({"ratings": {"read": True}}), "ratings") is None
    assert declared_types_for(_Ops({}), "ratings") is None
    assert _ratings_filter_index(dict(INDEX), {"types": ["movies", "shows", "seasons", "episodes"]}, None) == INDEX


def test_filter_drops_types_the_destination_cannot_take() -> None:
    fcfg = {"types": ["movies", "shows", "seasons", "episodes"]}

    out = _ratings_filter_index(dict(INDEX), fcfg, {"movie", "show", "episode"})

    assert sorted(out) == ["mal:1", "tmdb:1396#s01e01", "tmdb:1396#show", "tmdb:155"]


def test_every_ratings_adapter_declares_what_the_ui_allows() -> None:
    import importlib
    import re
    from pathlib import Path

    js = Path("assets/js/modals/pair-config/custom-rules.js").read_text(encoding="utf-8")
    block = js.split("const RATINGS_TYPE_RULES = {", 1)[1].split("};", 1)[0]
    ui = {name: set(re.findall(r'"(\w+)"', rest)) for name, rest in re.findall(r"(\w+): \{ disable: \[([^\]]*)\] \}", block)}
    names = {"movies", "shows", "seasons", "episodes"}
    singular = {"movies": "movie", "shows": "show", "seasons": "season", "episodes": "episode"}

    for path in sorted(Path("providers/sync").glob("_mod_*.py")):
        name = path.stem[5:]
        if name == "common":
            continue
        ops = importlib.import_module(f"providers.sync._mod_{name}").OPS
        if not dict(ops.features()).get("ratings"):
            continue
        allowed = {singular[t] for t in names - ui.get(name, set())}
        assert declared_types_for(ops, "ratings") == allowed, name


def test_pair_types_and_declared_types_both_apply() -> None:
    out = _ratings_filter_index(dict(INDEX), {"types": ["movies", "seasons"]}, {"movie", "show"})

    assert sorted(out) == ["tmdb:155"]
