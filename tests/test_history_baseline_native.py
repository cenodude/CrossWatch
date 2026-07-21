# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

import pytest

from cw_platform.id_map import canonical_key
from cw_platform.orchestrator.facade import Orchestrator


WATCHED = "2024-01-01T00:00:00Z"


def _simkl_event() -> dict[str, Any]:
    return {
        "type": "episode",
        "series_title": "Dragon Ball Z",
        "season": 1,
        "episode": 40,
        "watched_at": WATCHED,
        "show_ids": {"tmdb": "12971"},
        "ids": {},
    }


def _trakt_event() -> dict[str, Any]:
    return {
        "type": "episode",
        "series_title": "Dragon Ball Z",
        "season": 2,
        "episode": 1,
        "watched_at": WATCHED,
        "show_ids": {"tmdb": "12971"},
        "ids": {"trakt": "498798"},
    }


def _event_key(item: Mapping[str, Any]) -> str:
    return canonical_key(item)


@dataclass
class HistoryOps:
    provider: str
    index: dict[str, dict[str, Any]]
    feature: str = "history"
    translate_to: dict[str, Any] | None = None
    add_calls: list[list[dict[str, Any]]] = field(default_factory=list)
    remove_calls: list[list[dict[str, Any]]] = field(default_factory=list)
    view_calls: list[dict[str, Any]] = field(default_factory=list)

    def name(self) -> str:
        return self.provider

    def label(self) -> str:
        return self.provider

    def features(self) -> Mapping[str, bool]:
        return {self.feature: True}

    def capabilities(self) -> Mapping[str, Any]:
        return {
            "features": {self.feature: True},
            "observed_deletes": True,
            "index_semantics": "present",
        }

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return True

    def health(self, cfg: Mapping[str, Any], **_: Any) -> dict[str, Any]:
        return {"ok": True, "status": "ok", "features": {self.feature: True}, "api": {}}

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return {k: dict(v) for k, v in self.index.items()}

    def destination_comparison_view(
        self, cfg: Mapping[str, Any], *, feature: str, index: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        self.view_calls.append(dict(index))
        if not self.translate_to:
            return dict(index)
        dest_key = _event_key(self.translate_to)
        src_key = _event_key(_simkl_event())
        out: dict[str, Any] = {}
        for k, v in index.items():
            out[src_key if k == dest_key else k] = v
        return out

    def add(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        batch = [dict(x) for x in items]
        self.add_calls.append(batch)
        if dry_run:
            return {"ok": True, "count": len(batch)}

        confirmed_keys: list[str] = []
        destinations: dict[str, Any] = {}
        for it in batch:
            src_key = canonical_key(it)
            confirmed_keys.append(src_key)
            dest_item = dict(self.translate_to) if self.translate_to else dict(it)
            dest_key = _event_key(dest_item)
            self.index[dest_key] = dict(dest_item)
            destinations[src_key] = {"key": dest_key, "item": dest_item, "status": "added"}
        return {
            "ok": True,
            "count": len(batch),
            "confirmed_keys": confirmed_keys,
            "presence_confirmed_keys": confirmed_keys,
            "confirmed_destinations": destinations,
            "unresolved": [],
        }

    def remove(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        batch = [dict(x) for x in items]
        self.remove_calls.append(batch)
        return {"ok": True, "count": 0, "confirmed_keys": [], "unresolved": []}


def _cfg(feature: str) -> dict[str, Any]:
    return {
        "runtime": {
            "debug": False,
            "snapshot_ttl_sec": 0,
            "apply_chunk_size": 0,
            "apply_chunk_pause_ms": 0,
        },
        "sync": {
            "dry_run": False,
            "enable_add": True,
            "enable_remove": False,
            "include_observed_deletes": False,
            "allow_mass_delete": False,
        },
        "pairs": [
            {
                "id": "p1",
                "enabled": True,
                "source": "SRC",
                "target": "DST",
                "mode": "one-way",
                "feature": feature,
                "features": {feature: {"enable": True, "add": True, "remove": False}},
            }
        ],
    }


def _run(monkeypatch: pytest.MonkeyPatch, src: HistoryOps, dst: HistoryOps, feature: str) -> Orchestrator:
    monkeypatch.setattr(
        "cw_platform.orchestrator.facade.load_sync_providers",
        lambda: {"SRC": src, "DST": dst},
    )
    monkeypatch.setattr(
        "cw_platform.orchestrator._snapshots.provider_configured",
        lambda _cfg, _name: True,
    )
    orch = Orchestrator(_cfg(feature))
    orch.run()
    return orch


def _baseline(orch: Orchestrator, provider: str, feature: str) -> dict[str, Any]:
    st = orch.state_store.load_state() or {}
    block = ((st.get("providers") or {}).get(provider) or {}).get(feature) or {}
    return dict((block.get("baseline") or {}).get("items") or {})


def test_same_key_history_pair_converges_and_stays_native(
    config_base: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    event = _simkl_event()
    key = _event_key(event)
    src = HistoryOps("SRC", {key: dict(event)})
    dst = HistoryOps("DST", {})

    orch = _run(monkeypatch, src, dst, "history")

    assert len(dst.add_calls) == 1
    src_baseline = _baseline(orch, "SRC", "history")
    dst_baseline = _baseline(orch, "DST", "history")
    assert list(src_baseline) == [key]
    assert list(dst_baseline) == [key]
    assert dst_baseline[key]["season"] == 1
    assert dst_baseline[key]["episode"] == 40

    orch.run()
    assert len(dst.add_calls) == 1


def test_translated_history_pair_keeps_each_baseline_provider_native(
    config_base: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_event = _simkl_event()
    dest_event = _trakt_event()
    src_key = _event_key(source_event)
    dst_key = _event_key(dest_event)
    assert src_key != dst_key

    src = HistoryOps("SRC", {src_key: dict(source_event)})
    dst = HistoryOps("DST", {}, translate_to=dest_event)

    orch = _run(monkeypatch, src, dst, "history")

    src_baseline = _baseline(orch, "SRC", "history")
    dst_baseline = _baseline(orch, "DST", "history")

    assert list(src_baseline) == [src_key]
    assert src_baseline[src_key]["season"] == 1
    assert src_baseline[src_key]["episode"] == 40

    assert list(dst_baseline) == [dst_key]
    assert dst_baseline[dst_key]["season"] == 2
    assert dst_baseline[dst_key]["episode"] == 1
    assert dst_baseline[dst_key]["ids"]["trakt"] == "498798"
    assert src_key not in dst_baseline


def test_translated_history_pair_converges_on_second_run(
    config_base: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_event = _simkl_event()
    dest_event = _trakt_event()
    src = HistoryOps("SRC", {_event_key(source_event): dict(source_event)})
    dst = HistoryOps("DST", {}, translate_to=dest_event)

    orch = _run(monkeypatch, src, dst, "history")
    assert len(dst.add_calls) == 1

    orch.run()

    assert len(dst.add_calls) == 1
    assert dst.view_calls
    assert list(_baseline(orch, "DST", "history")) == [_event_key(dest_event)]


def test_ratings_baseline_still_rekeys_to_source_keyspace(
    config_base: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    src_item = {
        "type": "movie",
        "title": "A",
        "year": 2000,
        "ids": {"imdb": "tt01"},
        "rating": 8,
        "rated_at": WATCHED,
    }
    dst_item = {
        "type": "movie",
        "title": "A",
        "year": 2000,
        "ids": {"imdb": "tt01", "tmdb": "11"},
        "rating": 8,
        "rated_at": WATCHED,
    }
    src_key = canonical_key(src_item)
    dst_key = canonical_key(dst_item)
    assert src_key != dst_key

    src = HistoryOps("SRC", {src_key: dict(src_item)}, feature="ratings")
    dst = HistoryOps("DST", {dst_key: dict(dst_item)}, feature="ratings")

    orch = _run(monkeypatch, src, dst, "ratings")

    dst_baseline = _baseline(orch, "DST", "ratings")
    assert list(dst_baseline) == [src_key]
