# tests/test_continue_watching_removals.py
# CrossWatch - Continue Watching removals between media servers: detection guards, planning and hide flow
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from cw_platform.local_db import continue_rows as store
from cw_platform.orchestrator import _continue_watching as cw
from providers.sync import _continue_watching as registry

SHOWN = registry.SHOWN
OLD = "2026-11-01T10:00:00Z"
MID = "2026-11-05T10:00:00Z"
NEW = "2026-11-08T10:00:00Z"


def _movie(tmdb: str, at: str, ms: int = 600_000, shown: bool | None = True) -> dict[str, Any]:
    return {"type": "movie", "title": f"Movie {tmdb}", "ids": {"tmdb": tmdb}, "progress_ms": ms, "progress_at": at, SHOWN: shown}


def _episode(tvdb: str, s: int, e: int, at: str, ms: int = 600_000, shown: bool | None = True) -> dict[str, Any]:
    return {"type": "episode", "series_title": f"Show {tvdb}", "show_ids": {"tvdb": tvdb}, "ids": {},
            "season": s, "episode": e, "progress_ms": ms, "progress_at": at, SHOWN: shown}


class Servers:
    def __init__(self, views: dict[str, dict[str, dict[str, Any]]]):
        self.views = views
        self.hidden: list[tuple[str, list[str]]] = []
        self.fail_read: set[str] = set()
        self.fail_hide = 0

    def read(self, provider: str, _ops: Any, _cfg: Any) -> dict[str, dict[str, Any]]:
        if provider in self.fail_read:
            raise RuntimeError("read failed")
        return {k: dict(v) for k, v in self.views[provider].items()}

    def hide(self, provider: str, _ops: Any, _cfg: Any, items: list[dict[str, Any]]) -> dict[str, Any]:
        keys = [str(it["_cw_key"]) for it in items]
        if self.fail_hide:
            self.fail_hide -= 1
            return {"attempted": len(items), "confirmed_keys": [], "errors": len(items)}
        self.hidden.append((provider, keys))
        for key in keys:
            self.views[provider][key][SHOWN] = False
        return {"attempted": len(items), "confirmed_keys": keys, "errors": 0}


@pytest.fixture
def servers(monkeypatch) -> Servers:
    srv = Servers({"EMBY": {}, "PLEX": {}})
    monkeypatch.setattr(registry, "read_view", srv.read)
    monkeypatch.setattr(registry, "hide", srv.hide)
    return srv


@pytest.fixture
def state_store(config_base):
    return SimpleNamespace(pair_scope="EMBY-PLEX|progress", base_path=config_base)


def _run(state_store, *, two_way: bool = False, dry_run: bool = False, fcfg: dict[str, Any] | None = None,
         blocked: set[str] | None = None) -> tuple[int, list[tuple[str, dict[str, Any]]]]:
    events: list[tuple[str, dict[str, Any]]] = []
    hidden = cw.sync_removals(state_store, src=("EMBY", "default", object(), {}), dst=("PLEX", "default", object(), {}),
                              two_way=two_way, dry_run=dry_run, emit=lambda name, **f: events.append((name, f)),
                              fcfg=fcfg, blocked=blocked)
    return hidden, events


def test_first_run_only_takes_a_snapshot(servers, state_store):
    servers.views["EMBY"] = {"m1": _movie("1", NEW, shown=False), "m2": _movie("2", NEW)}
    servers.views["PLEX"] = {"p1": _movie("1", MID)}

    assert _run(state_store)[0] == 0
    assert servers.hidden == []
    assert store.load(state_store.base_path, state_store.pair_scope, "EMBY", "default")["items"]["m1"]["shown"] is False


def test_one_way_hides_a_removed_movie_once(servers, state_store):
    servers.views["EMBY"] = {"m1": _movie("1", NEW), "m2": _movie("2", MID)}
    servers.views["PLEX"] = {"p1": _movie("1", MID)}
    _run(state_store)

    servers.views["EMBY"]["m1"][SHOWN] = False
    hidden, events = _run(state_store)

    assert hidden == 1
    assert servers.hidden == [("PLEX", ["p1"])]
    assert ("continue_watching:hidden", {"dst": "PLEX", "destination_instance": "default", "attempted": 1,
                                         "confirmed": 1, "errors": 0, "items": ["Movie 1"]}) in events
    assert _run(state_store)[0] == 0
    assert len(servers.hidden) == 1


@pytest.mark.parametrize("change", [
    {"progress_ms": 900_000},
    {"progress_at": "2026-11-09T10:00:00Z"},
    {"progress_at": ""},
])
def test_watching_further_is_not_a_removal(servers, state_store, change):
    servers.views["EMBY"] = {"m1": _movie("1", NEW), "m2": _movie("2", MID)}
    servers.views["PLEX"] = {"p1": _movie("1", MID)}
    _run(state_store)

    servers.views["EMBY"]["m1"].update({SHOWN: False, **change})

    assert _run(state_store)[0] == 0
    assert servers.hidden == []


def test_items_older_than_the_row_are_left_alone(servers, state_store):
    servers.views["EMBY"] = {"m1": _movie("1", OLD), "m2": _movie("2", NEW)}
    servers.views["PLEX"] = {"p1": _movie("1", OLD)}
    _run(state_store)

    servers.views["EMBY"]["m1"][SHOWN] = False

    assert _run(state_store)[0] == 0


def test_an_empty_row_is_never_trusted(servers, state_store):
    servers.views["EMBY"] = {"m1": _movie("1", NEW)}
    servers.views["PLEX"] = {"p1": _movie("1", MID)}
    _run(state_store)

    servers.views["EMBY"]["m1"][SHOWN] = False

    assert _run(state_store)[0] == 0


def test_unknown_items_are_not_removals(servers, state_store):
    servers.views["EMBY"] = {"m1": _movie("1", NEW), "m2": _movie("2", MID)}
    servers.views["PLEX"] = {"p1": _movie("1", MID)}
    _run(state_store)

    servers.views["EMBY"]["m1"][SHOWN] = None

    assert _run(state_store)[0] == 0


def test_newer_progress_on_the_target_wins(servers, state_store):
    servers.views["EMBY"] = {"m1": _movie("1", MID), "m2": _movie("2", OLD)}
    servers.views["PLEX"] = {"p1": _movie("1", NEW)}
    _run(state_store)

    servers.views["EMBY"]["m1"][SHOWN] = False

    assert _run(state_store)[0] == 0


def test_target_items_already_off_the_row_are_skipped(servers, state_store):
    servers.views["EMBY"] = {"m1": _movie("1", NEW), "m2": _movie("2", MID)}
    servers.views["PLEX"] = {"p1": _movie("1", MID, shown=False)}
    _run(state_store)

    servers.views["EMBY"]["m1"][SHOWN] = False

    assert _run(state_store)[0] == 0


def test_episodes_match_on_show_ids_and_numbering(servers, state_store):
    servers.views["EMBY"] = {"e1": _episode("9", 1, 2, NEW), "m2": _movie("2", MID)}
    servers.views["PLEX"] = {"p3": _episode("9", 1, 3, MID), "p2": _episode("9", 1, 2, MID)}
    _run(state_store)

    servers.views["EMBY"]["e1"][SHOWN] = False

    assert _run(state_store)[0] == 1
    assert servers.hidden == [("PLEX", ["p2"])]


def test_one_way_ignores_removals_on_the_target(servers, state_store):
    servers.views["EMBY"] = {"m1": _movie("1", MID)}
    servers.views["PLEX"] = {"p1": _movie("1", NEW), "p2": _movie("2", MID)}
    _run(state_store)

    servers.views["PLEX"]["p1"][SHOWN] = False

    assert _run(state_store)[0] == 0


def test_two_way_hides_in_both_directions_without_echo(servers, state_store):
    servers.views["EMBY"] = {"m1": _movie("1", MID), "m2": _movie("2", NEW), "m3": _movie("3", MID)}
    servers.views["PLEX"] = {"p1": _movie("1", NEW), "p2": _movie("2", MID), "p3": _movie("3", MID)}
    _run(state_store, two_way=True)

    servers.views["PLEX"]["p1"][SHOWN] = False
    assert _run(state_store, two_way=True)[0] == 1
    assert servers.hidden == [("EMBY", ["m1"])]

    assert _run(state_store, two_way=True)[0] == 0

    servers.views["EMBY"]["m2"][SHOWN] = False
    assert _run(state_store, two_way=True)[0] == 1
    assert servers.hidden[-1] == ("PLEX", ["p2"])
    assert _run(state_store, two_way=True)[0] == 0
    assert len(servers.hidden) == 2


def test_own_hides_are_not_read_back_as_removals_in_the_same_run(servers, state_store):
    servers.views["EMBY"] = {"m1": _movie("1", NEW), "m2": _movie("2", MID)}
    servers.views["PLEX"] = {"p1": _movie("1", MID), "p2": _movie("2", MID)}
    _run(state_store, two_way=True)

    servers.views["EMBY"]["m1"][SHOWN] = False
    hidden, events = _run(state_store, two_way=True)

    detected = [f for name, f in events if name == "debug" and f.get("msg") == "continue_watching.detected"]
    assert hidden == 1
    assert [(f["provider"], f["removed"]) for f in detected] == [("EMBY", 1), ("PLEX", 0)]


def test_old_episodes_behind_a_newer_card_do_not_widen_the_row_age(servers, state_store):
    servers.views["EMBY"] = {"e-old": _episode("9", 1, 1, OLD), "e-new": _episode("9", 1, 2, NEW), "m1": _movie("1", MID)}
    servers.views["PLEX"] = {"p1": _movie("1", OLD)}
    _run(state_store)

    servers.views["EMBY"]["m1"][SHOWN] = False

    assert _run(state_store)[0] == 0


def test_newer_progress_anywhere_in_the_target_show_wins(servers, state_store):
    servers.views["EMBY"] = {"e1": _episode("9", 1, 1, MID), "m2": _movie("2", OLD)}
    servers.views["PLEX"] = {"p1": _episode("9", 1, 1, OLD), "p2": _episode("9", 1, 2, NEW)}
    _run(state_store)

    servers.views["EMBY"]["e1"][SHOWN] = False

    assert _run(state_store)[0] == 0


def test_excluded_specials_are_left_alone(servers, state_store):
    servers.views["EMBY"] = {"e0": _episode("9", 0, 1, NEW), "m2": _movie("2", MID)}
    servers.views["PLEX"] = {"p0": _episode("9", 0, 1, MID)}
    _run(state_store, fcfg={"include_specials": False})

    servers.views["EMBY"]["e0"][SHOWN] = False

    assert _run(state_store, fcfg={"include_specials": False})[0] == 0
    assert _run(state_store, fcfg={"include_specials": True})[0] == 0


def test_excluded_specials_still_count_as_newer_progress(servers, state_store):
    servers.views["EMBY"] = {"e1": _episode("9", 1, 1, MID), "m2": _movie("2", OLD)}
    servers.views["PLEX"] = {"p1": _episode("9", 1, 1, OLD), "p0": _episode("9", 0, 1, NEW)}
    _run(state_store, fcfg={"include_specials": False})

    servers.views["EMBY"]["e1"][SHOWN] = False

    assert _run(state_store, fcfg={"include_specials": False})[0] == 0


def test_manually_blocked_items_are_left_alone(servers, state_store):
    servers.views["EMBY"] = {"m1": _movie("1", NEW), "m2": _movie("2", MID)}
    servers.views["PLEX"] = {"p1": _movie("1", MID)}
    _run(state_store, blocked={"tmdb:1"})

    servers.views["EMBY"]["m1"][SHOWN] = False

    assert _run(state_store, blocked={"tmdb:1"})[0] == 0
    assert servers.hidden == []


def test_a_failed_hide_is_retried_on_the_next_run(servers, state_store):
    servers.views["EMBY"] = {"m1": _movie("1", NEW), "m2": _movie("2", MID)}
    servers.views["PLEX"] = {"p1": _movie("1", MID)}
    _run(state_store)

    servers.views["EMBY"]["m1"][SHOWN] = False
    servers.fail_hide = 1
    assert _run(state_store)[0] == 0

    assert _run(state_store)[0] == 1
    assert servers.hidden == [("PLEX", ["p1"])]
    assert _run(state_store)[0] == 0


def test_dry_run_writes_nothing(servers, state_store):
    servers.views["EMBY"] = {"m1": _movie("1", NEW), "m2": _movie("2", MID)}
    servers.views["PLEX"] = {"p1": _movie("1", MID)}
    _run(state_store)

    servers.views["EMBY"]["m1"][SHOWN] = False
    _hidden, events = _run(state_store, dry_run=True)

    assert servers.hidden == []
    assert any(name == "debug" and f.get("msg") == "continue_watching.hide" and f.get("planned") == 1 for name, f in events)
    assert _run(state_store)[0] == 1


def test_a_failed_read_keeps_the_last_snapshot(servers, state_store):
    servers.views["EMBY"] = {"m1": _movie("1", NEW), "m2": _movie("2", MID)}
    servers.views["PLEX"] = {"p1": _movie("1", MID)}
    _run(state_store)

    servers.views["EMBY"]["m1"][SHOWN] = False
    servers.fail_read = {"PLEX"}
    assert _run(state_store)[0] == 0

    servers.fail_read = set()
    assert _run(state_store)[0] == 1


def test_unsupported_pairs_do_nothing(state_store):
    events: list[tuple[str, dict[str, Any]]] = []
    hidden = cw.sync_removals(state_store, src=("JELLYFIN", "default", object(), {}), dst=("PLEX", "default", object(), {}),
                              two_way=False, dry_run=False, emit=lambda name, **f: events.append((name, f)))

    assert hidden == 0
    assert events == [("debug", {"msg": "continue_watching.unsupported_pair", "src": "JELLYFIN", "dst": "PLEX"})]


def test_option_is_progress_only():
    assert cw.requested("progress", {"continue_watching_removals": True})
    assert not cw.requested("history", {"continue_watching_removals": True})
    assert not cw.requested("progress", {})


def test_capabilities_cover_the_hide_capable_servers():
    assert registry.provider_caps("EMBY") == {"opt_in": "progress.continue_watching_removals", "hide_unit": "series", "beta": True}
    assert registry.provider_caps("TOFA")["hide_unit"] == "title"
    assert registry.provider_caps("JELLYFIN") == {}
    assert registry.provider_caps("STREMIO") == {}


class FakeResponse:
    def __init__(self, body: Any, status: int = 200):
        self.status_code = status
        self._body = body

    def json(self) -> Any:
        return self._body


class FakeEmby:
    def __init__(self, resume: list[dict[str, Any]], series: dict[str, str], played: dict[str, str]):
        self.resume, self.series, self.played = resume, series, played
        self.posts: list[tuple[str, dict[str, Any]]] = []

    def get(self, path: str, *, params: dict[str, Any] | None = None) -> FakeResponse:
        params = params or {}
        if path.endswith("/Items/Resume"):
            return FakeResponse({"Items": self.resume})
        if "Ids" in params:
            return FakeResponse({"Items": [{"Id": i, "SeriesId": self.series[i]} for i in params["Ids"].split(",") if i in self.series]})
        stamp = self.played.get(str(params.get("ParentId")))
        return FakeResponse({"Items": [{"Id": "x", "UserData": {"LastPlayedDate": stamp}}] if stamp else []})

    def post(self, path: str, *, params: dict[str, Any] | None = None, json: Any = None) -> FakeResponse:
        self.posts.append((path, dict(params or {})))
        return FakeResponse(None, 204)


def test_emby_reads_episodes_by_series_and_guards_later_plays(monkeypatch):
    from providers.sync.emby import _continue, _progress

    index = {
        "movie-shown": {"type": "movie", "emby_item_id": "10", "progress_at": NEW},
        "movie-hidden": {"type": "movie", "emby_item_id": "11", "progress_at": NEW},
        "ep-shown": {"type": "episode", "emby_item_id": "20", "progress_at": NEW},
        "ep-hidden": {"type": "episode", "emby_item_id": "21", "progress_at": MID},
        "ep-later": {"type": "episode", "emby_item_id": "22", "progress_at": MID},
        "ep-unknown": {"type": "episode", "emby_item_id": "23", "progress_at": MID},
    }
    monkeypatch.setattr(_progress, "build_index", lambda _adapter: index)
    http = FakeEmby(
        resume=[{"Id": "10", "Type": "Movie"}, {"Id": "99", "Type": "Episode", "SeriesId": "s20"}],
        series={"20": "s20", "21": "s21", "22": "s22"},
        played={"s22": NEW},
    )
    adapter = SimpleNamespace(client=http, cfg=SimpleNamespace(user_id="u"))

    view = _continue.view(adapter)

    assert {k: v[SHOWN] for k, v in view.items()} == {
        "movie-shown": True, "movie-hidden": False, "ep-shown": True,
        "ep-hidden": False, "ep-later": None, "ep-unknown": None,
    }
    res = _continue.hide(adapter, [{**view["movie-hidden"], "_cw_key": "movie-hidden"}])
    assert res == {"attempted": 1, "confirmed_keys": ["movie-hidden"], "errors": 0}
    assert http.posts == [("/Users/u/Items/11/HideFromResume", {"Hide": "true"})]


def test_silo_dismisses_next_up_cards_by_series():
    from providers.sync.silo._continue import _dismissal

    assert _dismissal({"content_id": "e1", "series_id": "s1", "item_source": "next_up"}) == ("next_up", {"series_id": "s1"})
    assert _dismissal({"content_id": "m1", "item_source": "in_progress", "progress_updated_at": NEW}) == (
        "continue_watching", {"progress_updated_at": NEW})
    assert _dismissal({"content_id": "m1", "item_source": "in_progress"}) is None
