from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace

import pytest

from cw_platform.log_context import log_run_id
from providers.sync.jellyfin import _common as common, _id_lookup as lookup


def item(iid, *, kind="Movie", external="1", library="A", changed=900, **extra):
    return {"Id": iid, "Type": kind, "Name": "English title", "ProductionYear": 2026,
            "ProviderIds": {"Tmdb": external}, "LibraryId": library, "changed": changed, **extra}


class Server:
    def __init__(self, *rows):
        self.rows = {row["Id"]: row for row in rows}
        self.episodes = {}
        self.calls = []
        self.fail = False
        self.repeat = False
        self.ignore_delta = False

    def get(self, path, *, params):
        self.calls.append((path, dict(params)))
        assert "SearchTerm" not in params, "Strict matching must never search titles"
        assert "AnyProviderIdEquals" not in params
        if self.fail:
            return SimpleNamespace(status_code=503, json=lambda: {})
        if path == "/Items":
            if params.get("Ids"):
                rows = [row for iid, row in self.rows.items() if iid in params["Ids"].split(",")]
            else:
                assert params["IncludeItemTypes"] == "Movie,Series", "No library-wide episode scan"
                rows = [row for row in self.rows.values() if not params.get("ParentId") or row["LibraryId"] == params["ParentId"]]
            if params.get("MinDateLastSaved") and not self.ignore_delta:
                after = datetime.fromisoformat(params["MinDateLastSaved"]).timestamp()
                rows = [row for row in rows if row["changed"] >= after]
        elif path.startswith("/Shows/"):
            rows = self.episodes.get(path.split("/")[2], [])
        elif path.endswith("/Ancestors"):
            row = self.rows.get(path.split("/")[2])
            return SimpleNamespace(status_code=200, json=lambda: [{"Id": row["LibraryId"]}] if row else [])
        else:
            row = self.rows.get(path.split("/")[2])
            return SimpleNamespace(status_code=200 if row else 404, json=lambda: dict(row or {}))
        start = 0 if self.repeat else params.get("StartIndex", 0)
        page = [dict(row) for row in rows[start:start + params["Limit"]]]
        # Jellyfin may report the page size when total counting is disabled.
        return SimpleNamespace(status_code=200, json=lambda: {"Items": page, "TotalRecordCount": len(page)})


def adapter(http, pair="pair-A", **config):
    return SimpleNamespace(client=http, config={"_cw_pair_scope": pair}, cfg=SimpleNamespace(**{
        "server": "http://jellyfin", "user_id": "user-A", "access_token": "private-token",
        "targeted_lookup": True, "strict_id_matching": True, **config,
    }))


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    token = log_run_id.set("id-run-1")
    monkeypatch.setattr(common, "STATE_DIR", tmp_path)
    monkeypatch.setattr(common._TARGETED_CACHE, "entry", None, raising=False)
    monkeypatch.setattr(common, "_pair_scope", lambda: None)
    monkeypatch.setattr(common, "build_provider_index", lambda *a, **kw: pytest.fail("Full episode/path index"))
    monkeypatch.setattr(lookup.time, "time", lambda: 1000.0)
    yield
    log_run_id.reset(token)


@pytest.mark.parametrize("title", [None, "Een andere titel"])
@pytest.mark.parametrize("feature", ["history", "progress"])
@pytest.mark.parametrize("strict", [True, False])
def test_movies_match_only_ids_despite_titles_years_and_duplicate_copies(title, feature, strict):
    server = Server(item("a"), item("b"), item("wrong", external="2"), item("series", kind="Series"))
    source = {"type": "movie", "year": 2000, "ids": {"tmdb": "1"}}
    if title:
        source["title"] = title
    ad = adapter(server, strict_id_matching=strict)
    assert common.resolve_item_ids(ad, source, feature=feature) == ["a", "b"]
    assert common.resolve_item_ids(ad, source, feature=feature) == ["a", "b"]
    assert len(server.calls) == 2  # Initial catalogue and delta-support probe, reused across batches.
    assert source.get("title") == title


@pytest.mark.parametrize("title", [None, "Achter de attractie"])
def test_series_id_and_coordinates_work_without_matching_title(title):
    server = Server(item("series", kind="Series", external="95738"))
    server.episodes["series"] = [item(str(n), kind="Episode", external=str(n + 100),
                                      SeriesId="series", ParentIndexNumber=1, IndexNumber=n) for n in range(1, 4)]
    for n in range(1, 4):
        source = {"type": "episode", "series_title": title, "season": 1, "episode": n,
                  "show_ids": {"tmdb": "95738"}}
        assert common.resolve_item_id(adapter(server), source) == str(n)
    assert len(server.calls) == 3


def test_episode_id_leads_when_servers_use_different_numbering():
    server = Server(item("series", kind="Series", external="95738"))
    server.episodes["series"] = [
        item("wrong", kind="Episode", external="101", SeriesId="series", ParentIndexNumber=1, IndexNumber=1),
        item("correct", kind="Episode", external="102", SeriesId="series", ParentIndexNumber=2, IndexNumber=4),
    ]
    source = {"type": "episode", "season": 1, "episode": 1,
              "show_ids": {"tmdb": "95738"}, "ids": {"tmdb": "102"}}
    assert common.resolve_item_id(adapter(server), source) == "correct"


def test_failed_episode_fetch_is_retried_in_same_run(monkeypatch):
    server = Server(item("series", kind="Series", external="95738"))
    source = {"type": "episode", "season": 1, "episode": 1, "show_ids": {"tmdb": "95738"}}
    ad = adapter(server)
    lookup.find(ad, "history", "Series", ["tmdb.95738"])
    server.fail = True
    assert common.resolve_item_id(ad, source) is None
    server.fail = False
    server.episodes["series"] = [item("correct", kind="Episode", SeriesId="series", ParentIndexNumber=1, IndexNumber=1)]
    assert common.resolve_item_id(ad, source) == "correct"


def test_pagination_reads_all_metadata_and_all_matched_series_episodes(monkeypatch):
    monkeypatch.setattr(lookup, "_PAGE_SIZE", 2)
    server = Server(item("m1"), item("m2"), item("series", kind="Series", external="9"))
    server.episodes["series"] = [item(str(n), kind="Episode", SeriesId="series",
                                      ParentIndexNumber=1, IndexNumber=n) for n in range(1, 502)]
    source = {"type": "episode", "season": 1, "episode": 501, "show_ids": {"tmdb": "9"}}
    assert common.resolve_item_id(adapter(server), source) == "501"
    assert [p["StartIndex"] for path, p in server.calls if path.startswith("/Shows/")] == [0, 500]


def test_next_run_refreshes_added_items_and_changed_ids(monkeypatch):
    server = Server(item("old", changed=100))
    source = {"type": "movie", "ids": {"tmdb": "1"}}
    assert common.resolve_item_id(adapter(server), source) == "old"
    server.rows["old"] = item("old", external="2", changed=1001)
    server.rows["new"] = item("new", external="3", changed=1001)
    monkeypatch.setattr(lookup.time, "time", lambda: 1010.0)
    log_run_id.set("id-run-2")
    server.calls.clear()
    ad = adapter(server)
    assert common.resolve_item_id(ad, source) is None
    assert common.resolve_item_id(ad, {"type": "movie", "ids": {"tmdb": "2"}}) == "old"
    assert common.resolve_item_id(ad, {"type": "movie", "ids": {"tmdb": "3"}}) == "new"
    assert len(server.calls) == 1
    assert "MinDateLastSaved" in server.calls[0][1]


@pytest.mark.parametrize("mutation", ["unchanged", "deleted", "changed_id", "moved_library"])
def test_persisted_hits_are_revalidated_once_per_run(mutation):
    server = Server(item("movie", changed=100))
    source = {"type": "movie", "ids": {"tmdb": "1"}}
    assert common.resolve_item_id(adapter(server, history_libraries=["A"]), source) == "movie"
    if mutation == "deleted":
        del server.rows["movie"]
    elif mutation == "changed_id":
        server.rows["movie"]["ProviderIds"] = {"Tmdb": "9"}
    elif mutation == "moved_library":
        server.rows["movie"]["LibraryId"] = "B"
    log_run_id.set("id-run-2")
    server.calls.clear()
    ad = adapter(server, history_libraries=["A"])
    expected = "movie" if mutation == "unchanged" else None
    assert common.resolve_item_id(ad, source) == expected
    assert common.resolve_item_id(ad, source) == expected
    assert sum(path == "/Items/movie" for path, _ in server.calls) == 1


@pytest.mark.parametrize("change", ["pair", "server", "user_id", "access_token", "libraries", "feature"])
def test_persistent_cache_does_not_cross_pair_or_profile_scope(change, tmp_path):
    server = Server(item("movie"))
    source = {"type": "movie", "ids": {"tmdb": "1"}}
    assert common.resolve_item_id(adapter(server), source) == "movie"
    config = {change: "other"} if change in {"server", "user_id", "access_token"} else {}
    if change == "libraries":
        config["history_libraries"] = ["B"]
    server.calls.clear()
    ad = adapter(server, pair="pair-B" if change == "pair" else "pair-A", **config)
    assert common.resolve_item_id(ad, source, feature="progress" if change == "feature" else "history") == (None if change == "libraries" else "movie")
    assert "MinDateLastSaved" not in server.calls[0][1]
    assert len(list(tmp_path.glob("jellyfin_identity.*.json"))) == 2
    assert all("private-token" not in p.read_text() for p in tmp_path.glob("*.json"))


def test_failed_refresh_preserves_disk_cache_and_is_not_retried_per_item(tmp_path):
    server = Server(item("movie", changed=100))
    source = {"type": "movie", "ids": {"tmdb": "1"}}
    assert common.resolve_item_id(adapter(server), source) == "movie"
    path = next(tmp_path.glob("*.json"))
    previous = path.read_bytes()
    log_run_id.set("id-run-2")
    server.fail = True
    server.calls.clear()
    ad = adapter(server)
    assert common.resolve_item_id(ad, source) is None
    assert common.resolve_item_id(ad, source) is None
    assert len(server.calls) == 1
    assert path.read_bytes() == previous


def test_repeated_page_does_not_publish_partial_cache(monkeypatch, tmp_path):
    monkeypatch.setattr(lookup, "_PAGE_SIZE", 2)
    server = Server(item("a"), item("b"), item("c"))
    server.repeat = True
    assert common.resolve_item_id(adapter(server), {"type": "movie", "ids": {"tmdb": "1"}}) is None
    assert not list(tmp_path.glob("*.json"))


def test_periodic_full_refresh_removes_deleted_entries(monkeypatch, tmp_path):
    server = Server(item("a"), item("b"))
    source = {"type": "movie", "ids": {"tmdb": "1"}}
    assert common.resolve_item_id(adapter(server), source)
    del server.rows["b"]
    log_run_id.set("id-run-2")
    monkeypatch.setattr(lookup.time, "time", lambda: 1000.0 + lookup._FULL_REFRESH_SECONDS)
    server.calls.clear()
    assert common.resolve_item_id(adapter(server), source) == "a"
    assert "MinDateLastSaved" not in server.calls[0][1]
    assert list(json.loads(next(tmp_path.glob("*.json")).read_text())["rows"]) == ["a"]


def test_unsupported_delta_filter_uses_metadata_only_full_refresh():
    server = Server(item("movie"))
    server.ignore_delta = True
    source = {"type": "movie", "ids": {"tmdb": "1"}}
    assert common.resolve_item_id(adapter(server), source) == "movie"
    log_run_id.set("id-run-2")
    server.calls.clear()
    assert common.resolve_item_id(adapter(server), source) == "movie"
    assert len(server.calls) == 1
    assert "MinDateLastSaved" not in server.calls[0][1]


@pytest.mark.parametrize("pair", [None, "", "unscoped", "default"])
def test_no_persistence_without_explicit_pair(pair, tmp_path):
    source = {"type": "movie", "ids": {"tmdb": "1"}}
    assert common.resolve_item_id(adapter(Server(item("movie")), pair=pair), source) == "movie"
    assert not list(tmp_path.glob("*.json"))


def test_history_prepares_cached_targets_in_batches_and_checks_current_ids():
    server = Server(*(item(str(n), external=str(n), changed=100) for n in range(105)))
    sources = [{"type": "movie", "ids": {"tmdb": str(n)}} for n in range(105)]
    ad = adapter(server)
    lookup.prepare(ad, "history", sources)
    log_run_id.set("id-run-2")
    server.calls.clear()
    del server.rows["2"]
    server.rows["3"]["ProviderIds"] = {"Tmdb": "900"}
    ad = adapter(server)
    lookup.prepare(ad, "history", sources)
    for n, source in enumerate(sources):
        assert common.resolve_item_id(ad, source) == (None if n in (2, 3) else str(n))
    assert len(server.calls) == 3  # One delta query and two batched native-ID reads.
    assert [len(params["Ids"].split(",")) for _, params in server.calls if params.get("Ids")] == [100, 5]


def test_new_episode_is_seen_next_run_without_rebuilding_series_catalogue():
    server = Server(item("series", kind="Series", external="9", changed=100))
    source = {"type": "episode", "season": 2, "episode": 1, "show_ids": {"tmdb": "9"}}
    assert common.resolve_item_id(adapter(server), source) is None
    server.episodes["series"] = [item("new", kind="Episode", SeriesId="series", ParentIndexNumber=2, IndexNumber=1)]
    log_run_id.set("id-run-2")
    server.calls.clear()
    assert common.resolve_item_id(adapter(server), source) == "new"
    assert "MinDateLastSaved" in server.calls[0][1]
    assert len(server.calls) == 3  # Delta, cached series validation, current episodes.


def test_strict_title_only_item_is_left_unresolved_without_search():
    server = Server(item("movie"))
    assert common.resolve_item_id(adapter(server), {"type": "movie", "title": "English title"}) is None
    assert not server.calls
