from __future__ import annotations

from types import SimpleNamespace

import pytest

from cw_platform.log_context import log_run_id
from providers.sync.jellyfin import _common as common, _id_lookup


class Response:
    def __init__(self, body, status=200):
        self.body, self.status_code = body, status

    def json(self):
        return self.body


class Server:
    def __init__(self):
        self.searches = {}
        self.episodes = {}
        self.items = {}
        self.ancestors = {}
        self.scoped_omissions = set()
        self.calls = []
        self.fail_parent = None

    def get(self, path, *, params):
        self.calls.append((path, dict(params)))
        assert "AnyProviderIdEquals" not in params
        if path == "/Items":
            assert params.get("SearchTerm"), "Unbounded library query in targeted mode"
            if params.get("ParentId") == self.fail_parent and self.fail_parent is not None:
                return Response({}, 503)
            rows = self.searches.get((params["includeItemTypes"], params["SearchTerm"]), [])
            if params.get("ParentId"):
                rows = [row for row in rows if row.get("LibraryId") == params["ParentId"]
                        and row["Id"] not in self.scoped_omissions]
            return Response({"Items": rows[:params["Limit"]], "TotalRecordCount": len(rows)})
        if path.startswith("/Shows/"):
            rows = self.episodes.get(path.split("/")[2], [])
            return Response({"Items": rows, "TotalRecordCount": len(rows)})
        if path.startswith("/Items/") and path.endswith("/Ancestors"):
            ancestors = self.ancestors.get(path.split("/")[2])
            return Response(ancestors or [], 200 if ancestors is not None else 503)
        if path.startswith("/Items/"):
            row = self.items.get(path.split("/")[2])
            return Response(row or {}, 200 if row else 404)
        raise AssertionError(path)


def row(iid, kind="Movie", name="Example", tmdb="1", **extra):
    return {"Id": iid, "Type": kind, "Name": name,
            "ProviderIds": {"Tmdb": tmdb} if tmdb else {}, **extra}


def adapter(http, *, pair="pair-a", **cfg):
    return SimpleNamespace(
        client=http,
        config={"_cw_pair_scope": pair},
        cfg=SimpleNamespace(**{
            "server": "http://jellyfin", "user_id": "user", "access_token": "credential",
            "targeted_lookup": True, "strict_id_matching": False, **cfg,
        }),
    )


@pytest.fixture(autouse=True)
def isolated_run(monkeypatch):
    # Exercise optional title fallback independently of the ID catalogue.
    monkeypatch.setattr(_id_lookup, "find", lambda *a, **kw: [])
    token = log_run_id.set("targeted-test")
    monkeypatch.setattr(common._TARGETED_CACHE, "entry", None, raising=False)
    monkeypatch.setattr(common, "build_provider_index", lambda *a, **kw: pytest.fail("Full index in targeted mode"))
    yield
    log_run_id.reset(token)


@pytest.mark.parametrize("feature", ["history", "progress"])
@pytest.mark.parametrize("item", [
    {"type": "movie", "title": "Missing", "ids": {"tmdb": "1"}},
    {"type": "movie", "ids": {}},
    {"type": "episode", "title": "Pilot", "season": 1, "episode": 1,
     "series_title": "Missing", "show_ids": {"tmdb": "1"}},
    {"type": "movie", "path": "/media/Example.mkv", "ids": {}},
])
def test_misses_and_paths_never_start_full_scan(feature, item):
    assert common.resolve_item_ids(adapter(Server()), item, feature=feature) == []


@pytest.mark.parametrize("strict", [False])
def test_id_match_accepts_different_title_and_collects_movie_copies(strict):
    server = Server()
    server.searches["Movie", "Example"] = [
        row("copy2", name="Example: Extended"), row("wrong", tmdb="2"),
        row("copy1", name="Example (Original Title)"), row("copy1"),
    ]
    item = {"type": "movie", "title": "Example", "ids": {"tmdb": "1"}}
    assert common.resolve_item_ids(adapter(server, strict_id_matching=strict), item) == ["copy1", "copy2"]
    assert len(server.calls) == 1


@pytest.mark.parametrize("strict,ids,expected", [
    (True, {}, []), (True, {"tmdb": "2"}, []),
    (False, {"tmdb": "2"}, []), (False, {}, ["movie"]),
])
def test_strict_id_setting_remains_separate(strict, ids, expected):
    server = Server()
    server.searches["Movie", "Example"] = [row("movie")]
    item = {"type": "movie", "title": "Example", "ids": ids}
    assert common.resolve_item_ids(adapter(server, strict_id_matching=strict), item) == expected


@pytest.mark.parametrize("feature", ["history", "progress"])
@pytest.mark.parametrize("targeted", [True, False])
@pytest.mark.parametrize("strict", [True, False])
@pytest.mark.parametrize("native_field", ["ids", "jellyfin_item_id"])
def test_native_id_cannot_use_title_to_replace_missing_public_ids(
    monkeypatch, feature, targeted, strict, native_field,
):
    server = Server()
    server.items["native"] = row("native", tmdb=None, ProductionYear=2020)
    monkeypatch.setattr(common, "build_provider_index", lambda *a, **kw: {})
    item = {"type": "movie", "title": "Example", "year": 2020, "ids": {"tmdb": "1"}}
    if native_field == "ids":
        item["ids"]["jellyfin"] = "native"
    else:
        item[native_field] = "native"
    ad = adapter(server, targeted_lookup=targeted, strict_id_matching=strict)
    assert common.resolve_item_id(ad, item, feature=feature) == (None if strict else "native")


@pytest.mark.parametrize("feature", ["history", "progress"])
@pytest.mark.parametrize("public_ids", [{}, {"tmdb": "1"}])
def test_strict_native_id_match_survives_changed_title_and_year(feature, public_ids):
    server = Server()
    server.items["native"] = row("native", name="Renamed", ProductionYear=2026)
    item = {"type": "movie", "title": "Old title", "year": 2000,
            "ids": {"jellyfin": "native", **public_ids}}
    assert common.resolve_item_id(adapter(server, strict_id_matching=True), item, feature=feature) == "native"


@pytest.mark.parametrize("returned_id", [None, "different"])
def test_strict_native_only_id_must_be_confirmed_by_server(returned_id):
    server = Server()
    if returned_id:
        server.items["native"] = row(returned_id)
    item = {"type": "movie", "ids": {"jellyfin": "native"}}
    assert common.resolve_item_id(adapter(server, strict_id_matching=True), item) is None


@pytest.mark.parametrize("feature", ["history", "progress"])
@pytest.mark.parametrize("ids", [{}, {"tmdb": "1"}])
def test_strict_full_lookup_skips_path_only_match(monkeypatch, feature, ids):
    server = Server()
    monkeypatch.setattr(common, "_path_match_item_id", lambda *a, **kw: pytest.fail("Path match in strict mode"))
    monkeypatch.setattr(common, "build_provider_index", lambda *a, **kw: {})
    item = {"type": "movie", "path": "/media/Example.mkv", "ids": ids}
    assert common.resolve_item_id(adapter(server, targeted_lookup=False, strict_id_matching=True), item, feature=feature) is None


def test_strict_full_lookup_does_not_confuse_movie_and_series_ids(monkeypatch):
    monkeypatch.setattr(common, "build_provider_index", lambda *a, **kw: {"tmdb.1": [row("series", "Series")]})
    item = {"type": "movie", "ids": {"tmdb": "1"}}
    assert common.resolve_item_id(adapter(Server(), targeted_lookup=False, strict_id_matching=True), item) is None


def test_ambiguous_title_only_movie_is_not_guessed():
    server = Server()
    server.searches["Movie", "Example"] = [row("a", tmdb="1"), row("b", tmdb="2")]
    assert common.resolve_item_ids(adapter(server, strict_id_matching=False), {"type": "movie", "title": "Example"}) == []


def test_duplicate_series_resolve_episodes_and_reuse_searches_between_batches():
    server = Server()
    server.searches["Series", "Example"] = [
        row("s1", "Series", "Example (US)"), row("s2", "Series", "Example (US)"),
    ]
    for sid in ("s1", "s2"):
        server.episodes[sid] = [
            row(f"{sid}e{n}", "Episode", tmdb=str(100 + n), SeriesId=sid,
                ParentIndexNumber=0, IndexNumber=n) for n in (1, 2)
        ]
    for n in (1, 2):
        item = {"type": "episode", "series_title": "Example", "season": 0, "episode": n,
                "show_ids": {"tmdb": "1"}, "ids": {"tmdb": str(100 + n)}}
        assert common.resolve_item_ids(adapter(server), item) == [f"s1e{n}"]
    assert len(server.calls) == 3  # One series search, one episode request per copy.


def test_episode_title_search_requires_episode_ids_without_series_metadata():
    server = Server()
    server.searches["Episode", "Pilot"] = [row("wrong", "Episode", "Pilot", "2"),
                                              row("right", "Episode", "Pilot: Part One", "1")]
    item = {"type": "episode", "title": "Pilot", "ids": {"tmdb": "1"}}
    assert common.resolve_item_ids(adapter(server), item) == ["right"]


@pytest.mark.parametrize("change", ["pair", "server", "user_id", "access_token", "libraries", "feature", "run"])
def test_targeted_cache_isolated_and_new_runs_see_new_items(change):
    server = Server()
    server.searches["Movie", "Example"] = [row("a", LibraryId="A")]
    item = {"type": "movie", "title": "Example", "ids": {"tmdb": "1"}}
    assert common.resolve_item_ids(adapter(server), item) == ["a"]
    server.searches["Movie", "Example"] = [row("b", LibraryId="B")]
    cfg = {change: "other"} if change in {"server", "user_id", "access_token"} else {}
    if change == "libraries":
        cfg["history_libraries"] = ["B"]
    if change == "run":
        log_run_id.set("next-run")
    other = adapter(server, pair="other" if change == "pair" else "pair-a", **cfg)
    assert common.resolve_item_ids(other, item, feature="progress" if change == "feature" else "history") == ["b"]
    assert len(server.calls) == 2


@pytest.mark.parametrize("pair", [None, "", "unscoped", "default"])
def test_incomplete_scope_does_not_share_between_adapters(pair, monkeypatch):
    monkeypatch.setattr(common, "_pair_scope", lambda: None)
    server = Server()
    item = {"type": "movie", "title": "Example", "ids": {"tmdb": "1"}}
    server.searches["Movie", "Example"] = [row("a")]
    assert common.resolve_item_ids(adapter(server, pair=pair), item) == ["a"]
    server.searches["Movie", "Example"] = [row("b")]
    assert common.resolve_item_ids(adapter(server, pair=pair), item) == ["b"]


def test_scoped_search_failure_does_not_cache_partial_results():
    server = Server()
    server.searches["Movie", "Example"] = [row("a", LibraryId="A"), row("b", LibraryId="B")]
    server.fail_parent = "B"
    ad = adapter(server, history_libraries=["A", "B"])
    item = {"type": "movie", "title": "Example", "ids": {"tmdb": "1"}}
    assert common.resolve_item_ids(ad, item) == []
    server.fail_parent = None
    assert common.resolve_item_ids(ad, item) == ["a", "b"]


def test_stale_native_id_is_revalidated_before_targeted_matching():
    server = Server()
    server.items["stale"] = row("stale", tmdb="2")
    server.searches["Movie", "Example"] = [row("correct")]
    item = {"type": "movie", "title": "Example", "ids": {"tmdb": "1"}, "jellyfin_item_id": "stale"}
    assert common.resolve_item_ids(adapter(server), item) == ["correct"]
    assert server.calls[0][0] == "/Items/stale"


@pytest.mark.parametrize("ancestor,expected", [("A", ["movie"]), ("B", []), (None, [])])
def test_scoped_search_omission_retries_title_with_verified_ancestry(ancestor, expected):
    server = Server()
    server.searches["Movie", "Example"] = [row("movie", LibraryId="A")]
    server.scoped_omissions.add("movie")
    if ancestor is not None:
        server.ancestors["movie"] = [{"Id": ancestor, "Type": "CollectionFolder"}]
    ad = adapter(server, history_libraries=["A"])
    item = {"type": "movie", "title": "Example", "ids": {"tmdb": "1"}}
    assert common.resolve_item_ids(ad, item) == expected
    assert any(path.endswith("/Ancestors") for path, _ in server.calls)


def test_failed_ancestor_check_is_retried_and_does_not_poison_cache():
    server = Server()
    server.searches["Movie", "Example"] = [row("movie", LibraryId="A")]
    server.scoped_omissions.add("movie")
    ad = adapter(server, history_libraries=["A"])
    item = {"type": "movie", "title": "Example", "ids": {"tmdb": "1"}}
    assert common.resolve_item_ids(ad, item) == []
    server.ancestors["movie"] = [{"Id": "A"}]
    assert common.resolve_item_ids(ad, item) == ["movie"]


@pytest.mark.parametrize("title", ["Example (NL)", "Example (2024)", "Example (2024) (nl)"])
@pytest.mark.parametrize("feature", ["history", "progress"])
def test_title_suffix_retry_requires_ids_and_preserves_source(title, feature):
    server = Server()
    server.searches["Movie", "Example"] = [row("wrong", tmdb="2"), row("right")]
    item = {"type": "movie", "title": title, "ids": {"tmdb": "1"}}
    assert common.resolve_item_ids(adapter(server), item, feature=feature) == ["right"]
    assert item["title"] == title
    assert [params["SearchTerm"] for _, params in server.calls] == [title, "Example"]


def test_year_suffix_on_series_uses_ids_and_episode_coordinates():
    server = Server()
    server.searches["Series", "Example"] = [row("series", "Series")]
    server.episodes["series"] = [row("episode", "Episode", tmdb="2", SeriesId="series",
                                    ParentIndexNumber=1, IndexNumber=3)]
    item = {"type": "episode", "series_title": "Example (2024)", "season": 1, "episode": 3,
            "show_ids": {"tmdb": "1"}, "ids": {"tmdb": "2"}}
    assert common.resolve_item_ids(adapter(server), item) == ["episode"]
    assert item["series_title"] == "Example (2024)"


@pytest.mark.parametrize("title,ids", [("Example (NL)", {}), ("Example (Extended Cut)", {"tmdb": "1"}),
                                       ("Example (2024) Part Two", {"tmdb": "1"})])
def test_no_title_cleanup_without_ids_or_for_unrecognized_suffix(title, ids):
    server = Server()
    server.searches["Movie", "Example"] = [row("movie")]
    item = {"type": "movie", "title": title, "ids": ids}
    assert common.resolve_item_ids(adapter(server, strict_id_matching=False), item) == []
    assert [params["SearchTerm"] for _, params in server.calls] == [title]


def test_suffix_retry_rejects_different_ids_even_when_title_matches():
    server = Server()
    server.searches["Movie", "Example"] = [row("wrong", tmdb="2")]
    item = {"type": "movie", "title": "Example (NL)", "ids": {"tmdb": "1"}}
    assert common.resolve_item_ids(adapter(server, strict_id_matching=False), item) == []


def test_original_title_match_does_not_make_variant_request():
    server = Server()
    server.searches["Movie", "Example (NL)"] = [row("movie")]
    assert common.resolve_item_ids(adapter(server), {"type": "movie", "title": "Example (NL)", "ids": {"tmdb": "1"}}) == ["movie"]
    assert len(server.calls) == 1


def test_debug_diagnostics_include_status_candidates_and_rejection_counts(monkeypatch):
    events = []
    monkeypatch.setattr(common, "_dbg", lambda event, **fields: events.append((event, fields)))
    server = Server()
    server.searches["Movie", "Example"] = [row("wrong", tmdb="2")]
    item = {"type": "movie", "title": "Example", "ids": {"tmdb": "1"}}
    assert common.resolve_item_ids(adapter(server), item) == []
    assert any(event == "targeted_search_response" and fields["status"] == 200
               and fields["candidates"] == 1 and fields["term"] == "Example" for event, fields in events)
    assert any(event == "targeted_search_match" and fields["rejected"] == {"id_mismatch": 1}
               for event, fields in events)


def test_debug_failure_does_not_log_exception_secrets(monkeypatch):
    events = []
    monkeypatch.setattr(common, "_dbg", lambda event, **fields: events.append((event, fields)))
    server = Server()
    def fail(*args, **kwargs):
        raise RuntimeError("secret-token-and-password-in-request-url")
    monkeypatch.setattr(server, "get", fail)
    assert common.resolve_item_ids(adapter(server), {"type": "movie", "title": "Example", "ids": {"tmdb": "1"}}) == []
    assert any(event == "targeted_search_failed" and fields["error_type"] == "RuntimeError"
               for event, fields in events)
    assert "secret-token-and-password" not in repr(events)
