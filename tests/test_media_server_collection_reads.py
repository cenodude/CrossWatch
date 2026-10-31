# CrossWatch test scripts
from __future__ import annotations

from types import SimpleNamespace

import pytest

from cw_platform.orchestrator import _snapshots
from providers.sync.emby import _collection as emby_collection
from providers.sync.jellyfin import _collection as jellyfin_collection


class Response:
    def __init__(self, payload, status_code=200, json_error=None):
        self.payload = payload
        self.status_code = status_code
        self.json_error = json_error

    def json(self):
        if self.json_error:
            raise self.json_error
        return self.payload


def _movies(first, count):
    return [{"Id": str(i), "Type": "Movie", "Name": f"Movie {i}", "ProviderIds": {"Tmdb": str(i)}} for i in range(first, first + count)]


class Library:
    def __init__(self, count=643, *, libraries=None, fail_at=None, error=None, json_error_at=None, body_at=None,
                 total=None, total_at=None, hidden=(), ignore_start=False):
        self.libraries = libraries or {None: _movies(1, count)}
        self.fail_at = fail_at
        self.error = error
        self.json_error_at = json_error_at
        self.body_at = body_at or {}
        self.total = total
        self.total_at = total_at or {}
        self.hidden = set(hidden)
        self.ignore_start = ignore_start
        self.calls = []

    def get(self, path, *, params):
        parent = params.get("ParentId")
        start = int(params.get("StartIndex", 0))
        self.calls.append((parent, start))
        if (parent, start) == self.fail_at or start == self.fail_at:
            if self.error:
                raise self.error
            return Response({}, status_code=502)
        if start == self.json_error_at:
            return Response(None, json_error=ValueError("Expecting value"))
        if start in self.body_at:
            return Response(self.body_at[start])
        rows = self.libraries[parent]
        window = rows[0 if self.ignore_start else start:][:int(params.get("Limit", 500))]
        page = [row for row in window if row["Id"] not in self.hidden]
        total = self.total_at.get(start, self.total if self.total is not None else len(rows))
        return Response({"Items": page, "TotalRecordCount": total})


MODULES = [
    (jellyfin_collection, "jf_selected_library_ids", "jellyfin"),
    (emby_collection, "emby_selected_library_ids", "emby"),
]


def _adapter(http):
    return SimpleNamespace(client=http, cfg=SimpleNamespace(user_id="u1", collection_media_info=False))


@pytest.fixture(params=MODULES, ids=[m[2] for m in MODULES])
def collection(request, monkeypatch):
    module, scope_fn, name = request.param
    scope: set[str] = set()
    monkeypatch.setattr(module, scope_fn, lambda cfg, feature: set(scope))
    return SimpleNamespace(module=module, name=name, scope=scope)


def _ids(rows):
    return [raw["Id"] for raw, _parent in rows]


def test_full_read_returns_every_row(collection):
    rows, _allowed = collection.module._fetch_rows(_adapter(Library(643)))
    assert len(rows) == 643


def test_jellyfin_read_excludes_virtual_items(monkeypatch):
    monkeypatch.setattr(jellyfin_collection, "jf_selected_library_ids", lambda cfg, feature: set())
    http = Library(3)
    seen = []
    get = http.get
    http.get = lambda path, *, params: (seen.append(dict(params)), get(path, params=params))[1]
    jellyfin_collection._fetch_rows(_adapter(http))
    assert seen and all(p.get("IsMissing") is False for p in seen)


def test_valid_empty_library_is_not_a_failure(collection):
    rows, _allowed = collection.module._fetch_rows(_adapter(Library(0)))
    assert rows == []


@pytest.mark.parametrize("kwargs,reason", [
    ({"fail_at": 500}, "http_502"),
    ({"fail_at": 0}, "http_502"),
    ({"fail_at": 500, "error": TimeoutError("read timed out")}, "read_failed:read timed out"),
    ({"fail_at": 500, "error": RuntimeError("client gave up")}, "read_failed:client gave up"),
    ({"json_error_at": 500}, "read_failed:Expecting value"),
    ({"total": 700}, "incomplete_read:643/700"),
    ({"ignore_start": True}, "repeated_page"),
    ({"total_at": {500: 642}}, r"total_changed:643->642"),
    ({"total_at": {500: 644}}, r"total_changed:643->644"),
    ({"total": 600}, "total_exceeded:643/600"),
])
def test_failed_or_inconsistent_read_raises_instead_of_returning_partial_rows(collection, kwargs, reason):
    with pytest.raises(RuntimeError, match=f"{collection.name}_collection_{reason}"):
        collection.module._fetch_rows(_adapter(Library(643, **kwargs)))


@pytest.mark.parametrize("body", [
    {},
    {"Items": None, "TotalRecordCount": 643},
    {"Items": [], "TotalRecordCount": None},
    {"Items": []},
    {"Items": [None] * 143, "TotalRecordCount": 643},
    {"Items": [{"Name": "no id"}], "TotalRecordCount": 643},
    {"Items": [], "TotalRecordCount": "643"},
    [],
    None,
])
def test_malformed_later_page_is_a_failure(collection, body):
    with pytest.raises(RuntimeError, match=f"{collection.name}_collection_invalid_page"):
        collection.module._fetch_rows(_adapter(Library(643, body_at={500: body})))


@pytest.mark.parametrize("hidden", [{"7"}, {"600"}, {"7", "498", "600"}])
def test_overlapping_pages_fail_instead_of_deduplicating(collection, hidden):
    with pytest.raises(RuntimeError, match=f"{collection.name}_collection_overlapping_page"):
        collection.module._fetch_rows(_adapter(Library(643, hidden=hidden)))


def test_overlap_with_unchanged_total_is_detected(collection):
    http = Library(4)
    pages = {0: _movies(1, 2), 2: _movies(2, 2)}
    http.get = lambda path, *, params: Response({"Items": pages.get(int(params.get("StartIndex", 0)), []), "TotalRecordCount": 4})
    with pytest.raises(RuntimeError, match=f"{collection.name}_collection_overlapping_page"):
        collection.module._fetch_rows(_adapter(http))


def test_hidden_item_at_the_end_cannot_be_told_apart_from_a_truncated_read(collection):
    with pytest.raises(RuntimeError, match=f"{collection.name}_collection_incomplete_read:642/643"):
        collection.module._fetch_rows(_adapter(Library(643, hidden={"643"})))


def test_multiple_libraries_count_per_library_and_dedupe_shared_items(collection):
    collection.scope.update({"lib-a", "lib-b"})
    shared = _movies(1, 1)
    http = Library(libraries={"lib-a": shared + _movies(2, 600), "lib-b": shared + _movies(700, 2)})
    rows, _allowed = collection.module._fetch_rows(_adapter(http))
    assert len(rows) == 603
    assert [parent for _raw, parent in rows].count("lib-b") == 2


def test_failure_in_second_library_fails_the_whole_read(collection):
    collection.scope.update({"lib-a", "lib-b"})
    http = Library(libraries={"lib-a": _movies(1, 10), "lib-b": _movies(100, 600)}, fail_at=("lib-b", 500))
    with pytest.raises(RuntimeError, match=f"{collection.name}_collection_http_502"):
        collection.module._fetch_rows(_adapter(http))


def test_failed_read_does_not_reach_snapshot_callback_or_cache(collection, monkeypatch):
    ad = _adapter(Library(643, fail_at=500))
    ops = SimpleNamespace(features=lambda: {"collection": True}, build_index=lambda *a, **k: collection.module.build_index(ad))
    monkeypatch.setattr(_snapshots, "provider_configured", lambda *a: True)
    monkeypatch.setattr(_snapshots, "allowed_providers_for_feature", lambda *a: set())
    callbacks, cache = [], {}
    with pytest.raises(RuntimeError, match=f"{collection.name}_collection_http_502"):
        _snapshots.build_snapshots_for_feature(feature="collection", config={}, providers={collection.name.upper(): ops},
            snap_cache=cache, snap_ttl_sec=300, dbg=lambda *a, **k: None, emit_info=lambda *a: None,
            on_snapshot=lambda *args: callbacks.append(args))
    assert callbacks == []
    assert cache == {}


@pytest.mark.parametrize("selected", [[], ["movie-selected"], ["movie-selected", "tv-selected"]])
def test_collection_query_library_survives_planner_filter(collection, monkeypatch, selected):
    from cw_platform.orchestrator._pairs_oneway import _filter_index_by_libraries

    collection.scope.update(selected)
    roots = {"movie-first": {"collection_type": "movies", "type": "movie"}, "tv-first": {"collection_type": "tvshows", "type": "show"},
             "movie-selected": {"collection_type": "movies", "type": "movie"}, "tv-selected": {"collection_type": "tvshows", "type": "show"}}
    prefix = "jf" if collection.name == "jellyfin" else "emby"
    monkeypatch.setattr(collection.module, prefix + "_get_library_roots", lambda a: roots)
    if selected:
        def unavailable(*a, **k):
            raise RuntimeError("library resolver unavailable")
        monkeypatch.setattr(collection.module, prefix + "_resolve_library_id", unavailable)
    libraries = {parent: _movies(i + 1, 1) for i, parent in enumerate(selected or [None])}
    if "tv-selected" in libraries:
        libraries["tv-selected"][0]["Type"] = "Series"
    out = collection.module.build_index(_adapter(Library(libraries=libraries)))
    assert len(out) == len(selected or [None])
    assert len(_filter_index_by_libraries(out, selected)) == len(out)
    assert {v["library_id"] for v in out.values()} == set(selected or ["movie-first"])


@pytest.mark.parametrize("feature", ["history", "ratings"])
@pytest.mark.parametrize("selected", [[], ["movie-selected"], ["movie-selected", "tv-selected"]])
def test_jellyfin_user_data_query_library_survives_planner_filter(monkeypatch, feature, selected):
    import importlib
    from cw_platform.orchestrator._pairs_oneway import _filter_index_by_libraries
    from providers.sync.jellyfin import _common

    module = importlib.import_module(f"providers.sync.jellyfin._{feature}")
    roots = {"movie-first": {"collection_type": "movies", "type": "movie"}, "tv-first": {"collection_type": "tvshows", "type": "show"},
             "movie-selected": {"collection_type": "movies", "type": "movie"}, "tv-selected": {"collection_type": "tvshows", "type": "show"}}
    monkeypatch.setattr(module, "jf_get_library_roots", lambda a: roots)
    monkeypatch.setattr(module, "jf_scope_" + feature, lambda cfg: {"ParentIds": selected})
    monkeypatch.setattr(_common, "_jf_lib_id_via_ancestors", lambda *a: None)
    if selected:
        def unavailable(*a, **k):
            raise RuntimeError("library resolver unavailable")
        monkeypatch.setattr(module, "jf_resolve_library_id", unavailable)
    monkeypatch.setattr(module, "_shadow_load", lambda *a, **k: {})
    if feature == "history":
        monkeypatch.setattr(module, "_bb_load", lambda: {})
    else:
        monkeypatch.setattr(module, "_meta_load", lambda: {})
        monkeypatch.setattr(module, "_meta_save", lambda *a: None)
        monkeypatch.setattr(module, "_flush", lambda: None)
    def get(path, *, params):
        if params.get("StartIndex", params.get("startIndex", 0)):
            return Response({"Items": [], "TotalRecordCount": 1})
        parent = params.get("ParentId")
        row = _movies(2 if parent == "tv-selected" else 1, 1)[0]
        row["Type"] = "Series" if parent == "tv-selected" and feature == "ratings" else "Movie"
        row["UserRating"] = 8
        row["UserData"] = {"Played": True, "LastPlayedDate": "2026-09-01T12:00:00Z", "Rating": 8}
        return Response({"Items": [row], "TotalRecordCount": 1})
    out = module.build_index(_adapter(SimpleNamespace(get=get)))
    assert len(out) == len(selected or [None])
    assert len(_filter_index_by_libraries(out, selected)) == len(out)
    assert {v["library_id"] for v in out.values()} == set(selected or ["movie-first"])
