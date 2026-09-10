from types import SimpleNamespace

import pytest

from cw_platform.log_context import log_run_id
from providers.sync.emby import _common as common


def movie(iid="1", tmdb="10", **extra):
    return {"Id": iid, "Type": "Movie", "Name": "Localized title", "ProviderIds": {"Tmdb": tmdb}, **extra}


class Server:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.calls = []
        self.fail_start = None
        self.ignore_filter = False

    def get(self, path, *, params=None):
        q = dict(params or {})
        self.calls.append((path, q))
        if path.startswith("/Shows/"):
            rows = [r for r in self.rows if r.get("SeriesId") == path.split("/")[2]]
        elif path.startswith("/Users/user/Items/"):
            row = next((r for r in self.rows if r["Id"] == path.rsplit("/", 1)[-1]), None)
            return SimpleNamespace(status_code=200 if row else 404, json=lambda: row)
        else:
            assert q.get("AnyProviderIdEquals"), "Unexpected full index or title search"
            pairs = set(q["AnyProviderIdEquals"].split(","))
            rows = [r for r in self.rows if self.ignore_filter or (
                r["Type"] in q["IncludeItemTypes"].split(",")
                and pairs.intersection(f"{k.lower()}.{v}" for k, v in r.get("ProviderIds", {}).items())
            )]
            if q.get("ParentId"):
                rows = [r for r in rows if r.get("LibraryId") == q["ParentId"]]
        start = q.get("StartIndex", 0)
        page = rows[start:start + q.get("Limit", 500)]
        return SimpleNamespace(status_code=503 if start == self.fail_start else 200,
                               json=lambda: {"Items": page, "TotalRecordCount": len(rows)})


def adapter(server=None, **cfg):
    return SimpleNamespace(client=server or Server(), config={"_cw_pair_scope": "pair1"},
                           cfg=SimpleNamespace(server="http://emby", user_id="user", access_token="token",
                                               strict_id_matching=True, **cfg))


@pytest.fixture(autouse=True)
def context(monkeypatch):
    for name in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        monkeypatch.delenv(name, raising=False)
    token = log_run_id.set("first")
    monkeypatch.setattr(common._RUN_LOOKUP_CACHE, "entry", None, raising=False)
    monkeypatch.setattr(common, "cw_log", lambda *a, **k: None)
    yield
    log_run_id.reset(token)


def test_localized_title_and_year_do_not_override_ids_and_all_copies_are_found():
    server = Server([movie(str(i), ProductionYear=2020 if i else 2024) for i in range(60)])
    item = {"type": "movie", "title": "Different title", "year": 2024, "ids": {"tmdb": "10"}}
    for _ in range(3):
        assert len(common.resolve_item_ids(adapter(server), item)) == 60
    assert [q["StartIndex"] for _, q in server.calls] == [0, 50]


def test_missing_ids_stay_narrow_and_retry_next_run():
    server = Server()
    ad = adapter(server)
    item = {"type": "movie", "title": "Name", "ids": {"tmdb": "10"}}
    assert common.resolve_item_ids(ad, item) == []
    server.rows = [movie()]
    assert common.resolve_item_ids(adapter(server), item) == []
    assert len(server.calls) == 1
    log_run_id.set("second")
    assert common.resolve_item_ids(ad, item) == ["1"]
    server.rows = [movie("2")]
    log_run_id.set("third")
    assert common.resolve_item_ids(ad, item) == ["2"]


@pytest.mark.parametrize("change", ["server", "user_id", "access_token", "pair", "feature", "libraries", "strict"])
def test_lookup_cache_is_isolated(change):
    server = Server([movie(LibraryId="A")])
    ad = adapter(server)
    item = {"type": "movie", "ids": {"tmdb": "10"}}
    assert common.resolve_item_ids(ad, item) == ["1"]
    feature = "history"
    if change == "pair":
        ad.config["_cw_pair_scope"] = "pair2"
    elif change == "feature":
        feature = "progress"
    elif change == "libraries":
        ad.cfg.history_libraries = ["A"]
    elif change == "strict":
        ad.cfg.strict_id_matching = False
    else:
        setattr(ad.cfg, change, "changed")
    assert common.resolve_item_ids(ad, item, feature=feature) == ["1"]
    assert len(server.calls) == 2


def test_feature_scope_uses_progress_libraries():
    ad = adapter(Server([movie("1", LibraryId="A"), movie("2", LibraryId="B")]),
                 history_libraries=["A"], progress_libraries=["B"])
    item = {"type": "movie", "ids": {"tmdb": "10"}}
    assert common.resolve_item_id(ad, item, feature="progress") == "2"
    assert common.resolve_item_ids(ad, item, feature="history") == ["1"]


def test_strict_native_id_cannot_use_title_to_replace_missing_external_ids():
    ad = adapter(Server([movie("1", ProviderIds={})]))
    assert common.resolve_item_ids(ad, {"type": "movie", "title": "Localized title",
                                        "ids": {"emby": "1", "tmdb": "10"}}) == []
    assert common.resolve_item_ids(ad, {"type": "movie", "title": "Other title", "ids": {"emby": "1"}}) == ["1"]


@pytest.mark.parametrize("failure", ["partial", "ignored_filter"])
def test_failed_or_unfiltered_query_does_not_cache_results(failure):
    server = Server([movie(str(i)) for i in range(60)])
    ad = adapter(server)
    if failure == "partial":
        server.fail_start = 50
    else:
        server.rows.append(movie("999", "999"))
        server.ignore_filter = True
    item = {"type": "movie", "ids": {"tmdb": "10"}}
    assert common.resolve_item_ids(ad, item) == []
    server.fail_start = None
    server.ignore_filter = False
    assert len(common.resolve_item_ids(ad, item)) == 60


def test_series_id_handles_specials_multiple_series_copies_and_refreshes():
    series = [{"Id": sid, "Type": "Series", "ProviderIds": {"Tvdb": "20"}} for sid in ("10", "11")]
    ep = {"Id": "100", "Type": "Episode", "SeriesId": "11", "ParentIndexNumber": 0, "IndexNumber": 1}
    server = Server(series + [ep])
    ad = adapter(server)
    item = {"type": "episode", "show_ids": {"tvdb": "20"}, "season": 0, "episode": 1}
    assert common.resolve_item_ids(ad, item) == ["100"]
    assert common.resolve_item_ids(adapter(server), item) == ["100"]
    assert len(server.calls) == 3
    server.rows.append({**ep, "Id": "101", "IndexNumber": 2})
    log_run_id.set("second")
    assert common.resolve_item_ids(ad, {**item, "episode": 2}) == ["101"]


def test_exact_episode_id_leads_over_numbering():
    ad = adapter(Server([{"Id": "100", "Type": "Episode", "ProviderIds": {"Tvdb": "123"},
                          "ParentIndexNumber": 2, "IndexNumber": 4}]))
    assert common.resolve_item_ids(ad, {"type": "episode", "ids": {"tvdb": "123"},
                                        "season": 1, "episode": 8}) == ["100"]


def test_missing_pair_scope_never_shares_between_adapters():
    server = Server([movie()])
    for _ in range(2):
        ad = adapter(server)
        ad.config = {}
        assert common.resolve_item_ids(ad, {"type": "movie", "ids": {"tmdb": "10"}}) == ["1"]
    assert len(server.calls) == 2


def test_standalone_lookup_refreshes_after_ttl(monkeypatch):
    log_run_id.set("")
    now = [1000.0]
    monkeypatch.setattr(common.time, "monotonic", lambda: now[0])
    server = Server([movie()])
    ad = adapter(server)
    item = {"type": "movie", "ids": {"tmdb": "10"}}
    assert common.resolve_item_ids(ad, item) == ["1"]
    server.rows = [movie("2")]
    now[0] += 301
    assert common.resolve_item_ids(ad, item) == ["2"]


def test_series_pages_without_totals_are_read_and_failed_pages_retry():
    class Episodes:
        fail = True

        def get(self, path, *, params):
            start = params["StartIndex"]
            rows = [{"Id": str(i)} for i in range(start, min(501, start + params["Limit"]))]
            return SimpleNamespace(status_code=503 if self.fail and start else 200, json=lambda: {"Items": rows})

    server = Episodes()
    ad = adapter(server)
    assert common._series_episodes_cached(ad, server, "user", "10") == []
    server.fail = False
    assert len(common._series_episodes_cached(ad, server, "user", "10")) == 501


def test_title_fallback_remains_available_when_strict_is_disabled():
    class Titles(Server):
        def get(self, path, *, params=None):
            if (params or {}).get("SearchTerm"):
                return SimpleNamespace(status_code=200, json=lambda: {"Items": [movie()]})
            return super().get(path, params=params)

    ad = adapter(Titles())
    ad.cfg.strict_id_matching = False
    assert common.resolve_item_ids(ad, {"type": "movie", "title": "Localized title"}) == ["1"]


@pytest.mark.parametrize("payload", [{}, {"Items": None}, {"Items": [None]}])
def test_malformed_id_query_response_is_not_cached(payload):
    class Malformed(Server):
        broken = True

        def get(self, path, *, params=None):
            if self.broken:
                return SimpleNamespace(status_code=200, json=lambda: payload)
            return super().get(path, params=params)

    server = Malformed([movie()])
    ad = adapter(server)
    source = {"type": "movie", "ids": {"tmdb": "10"}}
    assert common.resolve_item_ids(ad, source) == []
    server.broken = False
    assert common.resolve_item_ids(ad, source) == ["1"]


@pytest.mark.parametrize("extra", [{"Type": "Movie"}, {"SeriesId": "999"}])
def test_series_episode_response_cannot_match_wrong_type_or_parent(extra):
    series = {"Id": "10", "Type": "Series", "ProviderIds": {"Tmdb": "20"}}
    episode = {"Id": "100", "Type": "Episode", "SeriesId": "10", "ParentIndexNumber": 1, "IndexNumber": 1, **extra}

    class WrongSeries(Server):
        def get(self, path, *, params=None):
            if path.startswith("/Shows/"):
                return SimpleNamespace(status_code=200, json=lambda: {"Items": [episode], "TotalRecordCount": 1})
            return super().get(path, params=params)

    ad = adapter(WrongSeries([series]))
    assert common.resolve_item_ids(ad, {"type": "episode", "show_ids": {"tmdb": "20"}, "season": 1, "episode": 1}) == []


def test_zero_padded_provider_ids_are_valid_query_matches():
    row = movie(ProviderIds={"Tvdb": "0081871"})
    http = SimpleNamespace(get=lambda *a, **k: SimpleNamespace(status_code=200, json=lambda: {"Items": [row]}))
    assert common._direct_query_by_pairs(http, "user", ["tvdb.81871"], "Movie", {}) == [row]


def test_duplicate_series_and_episode_order_does_not_change_match():
    series = [{"Id": sid, "Type": "Series", "ProviderIds": {"Tmdb": "20"}} for sid in ("10", "20")]
    episodes = [{"Id": sid + suffix, "Type": "Episode", "SeriesId": sid, "ParentIndexNumber": 0, "IndexNumber": 1}
                for sid in ("10", "20") for suffix in ("1", "2")]
    server = Server(series + episodes)
    source = {"type": "episode", "show_ids": {"tmdb": "20"}, "season": 0, "episode": 1}
    assert common.resolve_item_id(adapter(server), source) == "101"
    server.rows.reverse()
    log_run_id.set("second")
    assert common.resolve_item_id(adapter(server), source) == "101"
