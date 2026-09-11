from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pytest

from providers.sync.plex import _common as common, _history as history, _progress as progress, _ratings as ratings, _watchlist as watchlist


def page(key="1", total=2):
    return {"MediaContainer": {"Metadata": [{"ratingKey": key, "type": "movie", "userRating": 7}], "totalSize": total}}


@pytest.fixture
def adapter(monkeypatch):
    for module in (ratings, watchlist):
        monkeypatch.setattr(module, "home_scope_enter", lambda *a: (False, False, None, None))
        monkeypatch.setattr(module, "home_scope_exit", lambda *a: None)
        monkeypatch.setattr(module, "normalize_discover_row", lambda row, **kw: {
            "type": "movie", "ids": {"tmdb": row["ratingKey"], "plex": row["ratingKey"]},
        })
        for name in ("_info", "_warn", "_dbg"):
            monkeypatch.setattr(module, name, lambda *a, **kw: None)
    monkeypatch.setattr(watchlist, "active_cloud_token", lambda *a: "test-token")
    srv = SimpleNamespace(baseurl="http://plex.test", _token="test-token")
    return SimpleNamespace(client=SimpleNamespace(server=srv, session=object()), cfg=SimpleNamespace(),
                           config={"plex": {"rating_workers": 1, "ratings": {"libraries": [1]}}},
                           libraries=lambda **kw: [SimpleNamespace(key="1", type="movie")])


@pytest.mark.parametrize("feature", ["ratings", "watchlist"])
@pytest.mark.parametrize("failure", ["missing", "malformed", "truncated", "repeated"])
def test_incomplete_snapshots_raise_instead_of_becoming_empty_or_partial(adapter, monkeypatch, feature, failure):
    second = {"missing": {}, "malformed": {"MediaContainer": {"Metadata": "invalid"}},
              "truncated": {"MediaContainer": {"Metadata": [], "totalSize": 2}}, "repeated": page()}[failure]
    pages = iter([page(), second])
    if feature == "watchlist":
        monkeypatch.setattr(watchlist, "_get_container", lambda *a, **kw: next(pages))
        module = watchlist
    else:
        adapter.client.server._session = SimpleNamespace(get=lambda *a, **kw: SimpleNamespace(
            ok=True, text="{}", headers={"Content-Type": "application/json"}, json=lambda: next(pages)))
        module = ratings
    with pytest.raises(RuntimeError):
        module.build_index(adapter)


@pytest.mark.parametrize("feature", ["ratings", "watchlist"])
def test_short_pages_follow_reported_total(adapter, monkeypatch, feature):
    pages = iter([page("1"), page("2")])
    paths = []
    if feature == "watchlist":
        monkeypatch.setattr(watchlist, "_get_container", lambda *a, **kw: next(pages))
        module = watchlist
    else:
        def get(path, **kw):
            paths.append(path)
            return SimpleNamespace(ok=True, headers={"Content-Type": "application/json"}, json=lambda: next(pages))
        adapter.client.server._session = SimpleNamespace(get=get)
        module = ratings
    result = module.build_index(adapter)
    assert len(result) == 2
    assert all("/library/sections/1/all" in path for path in paths)


@pytest.mark.parametrize("failure", ["missing", "truncated", "repeated"])
def test_progress_does_not_publish_incomplete_pages(failure):
    first = ET.fromstring('<MediaContainer totalSize="2"><Video ratingKey="1" viewOffset="1000" /></MediaContainer>')
    second = {"missing": None, "truncated": ET.fromstring('<MediaContainer totalSize="2" />'), "repeated": first}[failure]
    pages = iter([first, second])
    srv = SimpleNamespace(query=lambda path, **kw: ET.fromstring('<MediaContainer><Directory key="1" type="movie" /></MediaContainer>')
                          if path == "/library/sections" else next(pages))
    with pytest.raises(RuntimeError):
        progress._fetch_resume_items(srv, allowed_library_ids={"1"})


def test_unknown_library_identity_is_not_accepted_by_whitelist():
    assert not common.section_allowed(SimpleNamespace(), {"1"})
    assert common.section_allowed(SimpleNamespace(librarySectionID="1"), {"1"})


def test_external_id_takes_precedence_over_reused_native_rating_key():
    cat = history.HistoryCatalog()
    cat.add({"rk": "42", "type": "movie", "ids": {"plex": "42", "tmdb": "100"}})
    cat.add({"rk": "43", "type": "movie", "ids": {"plex": "43", "tmdb": "200"}})
    assert cat.resolve({"type": "movie", "ids": {"plex": "42", "tmdb": "200"}}, strict=True)[0] == "43"
    assert cat.resolve({"type": "movie", "ids": {"plex": "42", "tmdb": "300"}}, strict=True)[0] is None


def test_native_item_with_conflicting_external_id_is_rejected():
    obj = SimpleNamespace(type="movie", guids=[SimpleNamespace(id="tmdb://100")])
    assert not common.native_item_matches(obj, {"type": "movie", "ids": {"tmdb": "200", "plex": "42"}})
    assert common.native_item_matches(obj, {"type": "movie", "ids": {"tmdb": "100", "plex": "42"}})


def test_unsupported_shared_ratings_cannot_publish_empty_snapshot(adapter):
    adapter.client._user_scope_kind = "shared"
    with pytest.raises(RuntimeError, match="shared_user_ratings_unsupported"):
        ratings.build_index(adapter)


def test_server_token_precedes_adapter_cloud_credential():
    server = SimpleNamespace(_token="server-token", _session=SimpleNamespace(headers={"X-Plex-Token": "active-user-token"}))
    ad = SimpleNamespace(cfg=SimpleNamespace(token="account-token"), client=SimpleNamespace(server=server))
    assert common.active_pms_token(ad) == "active-user-token"
    assert common.active_cloud_token(ad) == "account-token"
    ad.client._cloud_token_suppressed = True
    assert common.active_cloud_token(ad) is None
