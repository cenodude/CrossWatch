from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from xml.etree import ElementTree as ET

import pytest

from cw_platform.log_context import log_run_id
from providers.sync.plex import _common as common, _history as history, _progress as progress


def obj(rk="11", kind="movie", external="1", **extra):
    return SimpleNamespace(ratingKey=rk, type=kind, librarySectionID="1", title="Different title",
                           guids=[SimpleNamespace(id=f"tmdb://{external}")], **extra)


class Server:
    _token = "test-token"
    machineIdentifier = "server"
    base = "http://plex"

    def __init__(self):
        self.rows = {"11": obj()}
        self.matches = {"tmdb://1": "11"}
        self.queries = []
        self.fail = False
        self.library = None

    def url(self, path):
        return self.base + path

    def query(self, path):
        self.queries.append(path)
        if self.fail:
            raise RuntimeError("Request failed")
        guid = parse_qs(urlsplit(path).query)["guid"][0]
        rk = self.matches.get(guid)
        return ET.fromstring(f'<MediaContainer size="1"><Video ratingKey="{rk}"/></MediaContainer>' if rk else '<MediaContainer size="0"/>')

    def fetchItem(self, rk):
        return self.rows[str(rk)]


def adapter(server=None, pair="pair/a", user="viewer"):
    return SimpleNamespace(client=SimpleNamespace(server=server or Server()), libraries=lambda **kwargs: [],
                           config={"_cw_pair_scope": pair, "plex": {"username": user, "strict_id_matching": True}})


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    token = log_run_id.set("run1")
    monkeypatch.setattr(common._LOOKUP_CACHE, "entry", None, raising=False)
    common._SHOW_EPISODE_CACHE.clear()
    history._clear_guid_index()
    for name in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        monkeypatch.delenv(name, raising=False)
    yield
    log_run_id.reset(token)


def test_query_hits_and_misses_are_reused_across_batches_and_refresh_next_run():
    srv = Server()
    sources = [{"type": "movie", "ids": {"tmdb": external}} for external in ("1", "2")]
    first_count = None
    for _ in range(3):
        assert [progress._resolve_rating_key(adapter(srv), source) for source in sources] == ["11", None]
        if first_count is None:
            first_count = len(srv.queries)
        assert len(srv.queries) == first_count
    srv.rows["22"] = obj("22", external="2")
    srv.matches["tmdb://2"] = "22"
    log_run_id.set("run2")
    assert progress._resolve_rating_key(adapter(srv), sources[1]) == "22"
    assert len(srv.queries) == first_count + 1


@pytest.mark.parametrize("change", ["pair", "user", "token", "base", "machine", "libraries", "run", "feature"])
def test_lookup_cache_cannot_cross_scope(change):
    srv = Server()
    ad = adapter(srv)
    key, _ = history._index_cache_context(ad, set(), "progress")
    with common.guid_lookup_scope(key):
        assert common.server_find_rating_key_by_guid(srv, ["tmdb://1"]) == "11"
    if change == "pair":
        ad.config["_cw_pair_scope"] = "pair?a"
    elif change == "user":
        ad.config["plex"]["username"] = "another-viewer"
    elif change == "run":
        log_run_id.set("run2")
    elif change in {"token", "base", "machine"}:
        setattr(srv, {"token": "_token", "machine": "machineIdentifier"}.get(change, change), "different")
    key, _ = history._index_cache_context(ad, {"1"} if change == "libraries" else set(), "history" if change == "feature" else "progress")
    with common.guid_lookup_scope(key):
        assert common.server_find_rating_key_by_guid(srv, ["tmdb://1"]) == "11"
    assert len(srv.queries) == 2


def test_failed_query_can_retry_in_same_run():
    srv = Server()
    ad = adapter(srv)
    srv.fail = True
    key, _ = history._index_cache_context(ad, set(), "progress")
    with common.guid_lookup_scope(key):
        assert common.server_find_rating_key_by_guid(srv, ["tmdb://1"]) is None
        srv.fail = False
        assert common.server_find_rating_key_by_guid(srv, ["tmdb://1"]) == "11"
    assert len(srv.queries) == 2


@pytest.mark.parametrize("kind,external", [("movie", "999"), ("episode", "1")])
def test_stale_native_id_is_rejected_before_external_lookup(kind, external):
    srv = Server()
    srv.rows["99"] = obj("99", kind=kind, external=external)
    source = {"type": "movie", "ids": {"plex": "99", "tmdb": "1"}}
    assert progress._resolve_rating_key(adapter(srv), source) == "11"
    assert len(srv.queries) == 1


def test_verified_native_id_needs_no_guid_query():
    srv = Server()
    assert progress._resolve_rating_key(adapter(srv), {"type": "movie", "title": "Localized title", "ids": {"plex": "11", "tmdb": "1"}}) == "11"
    assert srv.queries == []


def test_episode_cache_sees_new_episodes_on_next_run():
    srv = Server()
    rows = [SimpleNamespace(ratingKey="101", parentIndex=0, index=1)]
    calls = []

    def episodes():
        calls.append(1)
        return list(rows)

    for episode, run, expected in [(1, "run1", "101"), (2, "run1", None), (2, "run2", "102")]:
        log_run_id.set(run)
        ad = adapter(srv)
        key, _ = history._index_cache_context(ad, set(), "progress")
        show = SimpleNamespace(ratingKey="10", _server=srv, episodes=episodes)
        with common.guid_lookup_scope(key):
            assert common.episode_rating_key_from_show(show, 0, episode) == expected
        if episode == 1:
            rows.append(SimpleNamespace(ratingKey="102", parentIndex=0, index=2))
    assert len(calls) == 2


def test_no_pair_scope_never_shares_queries_between_adapters():
    srv = Server()
    for _ in range(2):
        assert progress._resolve_rating_key(adapter(srv, pair=None), {"type": "movie", "ids": {"tmdb": "1"}}) == "11"
    assert len(srv.queries) == 2


@pytest.mark.parametrize("wrong", ["series", "number"])
def test_native_episode_with_only_series_ids_checks_parent_and_number(wrong):
    source = {"type": "episode", "ids": {"plex": "99"}, "show_ids": {"tmdb": "10"}, "season": 0, "episode": 2}
    parent = obj("20", "show", external="999" if wrong == "series" else "10")
    stale = obj("99", "episode", parentIndex=0, index=1 if wrong == "number" else 2, show=lambda: parent)
    assert not progress._native_progress_identity_matches(stale, source, source["ids"])
    correct = obj("100", "episode", parentIndex=0, index=2, show=lambda: obj("20", "show", external="10"))
    assert progress._native_progress_identity_matches(correct, source, source["ids"])


def test_exact_episode_id_leads_over_numbering_and_numeric_id_padding():
    source = {"type": "episode", "ids": {"plex": "99", "tvdb": "000123"}, "season": 1, "episode": 2}
    target = obj("99", "episode", parentIndex=2, index=4)
    target.guids = [SimpleNamespace(id="tvdb://123")]
    assert progress._native_progress_identity_matches(target, source, source["ids"])
