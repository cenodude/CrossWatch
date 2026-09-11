from copy import deepcopy
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from xml.etree import ElementTree as ET

import pytest
from plexapi.server import PlexServer

from providers.sync.plex import _common as common, _history as history, _ratings as ratings


def media(kind, key, *, show="73739", season=1, episode=2, parent="900", modern=False):
    row = {"type": kind, "ratingKey": key, "key": f"/library/metadata/{key}",
           "librarySectionID": "1", "title": "Test item"}
    suffix = f"/{season}/{episode}" if kind == "episode" else f"/{season}" if kind == "season" else ""
    row["guid"] = f"com.plexapp.agents.thetvdb://{show}{suffix}?lang=en"
    if modern:
        row["guid"] = f"plex://{kind}/{key}"
        row["Guid"] = [{"id": f"tvdb://{show if kind == 'show' else '98765'}"}]
    if kind in ("episode", "season"):
        row.update(index=episode if kind == "episode" else season,
                   parentKey=f"/library/metadata/{parent}", parentRatingKey=parent,
                   parentGuid=f"tvdb://{show}")
    if kind == "episode":
        row.update(parentIndex=season, grandparentKey=f"/library/metadata/{parent}",
                   grandparentRatingKey=parent, grandparentGuid=f"tvdb://{show}", grandparentTitle="Test show")
    return row


class PlexTransport:
    def __init__(self):
        self.headers = {"X-Plex-Token": "test-token"}
        self.rows = {}
        self.writes = []
        self.reads = []

    def get(self, url, params=None, **kwargs):
        parsed = urlsplit(url)
        path = parsed.path
        query = {key: values[0] for key, values in parse_qs(parsed.query).items()}
        query.update(params or {})
        self.reads.append(path)
        if path in ("/:/scrobble", "/:/rate"):
            self.writes.append((path, str(query.get("key") or query.get("ratingKey"))))
            rows = []
        elif path == "/":
            rows = []
        elif path == "/library/all":
            guid = query.get("guid")
            rows = [row for row in self.rows.values() if row["guid"] == guid
                    or any(value["id"] == guid for value in row.get("Guid", []))][:1]
        elif path == "/library/sections/1/all":
            kind = {1: "movie", 2: "show", 4: "episode"}[int(query["type"])]
            rows = [row for row in self.rows.values() if row["type"] == kind]
        elif path.endswith("/allLeaves"):
            parent = path.split("/")[-2]
            rows = [row for row in self.rows.values() if row.get("grandparentRatingKey") == parent]
        elif path.startswith("/library/metadata/"):
            key = path.split("/")[-1]
            rows = [self.rows[key]] if key in self.rows else []
        else:
            raise AssertionError(f"Unexpected Plex request: {path}")
        root = ET.Element("MediaContainer", size=str(len(rows)), totalSize=str(len(rows)), machineIdentifier="test-server")
        for row in rows:
            node = ET.SubElement(root, "Video" if row["type"] in ("movie", "episode") else "Directory",
                                 {key: str(value) for key, value in row.items() if key != "Guid"})
            for guid in row.get("Guid", []):
                ET.SubElement(node, "Guid", guid)
        return SimpleNamespace(ok=True, status_code=200, text=ET.tostring(root, encoding="unicode"),
                               headers={"content-type": "application/xml"})


@pytest.fixture
def plex(monkeypatch, tmp_path):
    transport = PlexTransport()
    server = PlexServer("http://plex.test", "test-token", session=transport)
    adapter = SimpleNamespace(client=SimpleNamespace(server=server), cfg=SimpleNamespace(), config={"plex": {
        "strict_id_matching": True, "history_workers": 1,
        "history": {"libraries": [1]}, "ratings": {"libraries": [1]},
    }}, libraries=lambda **kwargs: [SimpleNamespace(key="1", type="show")])
    monkeypatch.setattr(common, "STATE_DIR", tmp_path)
    monkeypatch.setattr(history._INDEX_CACHE, "guid", None, raising=False)
    monkeypatch.setattr(common, "_SHOW_EPISODE_CACHE", {})
    for module in (history, ratings):
        monkeypatch.setattr(module, "home_scope_enter", lambda *args: (False, False, None, None))
        monkeypatch.setattr(module, "home_scope_exit", lambda *args: None)
    return SimpleNamespace(adapter=adapter, server=server, transport=transport)


def source_item(kind="episode", *, season=1, modern=False):
    row = common.normalize_discover_row(media(kind, "1234", season=season, modern=modern), token=None)
    assert row is not None
    if kind == "season":
        row["show_ids"] = {"tvdb": "73739", "plex": "900"}
    row.update(watched_at="2026-01-01T12:00:00Z", rating=8)
    return row


def ambiguous_catalogue(monkeypatch, item):
    cat = history.HistoryCatalog()
    for key in ("1001", "1002"):
        cat.add({"rk": key, "type": "episode", "ids": {"plex": key}, "show_ids": item["show_ids"],
                 "season": item["season"], "episode": item["episode"], "watched": False})
    assert cat.resolve(item, strict=True) == (None, history.CLASS_RESOLVE_AMBIGUOUS)
    monkeypatch.setattr(history, "_get_history_catalog", lambda *args, **kwargs: cat)


@pytest.mark.parametrize(("kind", "case"), [
    (kind, case) for kind in ("episode", "season")
    for case in ("correct", "wrong_season", "wrong_episode", "wrong_parent")
    if kind == "episode" or case != "wrong_episode"
])
@pytest.mark.parametrize("modern", [False, True])
def test_native_match_checks_coordinates_and_parent(plex, kind, modern, case):
    item = source_item(kind, modern=modern)
    plex.transport.rows["1234"] = media(kind, "1234", modern=modern,
        season=7 if case == "wrong_season" else 1, episode=11 if case == "wrong_episode" else 2)
    plex.transport.rows["900"] = media("show", "900", show="99999" if case == "wrong_parent" else "73739", modern=modern)
    obj = plex.server.fetchItem(1234)
    assert common.native_item_matches(obj, item) is (case == "correct")


@pytest.mark.parametrize(("feature", "kind"), [("history", "episode"), ("ratings", "episode"), ("ratings", "season")])
@pytest.mark.parametrize("modern", [False, True])
@pytest.mark.parametrize("case", ["correct", "wrong_episode", "wrong_parent"])
def test_writes_never_use_wrong_native_episode(plex, monkeypatch, feature, kind, modern, case):
    item = source_item(kind, modern=modern)
    plex.transport.rows["1234"] = media(kind, "1234", episode=11 if case == "wrong_episode" else 2,
        season=7 if case == "wrong_episode" and kind == "season" else 1, modern=modern,
        show="99999" if case == "wrong_parent" else "73739")
    plex.transport.rows["900"] = media("show", "900", show="99999" if case == "wrong_parent" else "73739")
    if feature == "history":
        ambiguous_catalogue(monkeypatch, item)
    module = history if feature == "history" else ratings
    count, unresolved = module.add(plex.adapter, [item])
    assert count == int(case == "correct")
    assert bool(unresolved) is (case != "correct")
    endpoint = "/:/scrobble" if feature == "history" else "/:/rate"
    assert plex.transport.writes == ([(endpoint, "1234")] if case == "correct" else [])


@pytest.mark.parametrize("correct_available", [False, True])
def test_history_reused_parent_key_cannot_override_external_show_id(plex, monkeypatch, correct_available):
    item = source_item()
    plex.transport.rows["1234"] = media("episode", "1234", show="99999", season=7, episode=11)
    plex.transport.rows["900"] = media("show", "900", show="99999")
    plex.transport.rows["5555"] = media("episode", "5555", show="99999")
    if correct_available:
        plex.transport.rows["901"] = media("show", "901")
        plex.transport.rows["5678"] = media("episode", "5678", parent="901")
    ambiguous_catalogue(monkeypatch, item)
    count, unresolved = history.add(plex.adapter, [item])
    assert count == int(correct_available)
    assert bool(unresolved) is not correct_available
    assert plex.transport.writes == ([("/:/scrobble", "5678")] if correct_available else [])
    assert "/library/metadata/900/allLeaves" not in plex.transport.reads


@pytest.mark.parametrize("feature", ["history", "ratings"])
def test_specials_keep_zero_season_during_external_id_fallback(plex, monkeypatch, feature):
    item = source_item(season=0)
    plex.transport.rows["900"] = media("show", "900")
    plex.transport.rows["1111"] = media("episode", "1111", season=1)
    plex.transport.rows["2222"] = media("episode", "2222", season=0)
    if feature == "history":
        ambiguous_catalogue(monkeypatch, item)
    module = history if feature == "history" else ratings
    count, unresolved = module.add(plex.adapter, [item])
    assert (count, unresolved) == (1, [])
    endpoint = "/:/scrobble" if feature == "history" else "/:/rate"
    assert plex.transport.writes == [(endpoint, "2222")]


def test_native_only_episode_still_requires_coordinates(plex):
    plex.transport.rows["1234"] = media("episode", "1234")
    obj = plex.server.fetchItem(1234)
    item = {"type": "episode", "ids": {"plex": "1234"}, "season": 1, "episode": 2}
    assert common.native_item_matches(obj, item)
    wrong = deepcopy(item)
    wrong["episode"] = 11
    assert not common.native_item_matches(obj, wrong)


@pytest.mark.parametrize("identity", ["episode_external_id", "show_guid"])
@pytest.mark.parametrize("correct_available", [False, True])
def test_history_parent_shortcut_requires_external_show_evidence(plex, monkeypatch, identity, correct_available):
    item = source_item(modern=True)
    item["show_ids"] = {"plex": "900"}
    plex.transport.rows["900"] = media("show", "900", show="99999")
    plex.transport.rows["5555"] = media("episode", "5555", show="99999")
    if identity == "show_guid":
        item["ids"] = {"plex": "1234"}
        item["guid"] = "plex://episode/original-episode"
        item["show_guid"] = "plex://show/original-show"
    if correct_available:
        plex.transport.rows["901"] = media("show", "901")
        plex.transport.rows["5678"] = media("episode", "5678", parent="901", modern=True)
        if identity == "show_guid":
            plex.transport.rows["901"]["guid"] = item["show_guid"]
    ambiguous_catalogue(monkeypatch, item)
    count, unresolved = history.add(plex.adapter, [item])
    assert count == int(correct_available)
    assert bool(unresolved) is not correct_available
    assert plex.transport.writes == ([("/:/scrobble", "5678")] if correct_available else [])
    assert "/library/metadata/900/allLeaves" not in plex.transport.reads
