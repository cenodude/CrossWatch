from __future__ import annotations

import importlib
from typing import Any

import pytest

plpl = importlib.import_module("providers.sync.plex._playlists")


class FObj:
    def __init__(
        self,
        tmdb: int,
        title: str,
        year: int = 2020,
        typ: str = "movie",
        rk: int | None = None,
        plid: int | None = None,
        section_id: int | str | None = None,
    ):
        self.type = typ
        self.title = title
        self.year = year
        self.guid = f"tmdb://{tmdb}"
        self.guids: list[Any] = []
        self.ratingKey = rk if rk is not None else int(tmdb)
        self.playlistItemID = plid
        if section_id is not None:
            self.librarySectionID = str(section_id)


class FakePlaylist:
    def __init__(self, title: str, rk: int, items: list[Any], smart: bool = False, ptype: str = "video"):
        self.title = title
        self.ratingKey = rk
        self.smart = smart
        self.playlistType = ptype
        self._items = list(items)
        self.calls: list[Any] = []

    def items(self) -> list[Any]:
        return list(self._items)

    def addItems(self, objs: Any) -> None:
        lst = objs if isinstance(objs, list) else [objs]
        self.calls.append(("add", lst))
        self._items.extend(lst)

    def removeItems(self, obj: Any) -> None:
        self.calls.append(("remove", obj))
        self._items.remove(obj)

    def moveItem(self, obj: Any, after: Any = None) -> None:
        self.calls.append(("move", obj, after))
        self._items.remove(obj)
        if after is None:
            self._items.insert(0, obj)
        else:
            idx = self._items.index(after)
            self._items.insert(idx + 1, obj)


class FakeCollection:
    def __init__(
        self,
        title: str,
        rk: int,
        items: list[Any],
        smart: bool = False,
        subtype: str = "movie",
        section_id: int | str = 1,
    ):
        self.title = title
        self.ratingKey = rk
        self.key = str(rk)
        self.smart = smart
        self.subtype = subtype
        self.childCount = len(items)
        self.librarySectionID = str(section_id)
        self.librarySectionKey = str(section_id)
        self._items = list(items)
        self.calls: list[Any] = []

    def items(self) -> list[Any]:
        return list(self._items)

    def addItems(self, objs: Any) -> None:
        lst = objs if isinstance(objs, list) else [objs]
        self.calls.append(("add", lst))
        self._items.extend(lst)
        self.childCount = len(self._items)

    def removeItems(self, obj: Any) -> None:
        self.calls.append(("remove", obj))
        self._items.remove(obj)
        self.childCount = len(self._items)


class FakeSection:
    def __init__(self, title: str, key: int | str, typ: str, collections: list[FakeCollection]):
        self.title = title
        self.key = str(key)
        self.type = typ
        self._collections = collections

    def collections(self) -> list[FakeCollection]:
        return list(self._collections)


class FakeLibrary:
    def __init__(self, sections: list[FakeSection]):
        self._sections = sections

    def sections(self) -> list[FakeSection]:
        return list(self._sections)


class FakeServer:
    def __init__(self, playlists: list[FakePlaylist], library: list[FObj], sections: list[FakeSection] | None = None):
        self._playlists = playlists
        self.items_library = library
        self.library = FakeLibrary(sections or [])
        self.created: list[FakePlaylist] = []

    def playlists(self) -> list[FakePlaylist]:
        return list(self._playlists)

    def createPlaylist(self, name: str, items: Any = None) -> FakePlaylist:
        pl = FakePlaylist(name, 999, items or [])
        self.created.append(pl)
        self._playlists.append(pl)
        return pl


class FakeClient:
    def __init__(self, server: FakeServer):
        self.server = server


class FakeAdapter:
    def __init__(self, server: FakeServer):
        self.client = FakeClient(server)
        self.instance_id = "default"


def _fake_resolve(srv: FakeServer, guids: list[str], allow: set, accept: set) -> Any:
    for g in guids:
        for obj in srv.items_library:
            if obj.guid == g and obj.type in accept:
                section = str(getattr(obj, "librarySectionID", "") or "")
                if allow and section and section not in allow:
                    continue
                return obj
    return None


@pytest.fixture(autouse=True)
def _patch_resolve(monkeypatch):
    monkeypatch.setattr(plpl, "resolve_obj_by_guids", _fake_resolve)


def _media(tmdb: int, title: str) -> dict[str, Any]:
    return {"type": "movie", "title": title, "year": 2020, "ids": {"tmdb": str(tmdb)}}


def test_capability_detection_regular_and_smart():
    a = FObj(1, "A", plid=101)
    regular = FakePlaylist("Weekend", 10, [a], smart=False)
    smart = FakePlaylist("Recently Added", 20, [a], smart=True)
    ad = FakeAdapter(FakeServer([regular, smart], [a]))

    res = plpl.list_resources(ad)
    by_id = {r.id: r for r in res}
    assert by_id[plpl.WATCHLIST_ID].name == "Watchlist"
    assert by_id[plpl.WATCHLIST_ID].can_add is True
    assert by_id[plpl.WATCHLIST_ID].can_reorder is False
    assert by_id["10"].is_smart is False
    assert by_id["10"].can_add is True and by_id["10"].can_reorder is True
    assert by_id["20"].is_smart is True
    assert by_id["20"].can_add is False and by_id["20"].can_remove is False


def test_collection_resources_manual_and_smart():
    movie = FObj(1, "A", section_id=1)
    show = FObj(2, "S", typ="show", section_id=2)
    manual = FakeCollection("Movies", 30, [movie], smart=False, subtype="movie", section_id=1)
    smart = FakeCollection("Shows", 40, [show], smart=True, subtype="show", section_id=2)
    sections = [
        FakeSection("Movies", 1, "movie", [manual]),
        FakeSection("Shows", 2, "show", [smart]),
    ]
    ad = FakeAdapter(FakeServer([], [movie, show], sections))

    res = plpl.list_resources(ad)
    by_id = {r.id: r for r in res}
    movie_id = f"{plpl.COLLECTION_PREFIX}1:30"
    show_id = f"{plpl.COLLECTION_PREFIX}2:40"
    assert by_id[movie_id].name == "Movies"
    assert by_id[movie_id].extra["endpoint_type"] == "collection"
    assert by_id[movie_id].media_types == ("movie",)
    assert by_id[movie_id].can_add is True and by_id[movie_id].can_reorder is False
    assert by_id[show_id].is_smart is True
    assert by_id[show_id].media_types == ("show",)
    assert by_id[show_id].can_add is False and by_id[show_id].can_remove is False


def test_watchlist_snapshot_and_writes_delegate(monkeypatch):
    monkeypatch.setattr(
        plpl.feat_watchlist,
        "build_index",
        lambda ad: {"tmdb:1": {"type": "movie", "title": "A", "ids": {"tmdb": "1"}}},
    )
    monkeypatch.setattr(plpl.feat_watchlist, "add", lambda ad, items: (len(list(items)), []))
    monkeypatch.setattr(plpl.feat_watchlist, "remove", lambda ad, items: (len(list(items)), []))

    a = FObj(1, "A", plid=101)
    ad = FakeAdapter(FakeServer([], [a]))
    snap = plpl.get_snapshot(ad, plpl.WATCHLIST_ID)
    assert snap.resource.name == "Watchlist"
    assert snap.ordered_keys() == ["tmdb:1"]

    item = _media(1, "A")
    add_res = plpl.add(ad, plpl.WATCHLIST_ID, [item])
    rm_res = plpl.remove(ad, plpl.WATCHLIST_ID, [item])
    assert add_res["count"] == 1 and add_res["confirmed_keys"] == ["tmdb:1"]
    assert rm_res["count"] == 1 and rm_res["confirmed_keys"] == ["tmdb:1"]
    assert plpl.reorder(ad, plpl.WATCHLIST_ID, ["tmdb:1"])["unsupported"] is True


def test_collection_snapshot_and_writes_use_scoped_ids():
    a = FObj(1, "A", section_id=1)
    b = FObj(2, "B", section_id=1)
    other = FObj(2, "B Other", section_id=2)
    coll = FakeCollection("Movies", 30, [a], subtype="movie", section_id=1)
    sections = [FakeSection("Movies", 1, "movie", [coll])]
    ad = FakeAdapter(FakeServer([], [a, other, b], sections))
    collection_id = f"{plpl.COLLECTION_PREFIX}1:30"

    snap = plpl.get_snapshot(ad, collection_id)
    assert snap.resource.name == "Movies"
    assert snap.resource.can_reorder is False
    assert snap.ordered_keys() == ["tmdb:1"]
    assert snap.items[0].position is None

    add_res = plpl.add(ad, collection_id, [_media(2, "B")])
    assert add_res["count"] == 1
    assert add_res["confirmed_keys"] == ["tmdb:2"]
    assert coll.calls[-1][0] == "add"
    assert coll.calls[-1][1][0] is b

    rm_res = plpl.remove(ad, collection_id, [_media(1, "A")])
    assert rm_res["count"] == 1
    assert rm_res["confirmed_keys"] == ["tmdb:1"]
    assert coll.calls[-1][0] == "remove"


def test_collection_missing_library_media_reports_not_in_library():
    a = FObj(1, "A", section_id=1)
    coll = FakeCollection("Movies", 30, [a], subtype="movie", section_id=1)
    sections = [FakeSection("Movies", 1, "movie", [coll])]
    ad = FakeAdapter(FakeServer([], [a], sections))

    res = plpl.add(ad, f"{plpl.COLLECTION_PREFIX}1:30", [_media(999, "Ghost")])
    assert res["count"] == 0
    assert res["confirmed_keys"] == []
    assert res["unresolved"] and res["unresolved"][0]["hint"] == "not_in_library"


def test_smart_collection_writes_rejected_and_reorder_unsupported():
    a = FObj(1, "A", section_id=1)
    coll = FakeCollection("Smart Movies", 30, [a], smart=True, subtype="movie", section_id=1)
    sections = [FakeSection("Movies", 1, "movie", [coll])]
    ad = FakeAdapter(FakeServer([], [a], sections))
    collection_id = f"{plpl.COLLECTION_PREFIX}1:30"

    with pytest.raises(plpl.SmartPlaylistError):
        plpl.add(ad, collection_id, [_media(1, "A")])
    with pytest.raises(plpl.SmartPlaylistError):
        plpl.remove(ad, collection_id, [_media(1, "A")])
    assert plpl.reorder(ad, collection_id, ["tmdb:1"])["unsupported"] is True


def test_missing_library_media_is_unresolved():
    a = FObj(1, "A", plid=101)
    regular = FakePlaylist("Weekend", 10, [a], smart=False)
    ad = FakeAdapter(FakeServer([regular], [a]))

    res = plpl.add(ad, "10", [_media(999, "Ghost")])
    assert res["count"] == 0
    assert res["confirmed_keys"] == []
    assert res["unresolved"] and res["unresolved"][0]["hint"] == "not_found"


def test_smart_playlist_writes_rejected():
    a = FObj(1, "A", plid=101)
    smart = FakePlaylist("Smart", 20, [a], smart=True)
    ad = FakeAdapter(FakeServer([smart], [a]))

    with pytest.raises(plpl.SmartPlaylistError):
        plpl.add(ad, "20", [_media(1, "A")])
    with pytest.raises(plpl.SmartPlaylistError):
        plpl.remove(ad, "20", [_media(1, "A")])
    with pytest.raises(plpl.SmartPlaylistError):
        plpl.reorder(ad, "20", ["tmdb:1"])


def test_add_remove_basic():
    a = FObj(1, "A", plid=101)
    b = FObj(2, "B", plid=102)
    regular = FakePlaylist("Weekend", 10, [a], smart=False)
    ad = FakeAdapter(FakeServer([regular], [a, b]))

    add_res = plpl.add(ad, "10", [_media(2, "B")])
    assert add_res["count"] == 1
    assert add_res["confirmed_keys"] == ["tmdb:2"]

    rm_res = plpl.remove(ad, "10", [_media(1, "A")])
    assert rm_res["count"] == 1
    assert rm_res["confirmed_keys"] == ["tmdb:1"]


def test_snapshot_ordered():
    a = FObj(1, "A", plid=101)
    b = FObj(2, "B", plid=102)
    regular = FakePlaylist("Weekend", 10, [a, b], smart=False)
    ad = FakeAdapter(FakeServer([regular], [a, b]))
    snap = plpl.get_snapshot(ad, "10")
    assert snap.ordered_keys() == ["tmdb:1", "tmdb:2"]
    assert snap.items[0].playlist_item_id == "101"


def test_reorder_minimal_moves_and_idempotent():
    a = FObj(1, "A", plid=101)
    b = FObj(2, "B", plid=102)
    c = FObj(3, "C", plid=103)
    regular = FakePlaylist("Weekend", 10, [a, b, c], smart=False)
    ad = FakeAdapter(FakeServer([regular], [a, b, c]))

    res = plpl.reorder(ad, "10", ["tmdb:3", "tmdb:1", "tmdb:2"])
    assert res["reordered"] >= 1
    assert [o.ratingKey for o in regular.items()] == [3, 1, 2]

    res2 = plpl.reorder(ad, "10", ["tmdb:3", "tmdb:1", "tmdb:2"])
    assert res2["reordered"] == 0
