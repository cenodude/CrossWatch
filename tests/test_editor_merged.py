from types import SimpleNamespace

from services import editor_merged
from services.editor_merged import merge_sources


def test_same_movie_merges_across_providers_by_shared_id() -> None:
    trakt = {"trakt:10": {"type": "movie", "title": "Heat", "year": 1995, "ids": {"trakt": "10", "imdb": "tt0113277"}}}
    simkl = {"imdb:tt0113277": {"type": "movie", "title": "Heat", "year": 1995, "ids": {"imdb": "tt0113277", "tmdb": "949"}}}
    items, presence = merge_sources("watchlist", [trakt, simkl])
    assert len(items) == 1
    key, item = next(iter(items.items()))
    assert item["ids"] == {"tmdb": "949", "imdb": "tt0113277", "trakt": "10"}
    assert [entry[0] for entry in presence[key]] == [0, 1]


def test_server_local_ids_and_bare_show_ids_do_not_merge() -> None:
    plex = {"plex:5": {"type": "movie", "title": "A", "ids": {"plex": "5"}}}
    emby = {"plex:5": {"type": "movie", "title": "B", "ids": {"plex": "5", "emby": "9"}}}
    show = {"tmdb:1#show": {"type": "show", "title": "Show", "ids": {"tmdb": "1"}}}
    movie = {"tmdb:1": {"type": "movie", "title": "Movie", "ids": {"tmdb": "1"}}}
    episode = {"tmdb:1#s01e02": {"type": "episode", "season": 1, "episode": 2, "show_ids": {"tmdb": "1"}, "ids": {}}}
    items, _ = merge_sources("watchlist", [show, movie, episode])
    assert len(items) == 3
    items, presence = merge_sources("watchlist", [plex, emby])
    assert len(items) == 1
    assert sum(len(rows) for rows in presence.values()) == 2


def test_history_collapses_rewatches_and_reports_latest_watch_per_provider() -> None:
    first = {"type": "episode", "season": 1, "episode": 2, "series_title": "MobLand", "show_ids": {"tmdb": "247718"},
             "ids": {"trakt": "77"}, "watched_at": "2025-04-01T10:00:00Z"}
    trakt = {
        "tmdb:247718#s01e02@1743501600": {**first, "_cw_event_key": "tmdb:247718#s01e02@1743501600"},
        "tmdb:247718#s01e02@1746093600": {**first, "watched_at": "2025-05-01T10:00:00Z"},
    }
    simkl = {"tvdb:9#s01e02": {"type": "episode", "season": 1, "episode": 2,
                               "show_ids": {"tvdb": "9", "tmdb": "247718"}, "ids": {}, "watched_at": "2025-03-01T10:00:00Z"}}
    items, presence = merge_sources("history", [trakt, simkl])
    assert len(items) == 1
    key, item = next(iter(items.items()))
    assert "@" not in key
    assert "_cw_event_key" not in item
    assert item["watched_at"] == "2025-05-01T10:00:00Z"
    assert presence[key] == [[0, 2, "2025-05-01T10:00:00Z"], [1, 1, "2025-03-01T10:00:00Z"]]


def test_anime_and_show_types_merge() -> None:
    simkl = {"mal:1": {"type": "anime", "title": "Cowboy Bebop", "ids": {"mal": "1", "tmdb": "30991"}}}
    trakt = {"tmdb:30991#show": {"type": "show", "title": "Cowboy Bebop", "ids": {"tmdb": "30991"}}}
    items, _ = merge_sources("watchlist", [simkl, trakt])
    assert len(items) == 1


def test_accounts_linked_by_an_enabled_pair_join_the_view_one_hop_only() -> None:
    cfg = {"pairs": [
        {"source": "SIMKL", "source_instance": "SIMKL-P01", "target": "ANILIST", "target_instance": "default", "feature": "history"},
        {"source": "TRAKT", "source_instance": "p9", "target": "SIMKL", "target_instance": "SIMKL-P01", "feature": "history"},
        {"source": "PLEX", "source_instance": "p2", "target": "ANILIST", "target_instance": "default", "feature": "history", "enabled": False},
        {"source": "MDBLIST", "source_instance": "p3", "target": "ANILIST", "target_instance": "default", "feature": "watchlist"},
    ]}
    assert editor_merged._linked(cfg, "history", None) == {("SIMKL", "SIMKL-P01")}
    assert editor_merged._linked(cfg, "history", {"SIMKL": ["SIMKL-P01"]}) == {("ANILIST", "default"), ("TRAKT", "p9")}


def test_only_accounts_in_an_enabled_pair_for_the_list_count_as_checked() -> None:
    cfg = {"pairs": [
        {"source": "SIMKL", "source_instance": "SIMKL-P01", "target": "ANILIST", "features": {"history": {"enable": True}}},
        {"source": "TRAKT", "target": "PUNCHPLAY", "enabled": False, "features": {"history": {"enable": True}}},
    ]}

    assert editor_merged._paired(cfg, "history") == {("CROSSWATCH", "default"), ("SIMKL", "SIMKL-P01"), ("ANILIST", "default")}
    assert editor_merged._paired(cfg, "watchlist") == {("CROSSWATCH", "default")}


def test_scope_is_one_profile_and_defaults_to_default_instances(monkeypatch) -> None:
    cfg = {}
    monkeypatch.setattr(editor_merged, "profile_instances_map", lambda _cfg, pid: {"TRAKT": ["p2"]} if pid == "anna" else {})
    managed = SimpleNamespace(state=SimpleNamespace(cw_user={"is_admin": False, "profile_id": "anna"}))
    monkeypatch.setattr(editor_merged, "managed_profile_id", lambda user: "" if not user or user.get("is_admin") else user["profile_id"])
    profile, instances, chooser = editor_merged._scope(cfg, managed)
    assert (profile, instances, chooser) == ("anna", {"TRAKT": ["p2"]}, False)
    assert editor_merged._in_scope(instances, "TRAKT", "p2")
    assert not editor_merged._in_scope(instances, "TRAKT", "default")
    profile, instances, chooser = editor_merged._scope(cfg, None)
    assert (profile, instances, chooser) == ("", None, True)
    assert editor_merged._in_scope(instances, "TRAKT", "default")
    assert not editor_merged._in_scope(instances, "TRAKT", "p2")
