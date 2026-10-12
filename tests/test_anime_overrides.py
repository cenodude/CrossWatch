# CrossWatch test scripts
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import overrides as ov
from cw_platform.anime_mapping import storage
from cw_platform.anime_mapping.episodes import resolve_absolute
from cw_platform.anime_mapping.service import AnimeMappingService

MAPPINGS: dict[str, Any] = {
    "tvdb_show:81472:s2": {
        "anilist:813": {"1-35": "40-74"},
        "mal:813": {"1-35": "40-74"},
    },
    "tvdb_show:600:s1": {"mal:600": {"1-12": "1-12"}},
}


@pytest.fixture()
def index(config_base: Path) -> Path:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    return paths["db"]


def _episode(show_ids: dict[str, str], season: int, episode: int) -> dict[str, Any]:
    return {"type": "episode", "show_ids": show_ids, "season": season, "episode": episode}


def _rule(**kw: Any) -> dict[str, Any]:
    base = {
        "media_type": "show",
        "title": "Rule",
        "match_provider": "tvdb",
        "match_id": "81472",
        "match_season": 2,
        "target_namespace": "mal",
        "target_id": "999",
        "episode_from": 1,
        "episode_to": 35,
        "episode_start_at": 500,
    }
    base.update(kw)
    return base


# --- storage / validation -----------------------------------------------------


def test_round_trip_persists_a_rule(config_base: Path) -> None:
    saved = ov.upsert_override(_rule())
    assert saved["id"].startswith("ovr_")
    rows = ov.load_overrides()
    assert len(rows) == 1
    assert rows[0]["target_id"] == "999"
    assert rows[0]["enabled"] is True


def test_upsert_updates_in_place_and_keeps_created_at(config_base: Path) -> None:
    first = ov.upsert_override(_rule())
    second = ov.upsert_override(_rule(id=first["id"], target_id="1000"))
    rows = ov.load_overrides()
    assert len(rows) == 1
    assert rows[0]["target_id"] == "1000"
    assert second["created_at"] == first["created_at"]


def test_delete_removes_only_the_named_rule(config_base: Path) -> None:
    a = ov.upsert_override(_rule(match_id="1"))
    ov.upsert_override(_rule(match_id="2"))
    assert ov.delete_override(a["id"]) is True
    assert [r["match_id"] for r in ov.load_overrides()] == ["2"]
    assert ov.delete_override("nope") is False


def test_export_round_trips_through_import(config_base: Path) -> None:
    ov.upsert_override(_rule(match_id="1", title="One"))
    ov.upsert_override(_rule(match_id="2", title="Two"))
    payload = ov.export_payload()
    assert len(payload["overrides"]) == 2

    ov.save_overrides([])
    result = ov.import_overrides(payload, mode="merge")

    assert result["added"] == 2
    assert result["updated"] == 0
    assert result["skipped_count"] == 0
    assert [r["title"] for r in ov.load_overrides()] == ["One", "Two"]


def test_import_merge_keeps_untouched_rules_and_preserves_created_at(config_base: Path) -> None:
    keep = ov.upsert_override(_rule(match_id="1", title="Keep"))
    existing = ov.upsert_override(_rule(match_id="2", title="Old"))

    result = ov.import_overrides(
        {"overrides": [_rule(id=existing["id"], match_id="2", title="New"), _rule(match_id="3", title="Fresh")]},
        mode="merge",
    )

    rows = {r["id"]: r for r in ov.load_overrides()}
    assert result["added"] == 1
    assert result["updated"] == 1
    assert len(rows) == 3
    assert rows[keep["id"]]["title"] == "Keep"
    assert rows[existing["id"]]["title"] == "New"
    assert rows[existing["id"]]["created_at"] == existing["created_at"]


def test_import_replace_drops_rules_not_in_the_file(config_base: Path) -> None:
    ov.upsert_override(_rule(match_id="1", title="Gone"))
    result = ov.import_overrides([_rule(match_id="9", title="Only")], mode="replace")

    assert result["mode"] == "replace"
    assert [r["title"] for r in ov.load_overrides()] == ["Only"]
    assert result["total"] == 1


def test_import_skips_invalid_rules_but_keeps_the_valid_ones(config_base: Path) -> None:
    result = ov.import_overrides(
        {
            "overrides": [
                _rule(match_id="1", title="Good"),
                _rule(match_id="2", title="Bad", match_provider="nope"),
                "not-a-rule",
            ]
        },
        mode="merge",
    )

    assert result["imported"] == 1
    assert result["skipped_count"] == 2
    assert result["skipped"][0]["title"] == "Bad"
    assert [r["title"] for r in ov.load_overrides()] == ["Good"]


def test_import_rejects_a_file_with_no_usable_rules(config_base: Path) -> None:
    ov.upsert_override(_rule(match_id="1", title="Keep"))

    with pytest.raises(ov.OverrideError):
        ov.import_overrides({"overrides": [_rule(match_provider="nope")]}, mode="replace")
    with pytest.raises(ov.OverrideError):
        ov.import_overrides({"rules": []}, mode="merge")
    with pytest.raises(ov.OverrideError):
        ov.import_overrides({"overrides": []}, mode="wipe")

    assert [r["title"] for r in ov.load_overrides()] == ["Keep"]


@pytest.mark.parametrize(
    "patch",
    [
        {"match_provider": "netflix"},
        {"match_id": ""},
        {"target_namespace": "tvdb"},
        {"target_id": ""},
        {"match_id": "a b"},
        {"episode_from": 0},
        {"episode_from": 10, "episode_to": 4},
        {"episode_start_at": None},
        {"match_season": None},
        {"media_type": "album"},
    ],
)
def test_invalid_rules_are_rejected(config_base: Path, patch: dict[str, Any]) -> None:
    with pytest.raises(ov.OverrideError):
        ov.upsert_override(_rule(**patch))
    assert ov.load_overrides() == []


def test_movie_rules_cannot_carry_episode_fields(config_base: Path) -> None:
    with pytest.raises(ov.OverrideError):
        ov.upsert_override(_rule(media_type="movie"))
    saved = ov.upsert_override(
        {
            "media_type": "movie",
            "match_provider": "tmdb",
            "match_id": "810693",
            "target_namespace": "mal",
            "target_id": "48561",
        }
    )
    assert saved["episode_start_at"] is None
    assert saved["match_season"] is None


def test_identity_only_show_rule_is_allowed(config_base: Path) -> None:
    saved = ov.upsert_override(
        {
            "media_type": "show",
            "match_provider": "tvdb",
            "match_id": "81472",
            "target_namespace": "simkl",
            "target_id": "41487",
        }
    )
    assert saved["episode_start_at"] is None


def test_corrupt_rows_are_skipped_on_load(config_base: Path) -> None:
    ov.upsert_override(_rule())
    path = ov.overrides_path()
    data = json.loads(path.read_text("utf-8"))
    data["overrides"].append({"match_provider": "bogus"})
    data["overrides"].append("not-a-dict")
    path.write_text(json.dumps(data), encoding="utf-8")
    assert len(ov.load_overrides()) == 1


# --- precedence over the dataset ---------------------------------------------


def test_override_beats_the_dataset(index: Path) -> None:
    before = resolve_absolute(_episode({"tvdb": "81472"}, 2, 13))
    assert before is not None and before.absolute == 52

    ov.upsert_override(_rule())
    after = resolve_absolute(_episode({"tvdb": "81472"}, 2, 13))
    assert after is not None
    assert (after.absolute, after.namespace, after.target_id) == (512, "mal", "999")
    assert after.basis == "user_override"
    assert after.entry.startswith("override:ovr_")


def test_disabled_override_falls_back_to_the_dataset(index: Path) -> None:
    saved = ov.upsert_override(_rule())
    ov.upsert_override({**saved, "enabled": False})
    got = resolve_absolute(_episode({"tvdb": "81472"}, 2, 13))
    assert got is not None and got.absolute == 52


def test_override_only_applies_inside_its_episode_range(index: Path) -> None:
    ov.upsert_override(_rule(episode_from=10, episode_to=12, episode_start_at=900))
    assert resolve_absolute(_episode({"tvdb": "81472"}, 2, 11)).absolute == 901
    assert resolve_absolute(_episode({"tvdb": "81472"}, 2, 13)).absolute == 52


def test_override_only_applies_to_its_season(index: Path) -> None:
    ov.upsert_override(_rule(match_season=9))
    got = resolve_absolute(_episode({"tvdb": "81472"}, 2, 13))
    assert got is not None and got.absolute == 52


def test_open_ended_override_has_no_upper_bound(index: Path) -> None:
    ov.upsert_override(_rule(episode_from=5, episode_to=None, episode_start_at=100))
    assert resolve_absolute(_episode({"tvdb": "81472"}, 2, 5)).absolute == 100
    assert resolve_absolute(_episode({"tvdb": "81472"}, 2, 30)).absolute == 125
    assert resolve_absolute(_episode({"tvdb": "81472"}, 2, 4)).absolute == 43


def test_override_works_where_the_dataset_has_nothing(index: Path) -> None:
    assert resolve_absolute(_episode({"tvdb": "404404"}, 1, 1)) is None
    ov.upsert_override(_rule(match_id="404404", match_season=1, episode_from=1, episode_to=None, episode_start_at=1))
    got = resolve_absolute(_episode({"tvdb": "404404"}, 1, 7))
    assert got is not None and got.absolute == 7


def test_override_can_target_simkl_directly(index: Path) -> None:
    ov.upsert_override(_rule(target_namespace="simkl", target_id="41487"))
    got = resolve_absolute(_episode({"tvdb": "81472"}, 2, 1))
    assert got is not None
    assert (got.namespace, got.target_id, got.absolute) == ("simkl", "41487", 500)


def test_override_applies_to_specials_without_opt_in(index: Path) -> None:
    ov.upsert_override(_rule(match_season=0, episode_from=1, episode_to=None, episode_start_at=1))
    got = resolve_absolute(_episode({"tvdb": "81472"}, 0, 3))
    assert got is not None and got.absolute == 3


def test_unrelated_shows_are_untouched(index: Path) -> None:
    ov.upsert_override(_rule())
    got = resolve_absolute(_episode({"tvdb": "600"}, 1, 4))
    assert got is not None
    assert got.basis == "anibridge_absolute"
    assert got.absolute == 4


# --- identity overrides -------------------------------------------------------


def test_identity_override_wins_in_enrich_ids(index: Path) -> None:
    svc = AnimeMappingService({"anime_mapping": {"enabled": True, "release_tag": "v3"}})
    plain = svc.enrich_ids({"tvdb": "81472"}, media_type="show")
    assert plain["ids"].get("mal") == "813"

    ov.upsert_override(
        {
            "media_type": "show",
            "match_provider": "tvdb",
            "match_id": "81472",
            "target_namespace": "mal",
            "target_id": "777",
        }
    )
    ruled = svc.enrich_ids({"tvdb": "81472"}, media_type="show")
    assert ruled["ids"]["mal"] == "777"
    assert ruled["changed"] is True
    assert ruled["detail"]["anime_mapping"]["overrides"] == {"mal": "777"}


def test_identity_override_applies_without_an_index(config_base: Path) -> None:
    svc = AnimeMappingService({"anime_mapping": {"enabled": True, "release_tag": "v3"}})
    assert svc.ready() is False
    ov.upsert_override(
        {
            "media_type": "movie",
            "match_provider": "tmdb",
            "match_id": "810693",
            "target_namespace": "anilist",
            "target_id": "131573",
        }
    )
    got = svc.enrich_ids({"tmdb": "810693"}, media_type="movie")
    assert got["ids"]["anilist"] == "131573"
    assert got["changed"] is True


def test_identity_override_respects_media_type(config_base: Path) -> None:
    ov.upsert_override(
        {
            "media_type": "movie",
            "match_provider": "tmdb",
            "match_id": "12971",
            "target_namespace": "mal",
            "target_id": "1",
        }
    )
    assert ov.find_identity_overrides({"tmdb": "12971"}, media_type="movie") == {"mal": "1"}
    assert ov.find_identity_overrides({"tmdb": "12971"}, media_type="show") == {}


def test_stats_reports_the_store(config_base: Path) -> None:
    ov.upsert_override(_rule())
    ov.upsert_override({"media_type": "movie", "match_provider": "tmdb", "match_id": "1", "target_namespace": "mal", "target_id": "2"})
    disabled = ov.upsert_override(_rule(match_id="55"))
    ov.upsert_override({**disabled, "enabled": False})

    got = ov.stats()
    assert got["total"] == 3
    assert got["enabled"] == 2
    assert got["shows"] == 1
    assert got["movies"] == 1
    assert got["episode_rules"] == 1


def test_anidb_keyed_override_applies_to_an_anidb_scraped_library(index: Path) -> None:
    ov.upsert_override(
        _rule(
            title="Mairimashita! Iruma-kun",
            match_provider="anidb",
            match_id="16627",
            match_season=1,
            target_namespace="simkl",
            target_id="1728821",
            episode_from=1,
            episode_to=21,
            episode_start_at=1,
        )
    )

    got = resolve_absolute(_episode({"tvdb": "369144", "anidb": "16627"}, 1, 14))

    assert got is not None
    assert (got.absolute, got.namespace, got.target_id, got.basis) == (14, "simkl", "1728821", "user_override")


OVERGEARED: dict[str, Any] = {"anidb:20077:R": {"mal:64340": {"1-12": "1-12"}}}


def _overgeared_rule(namespace: str, target_id: str) -> dict[str, Any]:
    return _rule(match_provider="tmdb", match_id="324502", match_season=1, target_namespace=namespace,
                 target_id=target_id, episode_from=1, episode_to=12, episode_start_at=1)


@pytest.fixture()
def overgeared(config_base: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    from providers.sync.anilist import _progress as progress_mod

    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(OVERGEARED), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")

    class _Svc:
        def __init__(self, cfg: Any = None) -> None:
            pass

        def ready(self) -> bool:
            return True

        def enrich_ids(self, ids: Any, *, media_type: Any = None) -> dict[str, Any]:
            return {"ids": dict(ids)}

    monkeypatch.setattr(progress_mod, "AnimeMappingService", _Svc)
    return {"anime_mapping": {"enabled": True}}


def test_find_episode_overrides_returns_every_rule_preferred_first(config_base: Path) -> None:
    ov.upsert_override(_overgeared_rule("anidb", "20077"))
    ov.upsert_override(_overgeared_rule("anilist", "212888"))
    ov.upsert_override(_overgeared_rule("mal", "64340"))
    ids = {"tmdb": "324502"}

    assert [hit.namespace for hit in ov.find_episode_overrides(ids, 1, 3)] == ["anidb", "anilist", "mal"]
    assert [hit.namespace for hit in ov.find_episode_overrides(ids, 1, 3, prefer="anilist")] == ["anilist", "anidb", "mal"]
    first = ov.find_episode_override(ids, 1, 3)
    assert first is not None and first.namespace == "anidb"
    assert ov.find_episode_overrides(ids, 1, 13) == []


@pytest.mark.parametrize("order", [("anidb", "anilist", "mal", "kitsu"), ("kitsu", "mal", "anilist", "anidb")])
def test_each_anime_target_uses_its_own_rule(overgeared: dict[str, Any], order: tuple[str, ...]) -> None:
    from providers.sync.anilist._progress import resolve_target as anilist_target
    from providers.sync.kitsu._common import resolve_target as kitsu_target
    from providers.sync.myanimelist._common import resolve_target as mal_target

    targets = {"anidb": "20077", "anilist": "212888", "mal": "64340", "kitsu": "49999"}
    for namespace in order:
        ov.upsert_override(_overgeared_rule(namespace, targets[namespace]))
    item = _episode({"tmdb": "324502"}, 1, 3)

    assert anilist_target(overgeared, item) == (212888, 3)
    assert mal_target(overgeared, item) == ("64340", 3)
    assert kitsu_target(overgeared, item) == ("49999", 3)


def test_anime_target_falls_back_to_a_rule_that_reaches_it(overgeared: dict[str, Any]) -> None:
    from providers.sync.anilist._progress import resolve_target as anilist_target
    from providers.sync.myanimelist._common import resolve_target as mal_target

    ov.upsert_override(_overgeared_rule("anilist", "212888"))
    ov.upsert_override(_overgeared_rule("anidb", "20077"))
    item = _episode({"tmdb": "324502"}, 1, 3)

    assert anilist_target(overgeared, item) == (212888, 3)
    assert mal_target(overgeared, item) == ("64340", 3)


def test_explicit_target_rule_wins_over_a_higher_rule_for_another_show(overgeared: dict[str, Any]) -> None:
    from providers.sync.myanimelist._common import resolve_target as mal_target

    ov.upsert_override(_overgeared_rule("anidb", "20077"))
    item = _episode({"tmdb": "324502"}, 1, 3)
    assert mal_target(overgeared, item) == ("64340", 3)

    ov.upsert_override(_overgeared_rule("mal", "777"))
    assert mal_target(overgeared, item) == ("777", 3)


def test_top_rule_wins_among_rules_for_the_same_target(overgeared: dict[str, Any]) -> None:
    from providers.sync.anilist._progress import resolve_target as anilist_target

    ov.upsert_override(_overgeared_rule("anilist", "212888"))
    ov.upsert_override(_overgeared_rule("anilist", "999"))

    assert anilist_target(overgeared, _episode({"tmdb": "324502"}, 1, 3)) == (212888, 3)
