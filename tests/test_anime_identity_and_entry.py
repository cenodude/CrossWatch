# CrossWatch test scripts
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import storage
from cw_platform.anime_mapping.episodes import Resolution, resolve_absolute

# A long runner: enough seasons that the whole show exceeds any per-source row cap.
_LONG_RUN_SEASONS = 400


def _long_runner() -> dict[str, Any]:
    out: dict[str, Any] = {}
    for season in range(1, _LONG_RUN_SEASONS + 1):
        lo = (season - 1) * 10 + 1
        hi = lo + 9
        out[f"tvdb_show:900:s{season}"] = {
            "anidb:900:R": {"1-10": f"{lo}-{hi}"},
            "mal:900": {"1-10": f"{lo}-{hi}"},
            "anilist:900": {"1-10": f"{lo}-{hi}"},
        }
    # A native entry with a flat absolute run, as anime-agent libraries report it.
    out["mal:700"] = {"anilist:700": {"1-120": "1-120"}}
    out["anidb:701:R"] = {"mal:701": {"1-60": "1-60"}}
    out["anidb:702:S"] = {"mal:702": {"1-4": "1-4"}}
    return out


MAPPINGS = _long_runner()

IDENTITY_TSV = "\n".join(
    [
        "\t".join(["title", "anidb", "anilist", "kitsu", "myanimelist", "simkl", "themoviedb", "thetvdb"]),
        "\t".join(["Clean", "1530", "813", "720", "813", "41487", "12971", "81472"]),
        "\t".join(["KitsuOnly", "2369", "269", "244", "269", "", "30984", "74796"]),
        "\t".join(["SimklOnly", "69", "21", "", "21", "38636", "37854", "81797"]),
        # Same MAL id on two rows with different SIMKL ids -> must be dropped as ambiguous.
        "\t".join(["DupA", "111", "", "", "555", "9001", "", ""]),
        "\t".join(["DupB", "222", "", "", "555", "9002", "", ""]),
        # No simkl and no kitsu -> contributes nothing.
        "\t".join(["Empty", "333", "444", "", "666", "", "", ""]),
    ]
)


@pytest.fixture()
def index(config_base: Path) -> Path:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    paths["identity"].write_text(IDENTITY_TSV, encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    return paths["db"]


def _episode(show_ids: dict[str, str], season: int, episode: int) -> dict[str, Any]:
    return {"type": "episode", "show_ids": show_ids, "season": season, "episode": episode}


# --- scoped edge queries (the LIMIT 500 truncation) ---------------------------


def test_late_season_of_a_long_runner_resolves(index: Path) -> None:
    got = resolve_absolute(_episode({"tvdb": "900"}, _LONG_RUN_SEASONS, 1))
    assert isinstance(got, Resolution)
    assert got.absolute == (_LONG_RUN_SEASONS - 1) * 10 + 1
    assert got.entry == "tvdb_direct"


def test_every_season_of_a_long_runner_resolves(index: Path) -> None:
    for season in (1, 2, 51, 200, 399, _LONG_RUN_SEASONS):
        got = resolve_absolute(_episode({"tvdb": "900"}, season, 4))
        assert got is not None, f"season {season} did not resolve"
        assert got.absolute == (season - 1) * 10 + 4


def test_query_edges_scope_filter_returns_only_that_season(index: Path) -> None:
    scoped = storage.query_edges("v3", "tvdb", "900", scope="s377")
    assert scoped
    assert {row["source_scope"] for row in scoped} == {"s377"}
    unscoped = storage.query_edges("v3", "tvdb", "900")
    assert len(unscoped) > len(scoped)


# --- native entry points (R3) -------------------------------------------------


def test_native_mal_only_source_resolves_by_passthrough(index: Path) -> None:
    got = resolve_absolute(_episode({"mal": "700"}, 1, 42))
    assert got is not None
    assert (got.absolute, got.namespace, got.target_id) == (42, "mal", "700")
    assert got.basis == "anibridge_native"
    assert got.entry == "mal_native"


def test_native_anidb_only_source_resolves(index: Path) -> None:
    got = resolve_absolute(_episode({"anidb": "701"}, 1, 7))
    assert got is not None
    assert (got.absolute, got.namespace) == (7, "anidb")


def test_native_passthrough_refuses_out_of_range_episode(index: Path) -> None:
    assert resolve_absolute(_episode({"mal": "700"}, 1, 121)) is None


def test_native_passthrough_refuses_non_first_season(index: Path) -> None:
    assert resolve_absolute(_episode({"mal": "700"}, 2, 5)) is None


def test_native_passthrough_ignores_anidb_specials_scope(index: Path) -> None:
    assert resolve_absolute(_episode({"anidb": "702"}, 1, 2)) is None


def test_native_passthrough_refuses_unknown_id(index: Path) -> None:
    assert resolve_absolute(_episode({"mal": "404404"}, 1, 1)) is None


def test_aired_entry_still_wins_over_native_passthrough(index: Path) -> None:
    got = resolve_absolute(_episode({"tvdb": "900", "mal": "900"}, 3, 2))
    assert got is not None
    assert got.entry == "tvdb_direct"
    assert got.absolute == 22


# --- identity table (R2) ------------------------------------------------------


def test_identity_lookup_returns_simkl_and_kitsu(index: Path) -> None:
    assert storage.query_native_identity("v3", "anidb", "1530") == {"simkl": "41487", "kitsu": "720"}
    assert storage.query_native_identity("v3", "mal", "813") == {"simkl": "41487", "kitsu": "720"}
    assert storage.query_native_identity("v3", "anilist", "813") == {"simkl": "41487", "kitsu": "720"}


def test_identity_lookup_handles_partial_rows(index: Path) -> None:
    assert storage.query_native_identity("v3", "anidb", "2369") == {"kitsu": "244"}
    assert storage.query_native_identity("v3", "anidb", "69") == {"simkl": "38636"}


def test_identity_drops_ambiguous_native_ids(index: Path) -> None:
    assert storage.query_native_identity("v3", "mal", "555") == {}
    # The unambiguous anidb keys from those same rows are still usable.
    assert storage.query_native_identity("v3", "anidb", "111") == {"simkl": "9001"}


def test_identity_ignores_rows_without_simkl_or_kitsu(index: Path) -> None:
    assert storage.query_native_identity("v3", "anidb", "333") == {}
    assert storage.query_native_identity("v3", "mal", "666") == {}


def test_identity_rejects_unknown_namespace(index: Path) -> None:
    assert storage.query_native_identity("v3", "tvdb", "81472") == {}
    assert storage.query_native_identity("v3", "", "1530") == {}


def test_identity_is_rebuilt_with_the_mapping_index(index: Path) -> None:
    # 3 namespaces x 3 usable rows, plus the two unambiguous anidb keys from the
    # duplicate pair whose shared mal id is dropped.
    res = storage.rebuild_sqlite_from_mappings(release_tag="v3")
    assert res["identity_count"] == 11
    assert storage.read_state("v3")["identity_count"] == 11


def test_rebuild_succeeds_without_an_identity_file(config_base: Path) -> None:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    res = storage.rebuild_sqlite_from_mappings(release_tag="v3")
    assert res["identity_count"] == 0
    assert storage.query_native_identity("v3", "anidb", "1530") == {}


# --- reader / path caching ----------------------------------------------------


def test_rebuild_invalidates_the_cached_reader(index: Path) -> None:
    assert storage.query_native_identity("v3", "anidb", "1530") == {"simkl": "41487", "kitsu": "720"}

    paths = storage.paths("v3")
    trimmed = "\n".join(
        [
            "\t".join(["title", "anidb", "anilist", "kitsu", "myanimelist", "simkl", "themoviedb", "thetvdb"]),
            "\t".join(["Changed", "1530", "813", "111", "813", "222", "12971", "81472"]),
        ]
    )
    paths["identity"].write_text(trimmed, encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")

    assert storage.query_native_identity("v3", "anidb", "1530") == {"simkl": "222", "kitsu": "111"}
    assert storage.query_native_identity("v3", "anidb", "69") == {}


def test_reader_survives_repeated_queries(index: Path) -> None:
    for _ in range(50):
        assert storage.query_edges("v3", "tvdb", "900", scope="s5")
        assert storage.query_native_identity("v3", "anidb", "1530")


def test_paths_cache_follows_config_base(config_base: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    first = storage.paths("v3")["db"]
    assert str(config_base) in str(first)

    other = tmp_path / "elsewhere"
    other.mkdir()
    monkeypatch.setenv("CONFIG_BASE", str(other))
    second = storage.paths("v3")["db"]
    assert str(other) in str(second)
    assert first != second


def test_release_tag_stays_inside_the_cache_root(config_base: Path) -> None:
    root = storage._base_root()
    for evil in ["../../../etc", "v3/../../..", "a" * 200, "", None, "v3;rm -rf /"]:
        db = storage.paths(evil)["db"]
        db.relative_to(root)


def test_malformed_identity_file_does_not_break_the_rebuild(config_base: Path) -> None:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    paths["identity"].write_text("not\ta\tvalid\theader", encoding="utf-8")
    res = storage.rebuild_sqlite_from_mappings(release_tag="v3")
    assert res["identity_count"] == 0
    assert res["edge_count"] > 0
