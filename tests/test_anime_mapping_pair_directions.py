# CrossWatch test scripts
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import storage
from cw_platform.anime_mapping import overrides as ovr
from cw_platform.anime_mapping.service import enrich_index_for_pair
from cw_platform.id_map import canonical_key

MAL = "52991"
ANIDB = "17617"
ANILIST = "154587"
TMDB = "209867"
TVDB = "424536"
IMDB = "tt22248376"
SIMKL = "1912385"
KITSU = "46654"

ORPHAN_SIMKL = "9999999"

MAPPINGS: dict[str, Any] = {
    f"mal:{MAL}": {
        f"tmdb_show:{TMDB}": {},
        f"tvdb_show:{TVDB}": {},
        f"imdb_show:{IMDB}": {},
        f"anidb:{ANIDB}": {},
        f"anilist:{ANILIST}": {},
    },
}

IDENTITY_TSV = "\n".join(
    [
        "\t".join(["title", "anidb", "anilist", "kitsu", "myanimelist", "simkl", "themoviedb", "thetvdb"]),
        "\t".join(["Frieren", ANIDB, ANILIST, KITSU, MAL, SIMKL, TMDB, TVDB]),
    ]
)

CFG = {"anime_mapping": {"enabled": True, "release_tag": "v3"}}


@pytest.fixture()
def index(config_base: Path) -> Path:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    paths["identity"].write_text(IDENTITY_TSV, encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    ovr.invalidate_cache()
    return paths["db"]


@pytest.fixture()
def custom_mapping(index: Path) -> None:
    ovr.upsert_override(
        {
            "media_type": "show",
            "title": "Orphan anime",
            "match_provider": "simkl",
            "match_id": ORPHAN_SIMKL,
            "target_namespace": "mal",
            "target_id": MAL,
        }
    )
    ovr.invalidate_cache()


def _enrich(item: dict[str, Any], *providers: str) -> dict[str, Any]:
    out = enrich_index_for_pair({"k": item}, CFG, *providers)
    assert len(out) == 1
    return next(iter(out.values()))


def _plex_show() -> dict[str, Any]:
    return {"type": "show", "title": "Frieren", "ids": {"tmdb": TMDB, "tvdb": TVDB}}


def _mdblist_show() -> dict[str, Any]:
    return {"type": "show", "title": "Frieren", "ids": {"tmdb": TMDB, "imdb": IMDB}}


def _simkl_anime() -> dict[str, Any]:
    return {"type": "show", "title": "Frieren", "ids": {"simkl": SIMKL}, "simkl_bucket": "anime"}


# --- 1. MDBList -> SIMKL ------------------------------------------------------


def test_mdblist_to_simkl_resolves_native_ids(index: Path) -> None:
    from providers.sync.simkl._watchlist import _ALLOWED_ID_KEYS, _kind_group

    out = _enrich(_mdblist_show(), "MDBLIST", "SIMKL")
    ids = out["ids"]
    assert ids["mal"] == MAL
    assert ids["anidb"] == ANIDB
    assert ids["anilist"] == ANILIST
    assert [k for k in ids if k in _ALLOWED_ID_KEYS]
    assert _kind_group(out) == "shows"


# --- 2. Plex -> SIMKL ---------------------------------------------------------


def test_plex_to_simkl_resolves_native_ids(index: Path) -> None:
    from providers.sync.simkl._watchlist import _ALLOWED_ID_KEYS, _kind_group

    out = _enrich(_plex_show(), "PLEX", "SIMKL")
    ids = out["ids"]
    assert ids["mal"] == MAL
    assert ids["anidb"] == ANIDB
    assert [k for k in ids if k in _ALLOWED_ID_KEYS]
    assert _kind_group(out) == "shows"


# --- 3. SIMKL -> MDBList ------------------------------------------------------


def test_simkl_to_mdblist_resolves_aired_ids(index: Path) -> None:
    from providers.sync.mdblist._watchlist import _batch_payload

    out = _enrich(_simkl_anime(), "SIMKL", "MDBLIST")
    ids = out["ids"]
    assert ids["tmdb"] == TMDB
    assert ids["imdb"] == IMDB
    assert ids["tvdb"] == TVDB

    accepted, rejected = _batch_payload([out])
    assert not rejected
    assert accepted[0]["type"] == "show"
    assert str(accepted[0]["ids"]["tmdb"]) == TMDB


# --- 4. SIMKL -> Plex ---------------------------------------------------------


def test_simkl_to_plex_resolves_aired_ids(index: Path) -> None:
    from providers.sync.plex._watchlist import _libtype_for_item

    out = _enrich(_simkl_anime(), "SIMKL", "PLEX")
    ids = out["ids"]
    assert ids["tmdb"] == TMDB
    assert ids["tvdb"] == TVDB
    assert ids["imdb"] == IMDB
    assert _libtype_for_item(out) == "show"


# --- both sides land on one key so the diff can match -------------------------


def test_simkl_and_plex_converge_on_one_key(index: Path) -> None:
    simkl_side = enrich_index_for_pair({"simkl:" + SIMKL: _simkl_anime()}, CFG, "SIMKL", "PLEX")
    plex_side = enrich_index_for_pair({"tmdb:" + TMDB: _plex_show()}, CFG, "SIMKL", "PLEX")
    assert set(simkl_side) == set(plex_side)


def test_simkl_and_mdblist_converge_on_one_key(index: Path) -> None:
    simkl_side = enrich_index_for_pair({"simkl:" + SIMKL: _simkl_anime()}, CFG, "SIMKL", "MDBLIST")
    mdblist_side = enrich_index_for_pair({"tmdb:" + TMDB: _mdblist_show()}, CFG, "SIMKL", "MDBLIST")
    assert set(simkl_side) == set(mdblist_side)


# --- custom mappings ----------------------------------------------------------


def test_custom_mapping_is_applied(index: Path, custom_mapping: None) -> None:
    out = _enrich({"type": "show", "ids": {"simkl": ORPHAN_SIMKL}}, "SIMKL", "PLEX")
    assert out["ids"]["mal"] == MAL


def test_custom_mapping_reaches_aired_ids_for_plex(index: Path, custom_mapping: None) -> None:
    out = _enrich({"type": "show", "ids": {"simkl": ORPHAN_SIMKL}}, "SIMKL", "PLEX")
    assert out["ids"].get("tmdb") == TMDB


def test_custom_mapping_reaches_aired_ids_for_mdblist(index: Path, custom_mapping: None) -> None:
    from providers.sync.mdblist._watchlist import _batch_payload

    out = _enrich({"type": "show", "ids": {"simkl": ORPHAN_SIMKL}}, "SIMKL", "MDBLIST")
    accepted, rejected = _batch_payload([out])
    assert not rejected, rejected
    assert str(accepted[0]["ids"]["tmdb"]) == TMDB


def test_custom_mapping_converges_with_plex_key(index: Path, custom_mapping: None) -> None:
    orphan = enrich_index_for_pair({"o": {"type": "show", "ids": {"simkl": ORPHAN_SIMKL}}}, CFG, "SIMKL", "PLEX")
    plex_side = enrich_index_for_pair({"p": _plex_show()}, CFG, "SIMKL", "PLEX")
    assert set(orphan) == set(plex_side)
    assert canonical_key(next(iter(orphan.values()))) == f"tmdb:{TMDB}"


def test_custom_mapping_outranks_the_dataset(index: Path) -> None:
    ovr.upsert_override(
        {
            "media_type": "show",
            "match_provider": "tmdb",
            "match_id": TMDB,
            "target_namespace": "mal",
            "target_id": "111111",
        }
    )
    ovr.invalidate_cache()
    out = _enrich(_plex_show(), "PLEX", "SIMKL")
    assert out["ids"]["mal"] == "111111"


def test_custom_mapping_seeds_do_not_leak_conflicting_graph_ids(index: Path) -> None:
    ovr.upsert_override(
        {
            "media_type": "show",
            "match_provider": "simkl",
            "match_id": ORPHAN_SIMKL,
            "target_namespace": "anidb",
            "target_id": ANIDB,
        }
    )
    ovr.invalidate_cache()
    out = _enrich({"type": "show", "ids": {"simkl": ORPHAN_SIMKL}}, "SIMKL", "PLEX")
    assert out["ids"]["anidb"] == ANIDB
    assert out["ids"]["tmdb"] == TMDB
    assert out["ids"]["simkl"] == ORPHAN_SIMKL


# --- AniList -> other providers -----------------------------------------------


def _anilist_item(with_mal: bool = True) -> dict[str, Any]:
    ids: dict[str, Any] = {"anilist": int(ANILIST)}
    if with_mal:
        ids["mal"] = int(MAL)
    return {"type": "anime", "title": "Frieren", "year": 2023, "ids": ids}


def test_anilist_to_plex_resolves_aired_ids(index: Path) -> None:
    from providers.sync.plex._watchlist import _libtype_for_item

    out = _enrich(_anilist_item(), "ANILIST", "PLEX")
    ids = out["ids"]
    assert ids["tmdb"] == TMDB
    assert ids["tvdb"] == TVDB
    assert ids["imdb"] == IMDB
    assert _libtype_for_item(out) == "show"


def test_anilist_to_mdblist_is_accepted(index: Path) -> None:
    from providers.sync.mdblist._watchlist import _batch_payload

    out = _enrich(_anilist_item(), "ANILIST", "MDBLIST")
    accepted, rejected = _batch_payload([out])
    assert not rejected, rejected
    assert accepted[0]["type"] == "show"
    assert str(accepted[0]["ids"]["tmdb"]) == TMDB


def test_anilist_to_simkl_is_accepted(index: Path) -> None:
    from providers.sync.simkl._watchlist import _ALLOWED_ID_KEYS, _kind_group

    out = _enrich(_anilist_item(), "ANILIST", "SIMKL")
    assert [k for k in out["ids"] if k in _ALLOWED_ID_KEYS]
    assert _kind_group(out) == "shows"


def test_anilist_without_mal_still_resolves(index: Path) -> None:
    out = _enrich(_anilist_item(with_mal=False), "ANILIST", "PLEX")
    assert out["ids"]["mal"] == MAL
    assert out["ids"]["tmdb"] == TMDB


def test_anilist_anime_type_is_normalised(index: Path) -> None:
    out = _enrich(_anilist_item(), "ANILIST", "PLEX")
    assert out["type"] == "show"


def test_anilist_and_plex_converge_on_one_key(index: Path) -> None:
    a = enrich_index_for_pair({"anilist:" + ANILIST: _anilist_item()}, CFG, "ANILIST", "PLEX")
    p = enrich_index_for_pair({"tmdb:" + TMDB: _plex_show()}, CFG, "ANILIST", "PLEX")
    assert set(a) == set(p)


def test_plex_to_anilist_resolves_anilist_id(index: Path) -> None:
    out = _enrich(_plex_show(), "PLEX", "ANILIST")
    assert out["ids"]["anilist"] == ANILIST
    assert out["ids"]["mal"] == MAL
