# CrossWatch test scripts
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import simkl_catalog as sc

DANDADAN_SEARCH: list[dict[str, Any]] = [
    {
        "endpoint_type": "anime",
        "ids": {"simkl_id": 2308514, "slug": "dan-da-dan", "tmdb": "240411"},
        "title": "Dan Da Dan",
        "title_romaji": "Dan Da Dan",
        "type": "tv",
        "year": 2024,
    },
    {
        "endpoint_type": "anime",
        "ids": {"simkl_id": 2665178, "slug": "dan-da-dan", "tmdb": "299464"},
        "title": "Dan Da Dan",
        "title_en": "Dan Da Dan Season 2",
        "type": "tv",
        "year": 2025,
    },
    {
        "endpoint_type": "anime",
        "ids": {"simkl_id": 2884140, "slug": "dan-da-dan"},
        "title": "Dan Da Dan",
        "title_en": "Dan Da Dan Season 3",
        "type": "tv",
    },
    {
        "endpoint_type": "anime",
        "ids": {"simkl_id": 2833324, "slug": "recap"},
        "title": "Dan Da Dan Recap",
        "type": "movie",
        "year": 2025,
    },
]

DETAILS: dict[str, dict[str, Any]] = {
    "2308514": {
        "ids": {"simkl": 2308514, "tmdb": "240411"},
        "title": "Dan Da Dan",
        "year": 2024,
        "anime_type": "tv",
        "total_episodes": 12,
        "relations": [
            {"anime_type": "tv", "ids": {"simkl": 2665178}, "relation_type": "sequel", "title": "Dan Da Dan", "year": 2025},
            {"anime_type": "tv", "ids": {"simkl": 2884140}, "relation_type": "sequel", "title": "Dan Da Dan", "year": None},
            {"anime_type": "movie", "ids": {"simkl": 999999}, "relation_type": "sequel", "title": "A movie", "year": 2025},
            {"anime_type": "tv", "ids": {"simkl": 111111}, "relation_type": "prequel", "title": "A prequel", "year": 2010},
        ],
    },
    "2665178": {
        "ids": {"simkl": 2665178, "tmdb": "299464"},
        "title": "Dan Da Dan",
        "en_title": "Dan Da Dan Season 2",
        "year": 2025,
        "anime_type": "tv",
        "total_episodes": 12,
        "relations": [],
    },
    "2884140": {
        "ids": {"simkl": 2884140},
        "title": "Dan Da Dan",
        "en_title": "Dan Da Dan Season 3",
        "year": None,
        "anime_type": "tv",
        "total_episodes": None,
        "relations": [],
    },
}

REZERO_DETAILS: dict[str, dict[str, Any]] = {
    "509292": {
        "ids": {"simkl": 509292, "tmdb": "65942"},
        "title": "Re:Zero kara Hajimeru Isekai Seikatsu",
        "en_title": "Re:Zero - Starting Life in Another World",
        "year": 2016,
        "anime_type": "tv",
        "total_episodes": 25,
        "relations": [
            {"anime_type": "tv", "ids": {"simkl": 2125704}, "relation_type": "sequel", "year": 2024},
            {"anime_type": "tv", "ids": {"simkl": 1063491}, "relation_type": "sequel", "year": 2020},
            {"anime_type": "tv", "ids": {"simkl": 2743422}, "relation_type": "sequel", "year": 2026},
            {"anime_type": "tv", "ids": {"simkl": 1367345}, "relation_type": "sequel", "year": 2021},
        ],
    },
    "1063491": {"ids": {"simkl": 1063491}, "title": "Re:Zero", "en_title": "S2", "year": 2020, "anime_type": "tv", "total_episodes": 13, "relations": []},
    "1367345": {"ids": {"simkl": 1367345}, "title": "Re:Zero", "en_title": "Part 2", "year": 2021, "anime_type": "tv", "total_episodes": 12, "relations": []},
    "2125704": {"ids": {"simkl": 2125704}, "title": "Re:Zero", "en_title": "S3", "year": 2024, "anime_type": "tv", "total_episodes": 16, "relations": []},
    "2743422": {"ids": {"simkl": 2743422, "tmdb": "328061"}, "title": "Re:Zero", "en_title": "S4", "year": 2026, "anime_type": "tv", "total_episodes": 11, "relations": []},
}


@pytest.fixture()
def simkl(monkeypatch, config_base: Path):
    calls: list[tuple[str, dict[str, Any]]] = []
    details: dict[str, dict[str, Any]] = dict(DETAILS)
    search_rows: list[dict[str, Any]] = list(DANDADAN_SEARCH)

    def fake_request(path: str, **params: Any) -> Any:
        calls.append((path, dict(params)))
        if path == sc.SEARCH_PATH:
            return list(search_rows)
        if path.startswith(f"{sc.DETAIL_PATH}/"):
            ident = path.rsplit("/", 1)[-1]
            if ident not in details:
                raise sc.SimklCatalogError("SIMKL returned 404")
            return dict(details[ident])
        raise AssertionError(f"unexpected path {path}")

    monkeypatch.setattr(sc, "client_id", lambda: "test-client-id")
    monkeypatch.setattr(sc, "_request", fake_request)
    return {"calls": calls, "details": details, "search_rows": search_rows}


def test_search_keeps_split_seasons_and_drops_non_tv(simkl) -> None:
    results = sc.search("Dan Da Dan")

    assert [r.simkl for r in results] == ["2308514", "2665178", "2884140"]
    assert results[1].label == "Dan Da Dan Season 2"
    assert results[1].tmdb == "299464"
    assert results[2].tmdb == ""
    assert simkl["calls"][0][1][sc.SEARCH_PARAM] == "Dan Da Dan"


def test_search_falls_back_to_title_when_there_is_no_english_label(simkl) -> None:
    results = sc.search("Dan Da Dan")
    assert results[0].label == "Dan Da Dan"


def test_season_chain_drops_unaired_entries(simkl) -> None:
    chain = sc.season_chain("2308514")

    assert [e.simkl for e in chain.entries] == ["2308514", "2665178"]
    assert [e.total_episodes for e in chain.entries] == [12, 12]
    assert chain.skipped and chain.skipped[0]["simkl"] == "2884140"


def test_plan_builds_contiguous_ranges_for_dandadan(simkl) -> None:
    plan = sc.plan_rules("2308514", match_provider="tmdb", match_id="240411", match_season=1)

    ranges = [(r["episode_from"], r["episode_to"], r["target_id"], r["episode_start_at"]) for r in plan["rules"]]
    assert ranges == [(1, 12, "2308514", 1), (13, 24, "2665178", 1)]
    assert plan["total_episodes"] == 24
    assert all(r["match_provider"] == "tmdb" and r["match_id"] == "240411" for r in plan["rules"])
    assert all(r["match_season"] == 1 and r["target_namespace"] == "simkl" for r in plan["rules"])


def test_plan_orders_the_chain_by_year_not_api_order(simkl, monkeypatch) -> None:
    simkl["details"].clear()
    simkl["details"].update(REZERO_DETAILS)

    plan = sc.plan_rules("509292", match_provider="tmdb", match_id="65942", match_season=1)

    ranges = [(r["episode_from"], r["episode_to"], r["target_id"]) for r in plan["rules"]]
    assert ranges == [
        (1, 25, "509292"),
        (26, 38, "1063491"),
        (39, 50, "1367345"),
        (51, 66, "2125704"),
        (67, 77, "2743422"),
    ]
    assert plan["total_episodes"] == 77


def test_plan_rejects_a_bad_provider_or_missing_source_id(simkl) -> None:
    with pytest.raises(sc.SimklCatalogError):
        sc.plan_rules("2308514", match_provider="netflix", match_id="240411")
    with pytest.raises(sc.SimklCatalogError):
        sc.plan_rules("2308514", match_provider="tmdb", match_id="")


def test_plan_survives_a_failing_sequel_lookup(simkl) -> None:
    simkl["details"].pop("2665178")

    plan = sc.plan_rules("2308514", match_provider="tmdb", match_id="240411")

    assert [r["target_id"] for r in plan["rules"]] == ["2308514"]
    assert any(row["reason"] == "lookup failed" for row in plan["skipped"])


def test_detail_rejects_a_non_numeric_id(simkl) -> None:
    with pytest.raises(sc.SimklCatalogError):
        sc.detail("../../etc/passwd")


def test_lookups_require_a_configured_client_id(monkeypatch, config_base: Path) -> None:
    monkeypatch.setattr(sc, "client_id", lambda instance_id=None: "")
    assert sc.configured() is False
    with pytest.raises(sc.SimklNotConfigured):
        sc._request("/search/anime", q="x")


def _with_config(monkeypatch, cfg: dict[str, Any]) -> None:
    monkeypatch.setattr(sc, "load_config", lambda: cfg)


def test_client_id_reads_the_default_profile(monkeypatch, config_base: Path) -> None:
    _with_config(monkeypatch, {"simkl": {"api_key": "base-key"}})

    assert sc.resolve_instance() == ("default", "base-key")
    assert sc.configured() is True
    assert sc.instances() == ["default"]


def test_client_id_falls_back_to_a_named_profile(monkeypatch, config_base: Path) -> None:
    _with_config(monkeypatch, {"simkl": {"instances": {"second": {"api_key": "profile-key"}}}})

    assert sc.resolve_instance() == ("second", "profile-key")
    assert sc.configured() is True
    assert sc.instances() == ["second"]


def test_a_named_profile_can_override_the_client_id(monkeypatch, config_base: Path) -> None:
    _with_config(
        monkeypatch,
        {"simkl": {"api_key": "base-key", "instances": {"second": {"api_key": "other-key"}}}},
    )

    assert sc.resolve_instance("second") == ("second", "other-key")
    assert sc.resolve_instance() == ("default", "base-key")
    assert sc.instances() == ["default", "second"]


def test_a_profile_without_its_own_key_inherits_the_base_one(monkeypatch, config_base: Path) -> None:
    _with_config(
        monkeypatch,
        {"simkl": {"api_key": "base-key", "instances": {"second": {"access_token": "t"}}}},
    )

    assert sc.resolve_instance("second") == ("second", "base-key")


def test_no_profile_anywhere_reports_unconfigured(monkeypatch, config_base: Path) -> None:
    _with_config(monkeypatch, {"simkl": {"instances": {"second": {"access_token": "t"}}}})

    assert sc.resolve_instance("second") == ("second", "")
    assert sc.configured() is False
    assert sc.instances() == []
