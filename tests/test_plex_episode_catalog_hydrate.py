from __future__ import annotations

from typing import Any

import pytest

EPISODE_ROW = {
    "type": "episode",
    "ratingKey": "5001",
    "title": "Ep 1",
    "guid": "com.plexapp.agents.hama://tvdb-81797/1/1?lang=en",
    "grandparentGuid": "com.plexapp.agents.hama://tvdb-81797?lang=en",
    "grandparentRatingKey": "900",
    "grandparentTitle": "Some Show",
    "parentIndex": 1,
    "index": 1,
    "librarySectionID": 2,
}


def _patch(monkeypatch, calls: list[str]):
    from providers.sync.plex import _common as c

    def _hydrate(token, rating_key):
        rk = str(rating_key or "")
        calls.append(rk)
        return {"tvdb": "81797"} if rk == "900" else {"tvdb": "999001"}

    monkeypatch.setattr(c, "hydrate_external_ids", _hydrate)
    return c


def test_episode_ids_are_not_hydrated_when_disabled(monkeypatch) -> None:
    calls: list[str] = []
    c = _patch(monkeypatch, calls)

    c.normalize_discover_row(EPISODE_ROW, token="T", hydrate_item_ids=False)

    assert "5001" not in calls, "episode rating key must not be hydrated"


def test_show_ids_are_still_hydrated_when_disabled(monkeypatch) -> None:
    calls: list[str] = []
    c = _patch(monkeypatch, calls)

    out = c.normalize_discover_row(EPISODE_ROW, token="T", hydrate_item_ids=False)

    assert "900" in calls, "grandparent rating key must still be hydrated"
    assert out["show_ids"].get("tvdb") == "81797"


def test_default_still_hydrates_item_ids(monkeypatch) -> None:
    calls: list[str] = []
    c = _patch(monkeypatch, calls)

    c.normalize_discover_row(EPISODE_ROW, token="T")

    assert "5001" in calls


def _catalog_from(rows: list[dict[str, Any]], *, hydrate: bool):
    from providers.sync.plex import _common as c
    from providers.sync.plex._history import HistoryCatalog

    cat = HistoryCatalog()
    for row in rows:
        meta = c.normalize_discover_row(row, token="T", hydrate_item_ids=hydrate) or {}
        ids = dict(meta.get("ids") or {})
        rk = str(ids.get("plex") or row.get("ratingKey") or "").strip()
        cat.add(
            {
                "rk": rk,
                "type": "episode",
                "title": meta.get("title"),
                "series_title": meta.get("series_title"),
                "library_id": meta.get("library_id"),
                "ids": ids,
                "show_ids": dict(meta.get("show_ids") or {}),
                "show_rk": (meta.get("show_ids") or {}).get("plex"),
                "season": meta.get("season"),
                "episode": meta.get("episode"),
            }
        )
    return cat


@pytest.mark.parametrize("count", [1, 5])
def test_episode_index_is_identical_with_and_without_item_hydration(monkeypatch, count) -> None:
    rows = []
    for i in range(1, count + 1):
        row = dict(EPISODE_ROW)
        row["ratingKey"] = str(5000 + i)
        row["index"] = i
        rows.append(row)

    calls_a: list[str] = []
    _patch(monkeypatch, calls_a)
    with_hydrate = _catalog_from(rows, hydrate=True)

    calls_b: list[str] = []
    _patch(monkeypatch, calls_b)
    without = _catalog_from(rows, hydrate=False)

    assert without.episode_index == with_hydrate.episode_index
    assert without.show_tokens == with_hydrate.show_tokens
    assert set(without.by_rk) == set(with_hydrate.by_rk)
    assert len(calls_b) < len(calls_a)
