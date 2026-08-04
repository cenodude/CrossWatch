from __future__ import annotations

from cw_platform.local_db import watchlist_hide
from cw_platform.orchestrator._state_store import StateStore
from services import watchlist


def test_build_watchlist_filters_hidden_keys_from_database(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(watchlist, "CONFIG", tmp_path)
    monkeypatch.setattr(watchlist, "_registry_sync_providers", lambda: ["TRAKT"])
    watchlist_hide.save_hidden(tmp_path, ["movie:hidden"])
    state = {
        "providers": {
            "TRAKT": {
                "watchlist": {
                    "baseline": {
                        "items": {
                            "movie:hidden": {"type": "movie", "title": "Hidden"},
                            "movie:visible": {"type": "movie", "title": "Visible"},
                        }
                    }
                }
            }
        }
    }

    rows = watchlist.build_watchlist(state, tmdb_ok=False)

    assert [row["key"] for row in rows] == ["movie:visible"]


def test_state_store_clears_watchlist_hidden_database(tmp_path) -> None:
    watchlist_hide.save_hidden(tmp_path, ["movie:hidden"])

    StateStore(tmp_path).clear_watchlist_hide()

    assert watchlist_hide.load_hidden(tmp_path) == set()
