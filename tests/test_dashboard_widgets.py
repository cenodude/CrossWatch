from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from services import activity, dashboard_widgets


def _loads_body(body: bytes | memoryview[int]) -> Any:
    return json.loads(bytes(body))


FLATTENED_PROVIDERS = (
    "PLEX",
    "JELLYFIN",
    "EMBY",
    "SIMKL",
    "TRAKT",
    "MDBLIST",
    "PUBLICMETADB",
    "CROSSWATCH",
    "ANILIST",
)


class FakeMetadataManager:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def resolve(self, **kwargs):
        self.calls.append(dict(kwargs))
        raw_ids = kwargs.get("ids")
        ids: dict[str, Any] = raw_ids if isinstance(raw_ids, dict) else {}
        if ids.get("title") == "Behind the Attraction":
            return {"ids": {"tmdb": 100, "trakt": ids.get("trakt")}, "title": "Behind the Attraction"}
        if ids.get("title") == "Heat":
            return {"ids": {"tmdb": 949}, "title": "Heat"}
        return {}


def test_latest_ratings_widget_dedupes_and_sorts_provider_state() -> None:
    state = {
        "providers": {
            "PLEX": {
                "ratings": {
                    "baseline": {
                        "items": {
                            "tmdb:10": {
                                "type": "movie",
                                "title": "Arrival",
                                "year": 2016,
                                "ids": {"tmdb": 10},
                                "rating": 8,
                                "rated_at": "2026-01-01T10:00:00Z",
                            },
                        }
                    }
                },
                "instances": {
                    "home": {
                        "ratings": {
                            "baseline": {
                                "items": {
                                    "tmdb:20": {
                                        "type": "show",
                                        "title": "Severance",
                                        "year": 2022,
                                        "ids": {"tmdb": 20},
                                        "rating": 9,
                                        "rated_at": "2026-01-03T10:00:00Z",
                                    }
                                }
                            }
                        }
                    }
                },
            },
            "TRAKT": {
                "ratings": {
                    "baseline": {
                        "items": {
                            "tmdb:10": {
                                "item": {
                                    "type": "movie",
                                    "title": "Arrival",
                                    "year": 2016,
                                    "ids": {"tmdb": 10},
                                },
                                "rating": 9,
                                "rated_at": "2026-01-04T10:00:00Z",
                            },
                        }
                    }
                },
            },
        }
    }

    payload = dashboard_widgets.latest_ratings_widget(state, limit=5)

    assert payload["ok"] is True
    assert [item["title"] for item in payload["items"]] == ["Arrival", "Severance"]
    assert payload["items"][0]["rating"] == 9
    assert {source["provider"] for source in payload["items"][0]["sources"]} == {"PLEX", "TRAKT"}
    assert payload["items"][0]["poster"] == "/art/tmdb/movie/10?kind=backdrop&size=w300"
    assert payload["items"][0]["cover"] == "/art/tmdb/movie/10?size=w342"
    assert payload["items"][1]["poster"] == "/art/tmdb/tv/20?kind=backdrop&size=w300"
    assert payload["items"][1]["cover"] == "/art/tmdb/tv/20?size=w342"


def test_latest_ratings_widget_uses_tracker_items_without_runtime_state() -> None:
    payload = dashboard_widgets.latest_ratings_widget(
        {"providers": {}},
        tracker_items={
            "tmdb:30": {
                "type": "movie",
                "title": "Heat",
                "year": 1995,
                "ids": {"tmdb": 30},
                "rating": 10,
                "rated_at": "2026-01-05T10:00:00Z",
            }
        },
    )

    assert payload["total"] == 1
    assert payload["items"][0]["title"] == "Heat"
    assert payload["items"][0]["sources"] == [{"provider": "CROSSWATCH", "instance": "default"}]


def test_dashboard_widgets_merge_flattened_provider_rows_across_providers(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {"ok": True, "total": 0, "items": []},
    )
    state = {
        "providers": {
            provider: {
                "history": {
                    "baseline": {
                        "items": {
                            f"{provider}:history:100:s3e2@1767229200": {
                                "type": "episode",
                                "series_title": "Behind the Attraction",
                                "year": 2026,
                                "season": 3,
                                "episode": 2,
                                "show_ids": {"tmdb": 100, provider.lower(): f"{provider.lower()}-show"},
                                "ids": {provider.lower(): f"{provider.lower()}-episode"},
                                "watched_at": "2026-01-01T02:00:00Z",
                            }
                        }
                    }
                },
                "ratings": {
                    "baseline": {
                        "items": {
                            f"{provider}:rating:949": {
                                "type": "movie",
                                "title": "Heat",
                                "year": 1995,
                                "ids": {"tmdb": 949, provider.lower(): f"{provider.lower()}-movie"},
                                "rating": 8,
                                "rated_at": "2026-01-01T03:00:00Z",
                            }
                        }
                    }
                },
            }
            for provider in FLATTENED_PROVIDERS
        }
    }

    history = dashboard_widgets.recent_history_widget(state, limit=5)
    ratings = dashboard_widgets.latest_ratings_widget(state, limit=5)

    assert history["total"] == 1
    assert history["items"][0]["title"] == "Behind the Attraction"
    assert history["items"][0]["poster"] == "/art/tmdb/tv/100?kind=still&season=3&episode=2&size=w300&artv=2"
    assert {source["provider"] for source in history["items"][0]["sources"]} == set(FLATTENED_PROVIDERS)
    assert ratings["total"] == 1
    assert ratings["items"][0]["title"] == "Heat"
    assert ratings["items"][0]["poster"] == "/art/tmdb/movie/949?kind=backdrop&size=w300"
    assert ratings["items"][0]["cover"] == "/art/tmdb/movie/949?size=w342"
    assert {source["provider"] for source in ratings["items"][0]["sources"]} == set(FLATTENED_PROVIDERS)


@pytest.mark.parametrize("provider", ["TRAKT", "SIMKL", "MDBLIST"])
def test_latest_ratings_widget_handles_nested_provider_movie_rows(provider: str) -> None:
    payload = dashboard_widgets.latest_ratings_widget(
        {
            "providers": {
                provider: {
                    "ratings": {
                        "baseline": {
                            "items": {
                                f"{provider.lower()}:movie:1": {
                                    "type": "movie",
                                    "movie": {
                                        "title": "Heat",
                                        "year": 1995,
                                        "ids": {"tmdb": 949, provider.lower(): 1},
                                    },
                                    "rating": 9,
                                    "rated_at": "2026-01-01T03:00:00Z",
                                }
                            }
                        }
                    }
                }
            }
        },
        limit=5,
    )

    assert payload["total"] == 1
    assert payload["items"][0]["title"] == "Heat"
    assert payload["items"][0]["year"] == 1995
    assert payload["items"][0]["tmdb"] == 949
    assert payload["items"][0]["poster"] == "/art/tmdb/movie/949?kind=backdrop&size=w300"
    assert payload["items"][0]["cover"] == "/art/tmdb/movie/949?size=w342"


def test_recent_history_widget_includes_latest_state_history(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {"ok": True, "total": 0, "items": []},
    )
    state = {
        "providers": {
            "PLEX": {
                "history": {
                    "baseline": {
                        "items": {
                            "tmdb:100:s3e1@1767225600": {
                                "type": "episode",
                                "title": "After de attractie",
                                "year": 2026,
                                "season": 3,
                                "episode": 1,
                                "ids": {"tmdb": 100},
                                "watched_at": "2026-01-01T01:00:00Z",
                            },
                            "tmdb:100:s3e2@1767229200": {
                                "type": "episode",
                                "title": "After de attractie",
                                "year": 2026,
                                "season": 3,
                                "episode": 2,
                                "ids": {"tmdb": 100},
                                "watched_at": "2026-01-01T02:00:00Z",
                            },
                        }
                    }
                }
            },
            "SIMKL": {
                "history": {
                    "baseline": {
                        "items": {
                            "simkl:100:s3e2@1767229200": {
                                "type": "episode",
                                "title": "After de attractie",
                                "year": 2026,
                                "season": 3,
                                "episode": 2,
                                "ids": {"tmdb": 100, "simkl": 200},
                                "watched_at": "2026-01-01T02:00:00Z",
                            },
                        }
                    }
                }
            },
        }
    }

    payload = dashboard_widgets.recent_history_widget(state, limit=5)

    assert payload["total"] == 2
    assert [item["episode_label"] for item in payload["items"]] == ["S03E02", "S03E01"]
    assert {source["provider"] for source in payload["items"][0]["sources"]} == {"PLEX", "SIMKL"}


def test_recent_history_widget_merges_movie_rows_with_cross_provider_ids(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {"ok": True, "total": 0, "items": []},
    )
    state = {
        "providers": {
            "TRAKT": {
                "history": {
                    "baseline": {
                        "items": {
                            "tmdb:1662317": {
                                "type": "movie",
                                "title": "The Crash",
                                "year": 2026,
                                "ids": {"tmdb": "1662317", "imdb": "tt40792117", "trakt": "1362157"},
                                "watched_at": "2026-08-18T23:07:00Z",
                            }
                        }
                    }
                }
            },
            "STREMIO": {
                "history": {
                    "baseline": {
                        "items": {
                            "imdb:tt40792117": {
                                "type": "movie",
                                "title": "The Crash",
                                "ids": {"imdb": "tt40792117"},
                                "watched_at": "2026-08-18T23:07:00Z",
                            }
                        }
                    }
                }
            },
        }
    }

    payload = dashboard_widgets.recent_history_widget(state, limit=5)

    assert payload["total"] == 1
    row = payload["items"][0]
    assert row["title"] == "The Crash"
    assert row["year"] == 2026
    assert row["tmdb"] == "1662317"
    assert row["ids"]["tmdb"] == "1662317"
    assert row["ids"]["imdb"] == "tt40792117"
    assert {source["provider"] for source in row["sources"]} == {"TRAKT", "STREMIO"}


def test_recent_history_widget_merges_untimed_episode_state_by_show_ids(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {"ok": True, "total": 0, "items": []},
    )
    state = {
        "providers": {
            "TRAKT": {
                "history": {
                    "baseline": {
                        "items": {
                            "tmdb:328735#s01e03": {
                                "type": "episode",
                                "title": "This Is Forever",
                                "series_title": "The Idaho Murders: College Nightmare",
                                "season": 1,
                                "episode": 3,
                                "watched_at": "2026-08-02T01:07:00Z",
                                "ids": {"tmdb": "7535444", "imdb": "tt43681035"},
                                "show_ids": {"tmdb": "328735", "imdb": "tt43680755"},
                            }
                        }
                    }
                }
            },
            "STREMIO": {
                "history": {
                    "baseline": {
                        "items": {
                            "imdb:tt43680755#s01e03": {
                                "type": "episode",
                                "title": "This Is Forever",
                                "series_title": "The Idaho Murders: College Nightmare",
                                "season": 1,
                                "episode": 3,
                                "watched_at": "1970-01-01T00:00:01Z",
                                "_stremio_watched_at_fallback": "unknown_episode_watch_time",
                                "ids": {"imdb": "tt43680755"},
                                "show_ids": {"imdb": "tt43680755"},
                            }
                        }
                    }
                }
            },
        }
    }

    payload = dashboard_widgets.recent_history_widget(state, limit=5)

    assert payload["total"] == 1
    row = payload["items"][0]
    assert row["title"] == "The Idaho Murders: College Nightmare"
    assert row["episode_label"] == "S01E03"
    assert row["tmdb"] == "328735"
    assert row["watched_at"] == 1785632820
    assert {source["provider"] for source in row["sources"]} == {"TRAKT", "STREMIO"}


def test_recent_history_widget_hides_stremio_unknown_episode_watch_time(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {"ok": True, "total": 0, "items": []},
    )
    state = {
        "providers": {
            "STREMIO": {
                "history": {
                    "baseline": {
                        "items": {
                            "imdb:tt43680755#s01e03": {
                                "type": "episode",
                                "series_title": "The Idaho Murders: College Nightmare",
                                "season": 1,
                                "episode": 3,
                                "watched": True,
                                "watched_at": "1970-01-01T00:00:01Z",
                                "_stremio_watched_at_fallback": "unknown_episode_watch_time",
                                "ids": {"imdb": "tt43680755"},
                                "show_ids": {"imdb": "tt43680755"},
                            }
                        }
                    }
                }
            }
        }
    }

    payload = dashboard_widgets.recent_history_widget(state, limit=5)

    assert payload["items"] == []


def test_recent_history_widget_merges_provider_local_episode_ids_and_inherits_art(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {"ok": True, "total": 0, "items": []},
    )
    state = {
        "providers": {
            "SIMKL": {
                "history": {
                    "baseline": {
                        "items": {
                            "simkl:episode:200@1767229200": {
                                "type": "episode",
                                "series_title": "Behind the Attraction",
                                "year": 2026,
                                "season": 3,
                                "episode": 2,
                                "show_ids": {"tmdb": 100, "simkl": 200},
                                "ids": {"simkl": 300},
                                "watched_at": "2026-01-01T02:00:00Z",
                            }
                        }
                    }
                }
            },
            "TRAKT": {
                "history": {
                    "baseline": {
                        "items": {
                            "trakt:episode:456@1767229210": {
                                "type": "episode",
                                "show": {
                                    "title": "Behind the Attraction",
                                    "year": 2026,
                                    "ids": {"trakt": 123},
                                },
                                "episode": {
                                    "season": 3,
                                    "number": 2,
                                    "ids": {"trakt": 456},
                                },
                                "watched_at": "2026-01-01T02:00:10Z",
                            }
                        }
                    }
                }
            },
        }
    }

    payload = dashboard_widgets.recent_history_widget(state, limit=5)

    assert payload["total"] == 1
    assert payload["items"][0]["title"] == "Behind the Attraction"
    assert payload["items"][0]["episode_label"] == "S03E02"
    assert payload["items"][0]["poster"] == "/art/tmdb/tv/100?kind=still&season=3&episode=2&size=w300&artv=2"
    assert {source["provider"] for source in payload["items"][0]["sources"]} == {"SIMKL", "TRAKT"}


def _translated_anime_state() -> dict:
    return {
        "providers": {
            "SIMKL": {
                "instances": {
                    "SIMKL-P01": {
                        "history": {
                            "baseline": {
                                "items": {
                                    "tmdb:12971#s01e291@1767229200": {
                                        "type": "episode",
                                        "series_title": "Dragon Ball Z",
                                        "season": 1,
                                        "episode": 291,
                                        "show_ids": {"tmdb": 12971},
                                        "watched_at": "2026-01-01T02:00:00Z",
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "TRAKT": {
                "instances": {
                    "TRAKT-P01": {
                        "history": {
                            "baseline": {
                                "items": {
                                    "tmdb:12971#s09e01@1767229200": {
                                        "type": "episode",
                                        "series_title": "Dragon Ball Z",
                                        "season": 9,
                                        "episode": 1,
                                        "show_ids": {"tmdb": 12971},
                                        "watched_at": "2026-01-01T02:00:00Z",
                                    }
                                }
                            }
                        }
                    }
                }
            },
        }
    }


def test_recent_history_widget_merges_translated_episode_via_pair_alias(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {"ok": True, "total": 0, "items": []},
    )
    monkeypatch.setattr(
        dashboard_widgets,
        "_history_alias_representatives",
        lambda: {"tmdb:12971#s01e291": "tmdb:12971#s09e01", "tmdb:12971#s09e01": "tmdb:12971#s09e01"},
    )

    payload = dashboard_widgets.recent_history_widget(_translated_anime_state(), limit=5)

    assert payload["total"] == 1
    assert {source["provider"] for source in payload["items"][0]["sources"]} == {"SIMKL", "TRAKT"}


def test_recent_history_widget_keeps_translated_episode_split_without_alias(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {"ok": True, "total": 0, "items": []},
    )
    monkeypatch.setattr(dashboard_widgets, "_history_alias_representatives", dict)

    payload = dashboard_widgets.recent_history_widget(_translated_anime_state(), limit=5)

    assert payload["total"] == 2


def _write_pair_alias(tmp_path, scope, items, name="trakt_history.pair_alias.p1.json"):
    import json

    (tmp_path / name).write_text(json.dumps({"scope": scope, "items": items}), encoding="utf-8")


def _alias_sandbox(monkeypatch, tmp_path, pairs):
    import cw_platform.config_base as config_base
    import services.analyzer as analyzer

    sandbox = tmp_path / ".cw_state"
    sandbox.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(analyzer, "CWS_DIR", sandbox)
    monkeypatch.setattr(config_base, "load_config", lambda: {"pairs": pairs})
    return sandbox


_ALIAS_PAIR = {
    "id": "p1",
    "enabled": True,
    "source": "SIMKL",
    "target": "TRAKT",
    "source_instance": "SIMKL-P01",
    "target_instance": "TRAKT-P01",
    "mode": "one-way",
    "features": {"history": {"enable": True}},
}
_ALIAS_SCOPE = "one-way:SIMKL#SIMKL-P01-TRAKT#TRAKT-P01:p1|SIMKL>TRAKT"


def test_history_alias_representatives_reads_scoped_file(monkeypatch, tmp_path) -> None:
    sandbox = _alias_sandbox(monkeypatch, tmp_path, [_ALIAS_PAIR])
    _write_pair_alias(sandbox, _ALIAS_SCOPE, {
        "tmdb:12971#s01e291@1767229200": {
            "destination_key": "tmdb:12971#s09e01",
            "destination_event_key": "tmdb:12971#s09e01@1767229200",
        }
    })

    reps = dashboard_widgets._history_alias_representatives()

    assert reps["tmdb:12971#s01e291"] == "tmdb:12971#s09e01"
    assert reps["tmdb:12971#s09e01"] == "tmdb:12971#s09e01"


def test_history_alias_representatives_rejects_foreign_scope(monkeypatch, tmp_path) -> None:
    sandbox = _alias_sandbox(monkeypatch, tmp_path, [_ALIAS_PAIR])
    _write_pair_alias(sandbox, "one-way:PLEX#PLEX-P01-TRAKT#TRAKT-P01:p9|PLEX>TRAKT", {
        "tmdb:12971#s01e291@1767229200": {"destination_key": "tmdb:12971#s09e01"}
    })

    assert dashboard_widgets._history_alias_representatives() == {}


def test_recent_history_widget_resolves_missing_art_from_metadata(monkeypatch) -> None:
    fake = FakeMetadataManager()
    monkeypatch.setattr(dashboard_widgets, "_METADATA_MANAGER", fake)
    monkeypatch.setattr(dashboard_widgets, "_METADATA_MANAGER_FAILED", False)
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {"ok": True, "total": 0, "items": []},
    )
    state = {
        "providers": {
            "TRAKT": {
                "history": {
                    "baseline": {
                        "items": {
                            "trakt:episode:456@1767229200": {
                                "type": "episode",
                                "show": {
                                    "title": "Behind the Attraction",
                                    "year": 2026,
                                    "ids": {"trakt": 123},
                                },
                                "episode": {"season": 3, "number": 2, "ids": {"trakt": 456}},
                                "watched_at": "2026-01-01T02:00:00Z",
                            }
                        }
                    }
                }
            }
        }
    }

    payload = dashboard_widgets.recent_history_widget(state, limit=5)

    assert payload["total"] == 1
    assert payload["items"][0]["tmdb"] == 100
    assert payload["items"][0]["poster"] == "/art/tmdb/tv/100?kind=still&season=3&episode=2&size=w300&artv=2"
    assert fake.calls[0]["entity"] == "show"
    assert fake.calls[0]["ids"]["title"] == "Behind the Attraction"
    assert fake.calls[0]["ids"]["trakt"] == 123
    assert "year" not in fake.calls[0]["ids"]


def test_latest_ratings_widget_merges_provider_local_movie_ids_and_inherits_art() -> None:
    state = {
        "providers": {
            "SIMKL": {
                "ratings": {
                    "baseline": {
                        "items": {
                            "simkl:rating:300": {
                                "type": "movie",
                                "title": "Heat",
                                "year": 1995,
                                "ids": {"tmdb": 949, "simkl": 300},
                                "rating": 8,
                                "rated_at": "2026-01-01T02:00:00Z",
                            }
                        }
                    }
                }
            },
            "TRAKT": {
                "ratings": {
                    "baseline": {
                        "items": {
                            "trakt:rating:456": {
                                "type": "movie",
                                "movie": {"title": "Heat", "year": 1995, "ids": {"trakt": 456}},
                                "rating": 8,
                                "rated_at": "2026-01-01T02:00:10Z",
                            }
                        }
                    }
                }
            },
        }
    }

    payload = dashboard_widgets.latest_ratings_widget(state, limit=5)

    assert payload["total"] == 1
    assert payload["items"][0]["title"] == "Heat"
    assert payload["items"][0]["poster"] == "/art/tmdb/movie/949?kind=backdrop&size=w300"
    assert payload["items"][0]["cover"] == "/art/tmdb/movie/949?size=w342"
    assert {source["provider"] for source in payload["items"][0]["sources"]} == {"SIMKL", "TRAKT"}


def test_latest_ratings_widget_merges_movie_rows_with_cross_provider_ids() -> None:
    state = {
        "providers": {
            "TRAKT": {
                "ratings": {
                    "baseline": {
                        "items": {
                            "tmdb:1662317": {
                                "type": "movie",
                                "title": "The Crash",
                                "year": 2026,
                                "ids": {"tmdb": "1662317", "imdb": "tt40792117"},
                                "rating": 8,
                                "rated_at": "2026-08-18T23:07:00Z",
                            }
                        }
                    }
                }
            },
            "STREMIO": {
                "ratings": {
                    "baseline": {
                        "items": {
                            "imdb:tt40792117": {
                                "type": "movie",
                                "title": "The Crash",
                                "ids": {"imdb": "tt40792117"},
                                "rating": 8,
                                "rated_at": "2026-08-18T23:07:00Z",
                            }
                        }
                    }
                }
            },
        }
    }

    payload = dashboard_widgets.latest_ratings_widget(state, limit=5)

    assert payload["total"] == 1
    row = payload["items"][0]
    assert row["ids"]["tmdb"] == "1662317"
    assert row["ids"]["imdb"] == "tt40792117"
    assert {source["provider"] for source in row["sources"]} == {"TRAKT", "STREMIO"}


def test_latest_ratings_widget_keeps_original_provider_time_over_same_rating_sync_echo() -> None:
    state = {
        "providers": {
            "SCROB": {
                "ratings": {
                    "baseline": {
                        "items": {
                            "tmdb:1151272": {
                                "type": "movie",
                                "title": "Sirat",
                                "year": 2025,
                                "ids": {"tmdb": 1151272},
                                "rating": 6,
                                "rated_at": "2026-08-17T00:13:57.000Z",
                            }
                        }
                    }
                }
            },
            "TRAKT": {
                "ratings": {
                    "baseline": {
                        "items": {
                            "tmdb:1151272": {
                                "type": "movie",
                                "title": "Sirat",
                                "year": 2025,
                                "ids": {"tmdb": 1151272},
                                "rating": 6,
                                "rated_at": "2026-04-03T21:03:13.000Z",
                            }
                        }
                    }
                }
            },
        }
    }

    payload = dashboard_widgets.latest_ratings_widget(state, limit=5)

    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["rating"] == 6
    assert item["rated_at"] == "2026-04-03T21:03:13.000Z"
    assert item["sort_epoch"] == 1775250193
    assert {source["provider"] for source in item["sources"]} == {"SCROB", "TRAKT"}


def test_latest_ratings_widget_keeps_tracker_rated_at_over_destination_sync_time() -> None:
    tracker_items = {
        "movie|imdb:tt0113277": {
            "type": "movie",
            "title": "Heat",
            "year": 1995,
            "rating": 8,
            "rated_at": "2020-01-01T00:00:00Z",
            "sources_by_provider": {"TRAKT": ["main"]},
        },
        "movie|imdb:tt0468569": {
            "type": "movie",
            "title": "The Dark Knight",
            "year": 2008,
            "rating": 9,
            "rated_at": "2026-01-05T00:00:00Z",
            "sources_by_provider": {"TRAKT": ["main"]},
        },
    }
    state = {
        "providers": {
            "TMDB": {
                "ratings": {
                    "baseline": {
                        "items": {
                            "tmdb:rating:949": {
                                "type": "movie",
                                "title": "Heat",
                                "year": 1995,
                                "ids": {"tmdb": 949},
                                "rating": 8,
                                "rated_at": "2026-02-01T00:00:00Z",
                            },
                            "tmdb:rating:680": {
                                "type": "movie",
                                "title": "Pulp Fiction",
                                "year": 1994,
                                "ids": {"tmdb": 680},
                                "rating": 7,
                                "rated_at": "2026-01-10T00:00:00Z",
                            },
                        }
                    }
                }
            }
        }
    }

    payload = dashboard_widgets.latest_ratings_widget(state, limit=5, tracker_items=tracker_items)

    titles = [item["title"] for item in payload["items"]]
    assert titles == ["Pulp Fiction", "The Dark Knight", "Heat"]

    heat = payload["items"][2]
    assert heat["rated_at"] == "2020-01-01T00:00:00Z"
    assert heat["tmdb"] == 949
    assert {source["provider"] for source in heat["sources"]} == {"TRAKT", "TMDB"}
    assert all(not key.startswith("_") for key in heat)


def test_latest_ratings_widget_resolves_missing_art_from_metadata(monkeypatch) -> None:
    fake = FakeMetadataManager()
    monkeypatch.setattr(dashboard_widgets, "_METADATA_MANAGER", fake)
    monkeypatch.setattr(dashboard_widgets, "_METADATA_MANAGER_FAILED", False)

    payload = dashboard_widgets.latest_ratings_widget(
        {
            "providers": {
                "TRAKT": {
                    "ratings": {
                        "baseline": {
                            "items": {
                                "trakt:rating:456": {
                                    "type": "movie",
                                    "movie": {"title": "Heat", "year": 1995, "ids": {"trakt": 456}},
                                    "rating": 8,
                                    "rated_at": "2026-01-01T02:00:10Z",
                                }
                            }
                        }
                    }
                }
            }
        },
        limit=5,
    )

    assert payload["total"] == 1
    assert payload["items"][0]["tmdb"] == 949
    assert payload["items"][0]["poster"] == "/art/tmdb/movie/949?kind=backdrop&size=w300"
    assert payload["items"][0]["cover"] == "/art/tmdb/movie/949?size=w342"
    assert fake.calls[0]["entity"] == "movie"


def test_recent_history_widget_prefers_show_title_for_episode_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {"ok": True, "total": 0, "items": []},
    )

    payload = dashboard_widgets.recent_history_widget(
        {"providers": {}},
        tracker_items={
            "tmdb:100:s3e2@1767229200": {
                "type": "episode",
                "title": "S03E02",
                "series_title": "Achter de attractie",
                "year": 2026,
                "season": 3,
                "episode": 2,
                "ids": {"tmdb": 100},
                "watched_at": "2026-01-01T02:00:00Z",
            }
        },
    )

    assert payload["total"] == 1
    assert payload["items"][0]["title"] == "Achter de attractie"
    assert payload["items"][0]["episode_label"] == "S03E02"


@pytest.mark.parametrize("provider", ["TRAKT", "SIMKL", "MDBLIST"])
def test_recent_history_widget_uses_nested_show_tmdb_for_episode_art(monkeypatch, provider: str) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {"ok": True, "total": 0, "items": []},
    )

    payload = dashboard_widgets.recent_history_widget(
        {
            "providers": {
                provider: {
                    "history": {
                        "baseline": {
                            "items": {
                                f"{provider.lower()}:episode:456@1767229200": {
                                    "type": "episode",
                                    "show": {
                                        "title": "Behind the Attraction",
                                        "year": 2026,
                                        "ids": {provider.lower(): 123, "tmdb": 100},
                                    },
                                    "episode": {
                                        "season": 3,
                                        "number": 2,
                                        "title": "S03E02",
                                        "ids": {provider.lower(): 456},
                                    },
                                    "watched_at": "2026-01-01T02:00:00Z",
                                }
                            }
                        }
                    }
                }
            }
        },
        limit=5,
    )

    assert payload["total"] == 1
    assert payload["items"][0]["title"] == "Behind the Attraction"
    assert payload["items"][0]["episode_label"] == "S03E02"
    assert payload["items"][0]["tmdb"] == 100
    assert payload["items"][0]["poster"] == "/art/tmdb/tv/100?kind=still&season=3&episode=2&size=w300&artv=2"


def test_recent_history_widget_uses_show_tmdb_for_episode_art_when_episode_has_tmdb(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {"ok": True, "total": 0, "items": []},
    )

    payload = dashboard_widgets.recent_history_widget(
        {
            "providers": {
                "TRAKT": {
                    "history": {
                        "baseline": {
                            "items": {
                                "tmdb:95738#s03e02": {
                                    "type": "episode",
                                    "title": "Behind the Attraction",
                                    "season": 3,
                                    "episode": 2,
                                    "ids": {
                                        "tmdb": "7289263",
                                        "trakt": "14195446",
                                        "show_ids": {
                                            "tmdb": "95738",
                                            "trakt": "181552",
                                            "tvdb": "404205",
                                        },
                                    },
                                    "watched_at": "2026-01-01T02:00:00Z",
                                }
                            }
                        }
                    }
                }
            }
        },
        limit=5,
    )

    assert payload["total"] == 1
    assert payload["items"][0]["tmdb"] == "95738"
    assert payload["items"][0]["poster"] == "/art/tmdb/tv/95738?kind=still&season=3&episode=2&size=w300&artv=2"


def test_existing_tmdb_art_does_not_emit_debug_log(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(dashboard_widgets, "_cw_log", lambda *args, **kwargs: calls.append((args, kwargs)))

    row = {
        "type": "episode",
        "title": "Behind the Attraction",
        "season": 3,
        "episode": 2,
        "tmdb": "95738",
        "poster": "/art/tmdb/tv/95738?kind=still&season=3&episode=2&size=w300&artv=2",
    }

    dashboard_widgets._resolve_missing_art(row, size="w300", episode_still=True)

    assert row["art_reason"] == "existing_tmdb"
    assert calls == []


def test_recent_history_widget_uses_tracker_items_without_runtime_state(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {"ok": True, "total": 0, "items": []},
    )

    payload = dashboard_widgets.recent_history_widget(
        {"providers": {}},
        tracker_items={
            "tmdb:40@1767225600": {
                "type": "movie",
                "title": "Arrival",
                "year": 2016,
                "ids": {"tmdb": 40},
                "watched_at": "2026-01-01T01:00:00Z",
            }
        },
    )

    assert payload["total"] == 1
    assert payload["items"][0]["title"] == "Arrival"
    assert payload["items"][0]["sources"] == [{"provider": "CROSSWATCH", "instance": "default"}]


def test_recent_scrobble_widget_uses_activity_log_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {
            "ok": True,
            "total": 1,
            "items": [
                {
                    "id": "event-2",
                    "kind": "scrobble",
                    "method": "webhook",
                    "event": "scrobble_stop",
                    "media_type": "movie",
                    "title": "Heat",
                    "year": 1995,
                    "source": "plex",
                    "target": "trakt",
                    "ids": {"tmdb": 949},
                    "watched_at": 1767225600,
                    "captured_at": 1767229200,
                    "status": "ok",
                }
            ],
        },
    )

    payload = dashboard_widgets.recent_scrobble_widget(limit=3)

    assert payload["ok"] is True
    assert payload["total"] == 1
    assert payload["items"][0]["title"] == "Heat"
    assert payload["items"][0]["poster"] == "/art/tmdb/movie/949?size=w300"
    assert payload["items"][0]["sources"] == [
        {"provider": "PLEX", "instance": "default"},
        {"provider": "TRAKT", "instance": "default"},
    ]


def test_recent_scrobble_widget_uses_nested_history_sync_item_art(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(activity, "state_dir", lambda: tmp_path)
    activity.clear_events()

    activity.record_history_sync_items(
        [
            {
                "type": "episode",
                "show": {
                    "title": "Behind the Attraction",
                    "year": 2026,
                    "ids": {"tmdb": 100, "trakt": 123},
                },
                "episode": {"season": 3, "number": 2, "ids": {"trakt": 456}},
                "watched_at": 1767229200,
            }
        ],
        source="trakt",
        target="crosswatch",
    )

    payload = dashboard_widgets.recent_scrobble_widget(limit=3)

    assert payload["total"] == 1
    assert payload["items"][0]["title"] == "Behind the Attraction"
    assert payload["items"][0]["episode_label"] == "S03E02"
    assert payload["items"][0]["poster"] == "/art/tmdb/tv/100?kind=still&season=3&episode=2&size=w300&artv=2"
    assert payload["items"][0]["sources"] == [
        {"provider": "TRAKT", "instance": "default"},
        {"provider": "CROSSWATCH", "instance": "default"},
    ]
    assert payload["scrobble_total"] == 0
    assert payload["scrobble_hours"] == 0.0


def test_recent_scrobble_widget_reports_scrobble_total_separately(monkeypatch) -> None:
    def fake_list_events(**kwargs: Any) -> dict[str, Any]:
        if kwargs.get("kind") == "scrobble":
            return {"ok": True, "total": 5, "items": []}
        return {
            "ok": True,
            "items": [
                {
                    "id": "history-1",
                    "kind": "history_sync",
                    "status": "ok",
                    "source": "trakt",
                    "target": "crosswatch",
                    "media_type": "movie",
                    "title": "Imported Movie",
                    "watched_at": 1767225600,
                }
            ],
        }

    monkeypatch.setattr(dashboard_widgets, "list_events", fake_list_events)

    payload = dashboard_widgets.recent_scrobble_widget(limit=3)

    assert payload["total"] == 1
    assert payload["scrobble_total"] == 5
    assert payload["scrobble_hours"] == 0.0


def _history_tracker_items(count: int) -> dict:
    return {
        f"movie:tmdb:{idx}": {
            "type": "movie",
            "title": f"Movie {idx}",
            "year": 2000 + (idx % 20),
            "ids": {"tmdb": 900000 + idx},
            "watched_at": f"2026-01-01T00:00:{idx % 60:02d}Z",
            "watched": True,
            "_epoch": 1767225600 + idx,
        }
        for idx in range(count)
    }


def test_recent_history_widget_windowing_keeps_newest_items(monkeypatch) -> None:
    monkeypatch.setattr(dashboard_widgets, "_resolve_missing_art_rows", lambda rows, **_kwargs: rows)
    monkeypatch.setattr(dashboard_widgets, "_history_alias_representatives", lambda: {})

    count = dashboard_widgets._ROW_WINDOW * 3
    items = _history_tracker_items(count)
    for idx, key in enumerate(items):
        items[key]["watched_at"] = 1767225600 + idx

    payload = dashboard_widgets.recent_history_widget({}, limit=6, tracker_items=items)
    titles = [row["title"] for row in payload["items"]]

    assert titles == [f"Movie {idx}" for idx in range(count - 1, count - 7, -1)]
    assert payload["library_total"] == count


def test_recent_history_widget_window_matches_unwindowed_top_rows(monkeypatch) -> None:
    monkeypatch.setattr(dashboard_widgets, "_resolve_missing_art_rows", lambda rows, **_kwargs: rows)
    monkeypatch.setattr(dashboard_widgets, "_history_alias_representatives", lambda: {})

    items = _history_tracker_items(dashboard_widgets._ROW_WINDOW * 2)
    for idx, key in enumerate(items):
        items[key]["watched_at"] = 1767225600 + idx

    windowed = dashboard_widgets._latest_history_tracker_rows(items, window=dashboard_widgets._ROW_WINDOW)
    full = dashboard_widgets._latest_history_tracker_rows(items, window=None)

    assert [row["key"] for row in windowed[:24]] == [row["key"] for row in full[:24]]
    assert len(windowed) <= dashboard_widgets._ROW_WINDOW
    assert len(full) == len(items)


def _rating_tracker_items(count: int, *, same_epoch: bool) -> dict:
    return {
        f"movie:tmdb:{idx}": {
            "type": "movie",
            "title": f"Rating {idx:05d}",
            "year": 2000 + (idx % 20),
            "ids": {"tmdb": 800000 + idx},
            "rating": (idx % 10) + 1,
            "rated_at": "2026-01-01T00:00:00Z" if same_epoch else f"2026-01-01T00:00:{idx % 60:02d}Z",
        }
        for idx in range(count)
    }


def test_latest_ratings_widget_window_respects_full_sort_tiebreak(monkeypatch) -> None:
    monkeypatch.setattr(dashboard_widgets, "_resolve_missing_art_rows", lambda rows, **_kwargs: rows)

    items = _rating_tracker_items(dashboard_widgets._ROW_WINDOW * 2, same_epoch=True)

    windowed = dashboard_widgets.latest_ratings_widget(
        {}, limit=24, tracker_items=items, window=dashboard_widgets._ROW_WINDOW
    )
    full = dashboard_widgets.latest_ratings_widget({}, limit=24, tracker_items=items, window=0)

    assert [row["title"] for row in windowed["items"]] == [row["title"] for row in full["items"]]
    assert windowed["library_total"] == full["library_total"] == len(items)


def test_latest_ratings_widget_widens_window_when_merge_underfills(monkeypatch) -> None:
    monkeypatch.setattr(dashboard_widgets, "_resolve_missing_art_rows", lambda rows, **_kwargs: rows)

    items = _rating_tracker_items(100, same_epoch=False)
    payload = dashboard_widgets.latest_ratings_widget({}, limit=24, tracker_items=items, window=5)

    assert len(payload["items"]) == 24


def test_recent_history_widget_widens_window_when_merge_underfills(monkeypatch) -> None:
    monkeypatch.setattr(dashboard_widgets, "_resolve_missing_art_rows", lambda rows, **_kwargs: rows)
    monkeypatch.setattr(dashboard_widgets, "_history_alias_representatives", lambda: {})
    monkeypatch.setattr(dashboard_widgets, "_ROW_WINDOW", 2)

    items = _history_tracker_items(100)
    for idx, key in enumerate(items):
        items[key]["watched_at"] = 1767225600 + idx

    payload = dashboard_widgets.recent_history_widget({}, limit=24, tracker_items=items)

    assert len(payload["items"]) == 24


def test_dashboard_widgets_api_version_tracks_history_alias_files(monkeypatch, tmp_path) -> None:
    import cw_platform.config_base as config_base
    from api import dashboardAPI
    from services import analyzer

    alias_dir = tmp_path / ".cw_state"
    alias_dir.mkdir()
    alias_file = alias_dir / "PLEX-TRAKT.history.pair_alias.json"
    alias_file.write_text('{"a": 1}', encoding="utf-8")

    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(analyzer, "CWS_DIR", alias_dir)
    monkeypatch.setattr(dashboard_widgets, "_tracker_feature_items", lambda _kind: {})
    dashboardAPI.clear_dashboard_payload_cache()

    first = dashboardAPI.dashboard_widgets(include="history", history_limit=8)
    alias_file.write_text('{"a": 2, "b": 3}', encoding="utf-8")
    second = dashboardAPI.dashboard_widgets(include="history", history_limit=8)

    assert _loads_body(first.body)["version"] != _loads_body(second.body)["version"]
    dashboardAPI.clear_dashboard_payload_cache()


def test_recent_scrobble_widget_total_is_not_page_limited(monkeypatch) -> None:
    rows = [
        {
            "id": f"scrobble-{idx}",
            "kind": "scrobble",
            "status": "ok",
            "source": "plex",
            "target": "trakt",
            "media_type": "movie",
            "title": f"Movie {idx}",
            "watched_at": 1767225600 + idx,
            "captured_at": 1767225600 + idx,
        }
        for idx in range(20)
    ]
    noise = [
        {
            "id": f"activity-{idx}",
            "kind": "activity",
            "status": "ok",
            "source": "sync",
            "media_type": "movie",
            "title": f"Noise {idx}",
            "captured_at": 1767225500 + idx,
        }
        for idx in range(5)
    ]

    def fake_list_events(**kwargs: Any) -> dict[str, Any]:
        kind = kwargs.get("kind")
        items = rows if kind == "scrobble" else [*rows, *noise]
        limit = int(kwargs.get("limit") or len(items))
        return {"ok": True, "total": len(items), "items": items[:limit]}

    monkeypatch.setattr(dashboard_widgets, "list_events", fake_list_events)
    monkeypatch.setattr(dashboard_widgets, "_resolve_missing_art_rows", lambda rows, **_kwargs: rows)

    payload = dashboard_widgets.recent_scrobble_widget(limit=4)

    assert len(payload["items"]) == 4
    assert payload["total"] == 20
    assert payload["scrobble_total"] == 20


def test_recent_scrobble_widget_reports_scrobble_hours(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {
            "ok": True,
            "total": 2,
            "items": [
                {
                    "id": "movie-1",
                    "kind": "scrobble",
                    "status": "ok",
                    "source": "plex",
                    "media_type": "movie",
                    "title": "Short Movie",
                    "duration_minutes": 100,
                    "watched_at": 1767225600,
                },
                {
                    "id": "episode-1",
                    "kind": "scrobble",
                    "status": "ok",
                    "source": "plex",
                    "media_type": "episode",
                    "title": "Pilot",
                    "watched_at": 1767312000,
                },
            ],
        },
    )

    payload = dashboard_widgets.recent_scrobble_widget(limit=3)

    assert payload["scrobble_total"] == 2
    assert payload["scrobble_hours"] == 2.4


def test_recent_progress_widget_uses_sync_state_not_live_provider_endpoints() -> None:
    state = {
        "providers": {
            "PLEX": {
                "progress": {
                    "baseline": {
                        "items": {
                            "tmdb:550": {
                                "type": "movie",
                                "title": "Fight Club",
                                "year": 1999,
                                "ids": {"tmdb": 550},
                                "progress_percent": 42.2,
                                "progress_at": "2026-01-02T02:00:00Z",
                            },
                            "tmdb:40": {
                                "type": "movie",
                                "title": "Arrival",
                                "year": 2016,
                                "ids": {"tmdb": 40},
                                "progress_ms": 900000,
                                "duration_ms": 1800000,
                                "updated_at": "2026-01-01T02:00:00Z",
                            },
                        }
                    }
                }
            }
        }
    }

    payload = dashboard_widgets.recent_progress_widget(state, limit=5, tracker_items={})

    assert payload["ok"] is True
    assert payload["total"] == 2
    assert payload["items"][0]["title"] == "Fight Club"
    assert payload["items"][0]["progress"] == 42.2
    assert payload["items"][1]["progress"] == 50.0
    assert payload["items"][0]["sources"] == [{"provider": "PLEX", "instance": "default"}]


def test_recent_progress_widget_merges_movie_rows_with_cross_provider_ids() -> None:
    state = {
        "providers": {
            "TRAKT": {
                "progress": {
                    "baseline": {
                        "items": {
                            "tmdb:1662317": {
                                "type": "movie",
                                "title": "The Crash",
                                "year": 2026,
                                "ids": {"tmdb": "1662317", "imdb": "tt40792117"},
                                "progress_percent": 42.0,
                                "progress_at": "2026-08-18T23:07:00Z",
                            }
                        }
                    }
                }
            },
            "STREMIO": {
                "progress": {
                    "baseline": {
                        "items": {
                            "imdb:tt40792117": {
                                "type": "movie",
                                "title": "The Crash",
                                "ids": {"imdb": "tt40792117"},
                                "progress_percent": 42.0,
                                "progress_at": "2026-08-18T23:07:00Z",
                            }
                        }
                    }
                }
            },
        }
    }

    payload = dashboard_widgets.recent_progress_widget(state, limit=5, tracker_items={})

    assert payload["total"] == 1
    row = payload["items"][0]
    assert row["ids"]["tmdb"] == "1662317"
    assert row["ids"]["imdb"] == "tt40792117"
    assert {source["provider"] for source in row["sources"]} == {"TRAKT", "STREMIO"}


def test_recent_playlists_widget_uses_playlist_activity(monkeypatch) -> None:
    from services import playlists

    monkeypatch.setattr(
        playlists,
        "activity",
        lambda _cfg, limit=25: [
            {"ts": 1767225600, "type": "Run", "status": "completed", "label": "MAP-01", "details": "+2/-0"}
        ][:limit],
    )
    monkeypatch.setattr("cw_platform.config_base.load_config", lambda: {})

    payload = dashboard_widgets.recent_playlists_widget(limit=3)

    assert payload == {
        "ok": True,
        "items": [{"ts": 1767225600, "type": "Run", "status": "completed", "label": "MAP-01", "details": "+2/-0"}],
        "total": 1,
    }


def test_recent_playlists_widget_total_is_not_page_limited(monkeypatch) -> None:
    from services import playlists

    rows = [
        {"ts": 1767225600 + idx, "type": "Run", "status": "completed", "label": f"MAP-{idx:02d}", "details": "+1/-0"}
        for idx in range(10)
    ]
    monkeypatch.setattr(playlists, "activity", lambda _cfg, limit=25: rows if limit is None else rows[:limit])
    monkeypatch.setattr("cw_platform.config_base.load_config", lambda: {})

    payload = dashboard_widgets.recent_playlists_widget(limit=4)

    assert len(payload["items"]) == 4
    assert payload["total"] == 10


def _many_movie_items(feature: str, count: int) -> dict:
    time_key = "rated_at" if feature == "ratings" else "watched_at"
    return {
        "providers": {
            "PLEX": {
                feature: {
                    "baseline": {
                        "items": {
                            f"tmdb:{n}@17672292{n:02d}": {
                                "type": "movie",
                                "title": f"Movie {n}",
                                "year": 2000 + n,
                                "ids": {"tmdb": n},
                                "rating": 8,
                                time_key: f"2026-01-01T00:00:{n:02d}Z",
                            }
                            for n in range(1, count + 1)
                        }
                    }
                }
            }
        }
    }


@pytest.mark.parametrize(
    ("feature", "widget", "per_row"),
    [("history", "recent_history_widget", 2), ("ratings", "latest_ratings_widget", 3), ("progress", "recent_progress_widget", 2)],
)
def test_widgets_resolve_art_namespace_only_for_returned_rows(monkeypatch, feature, widget, per_row) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {"ok": True, "total": 0, "items": []},
    )
    calls: list[object] = []
    real = dashboard_widgets._resolved_art_type
    monkeypatch.setattr(
        dashboard_widgets,
        "_resolved_art_type",
        lambda item, tmdb: (calls.append(tmdb), real(item, tmdb))[1],
    )

    payload = getattr(dashboard_widgets, widget)(_many_movie_items(feature, 60), limit=5)

    assert payload["total"] == 60
    assert len(payload["items"]) == 5
    assert len(calls) <= 5 * per_row
    assert len(set(calls)) == 5


def test_dashboard_widgets_payload_only_builds_included_widgets(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(dashboard_widgets, "_tracker_feature_items", lambda kind: calls.append(f"tracker:{kind}") or {})
    monkeypatch.setattr(
        dashboard_widgets,
        "recent_history_widget",
        lambda *_args, **_kwargs: calls.append("history") or {"ok": True, "items": [], "total": 0},
    )
    monkeypatch.setattr(
        dashboard_widgets,
        "recent_scrobble_widget",
        lambda **_kwargs: calls.append("scrobble") or {"ok": True, "items": [], "total": 0},
    )
    monkeypatch.setattr(
        dashboard_widgets,
        "latest_ratings_widget",
        lambda *_args, **_kwargs: calls.append("ratings") or {"ok": True, "items": [], "total": 0},
    )
    monkeypatch.setattr(
        dashboard_widgets,
        "recent_progress_widget",
        lambda *_args, **_kwargs: calls.append("progress") or {"ok": True, "items": [], "total": 0},
    )
    monkeypatch.setattr(
        dashboard_widgets,
        "recent_playlists_widget",
        lambda **_kwargs: calls.append("playlists") or {"ok": True, "items": [], "total": 0},
    )

    payload = dashboard_widgets.dashboard_widgets_payload({}, include={"ratings", "playlists"})

    assert payload == {
        "ok": True,
        "latest_ratings": {"ok": True, "items": [], "total": 0},
        "recent_playlists": {"ok": True, "items": [], "total": 0},
    }
    assert calls == ["tracker:ratings", "ratings", "playlists"]


def test_dashboard_widgets_api_reads_state_widgets_from_db(monkeypatch, tmp_path) -> None:
    import cw_platform.config_base as config_base
    from api import dashboardAPI
    from cw_platform.orchestrator._state_store import StateStore

    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(dashboard_widgets, "_tracker_feature_items", lambda _kind: {})
    store = StateStore(tmp_path)
    store.save_feature_baseline(
        provider="TRAKT",
        feature="history",
        items={"movie:tmdb:949@1767225600": {"type": "movie", "title": "Heat", "ids": {"tmdb": 949}, "watched_at": "2026-01-01T01:00:00Z"}},
        last_sync_epoch=1767225600,
    )
    store.save_feature_baseline(
        provider="TRAKT",
        feature="ratings",
        items={"movie:tmdb:550": {"type": "movie", "title": "Fight Club", "ids": {"tmdb": 550}, "rating": 9, "rated_at": "2026-01-02T01:00:00Z"}},
        last_sync_epoch=1767225600,
    )
    store.save_feature_baseline(
        provider="TRAKT",
        feature="progress",
        items={"movie:tmdb:40": {"type": "movie", "title": "Arrival", "ids": {"tmdb": 40}, "progress_ms": 900000, "duration_ms": 1800000, "progress_at": "2026-01-03T01:00:00Z"}},
        last_sync_epoch=1767225600,
    )

    response = dashboardAPI.dashboard_widgets(
        history_limit=8,
        ratings_limit=12,
        scrobble_limit=8,
        progress_limit=8,
        playlists_limit=8,
        include="history,ratings,progress",
    )
    payload = _loads_body(response.body)

    assert payload["ok"] is True
    assert payload["recent_history"]["items"][0]["tmdb"] == "949"
    assert payload["latest_ratings"]["items"][0]["tmdb"] == "550"
    assert payload["recent_progress"]["items"][0]["tmdb"] == "40"


def test_dashboard_widgets_api_returns_not_modified_for_known_version(monkeypatch, tmp_path) -> None:
    import cw_platform.config_base as config_base
    from api import dashboardAPI

    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(dashboard_widgets, "_tracker_feature_items", lambda _kind: {})

    first = dashboardAPI.dashboard_widgets(
        history_limit=8,
        ratings_limit=12,
        scrobble_limit=8,
        progress_limit=8,
        playlists_limit=8,
        include="history",
    )
    version = _loads_body(first.body)["version"]
    monkeypatch.setattr(
        dashboardAPI,
        "dashboard_widgets_payload",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("payload should not rebuild")),
    )

    second = dashboardAPI.dashboard_widgets(
        history_limit=8,
        ratings_limit=12,
        scrobble_limit=8,
        progress_limit=8,
        playlists_limit=8,
        include="history",
        known_version=version,
    )
    payload = _loads_body(second.body)

    assert payload == {"ok": True, "not_modified": True, "version": version}


def test_dashboard_widgets_api_reuses_payload_for_same_version(monkeypatch, tmp_path) -> None:
    import cw_platform.config_base as config_base
    from api import dashboardAPI

    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(dashboard_widgets, "_tracker_feature_items", lambda _kind: {})
    dashboardAPI.clear_dashboard_payload_cache()

    first = dashboardAPI.dashboard_widgets(
        history_limit=8,
        ratings_limit=12,
        scrobble_limit=8,
        progress_limit=8,
        playlists_limit=8,
        include="history",
    )
    first_payload = _loads_body(first.body)

    monkeypatch.setattr(
        dashboardAPI,
        "dashboard_widgets_payload",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("payload should not rebuild")),
    )

    second = dashboardAPI.dashboard_widgets(
        history_limit=8,
        ratings_limit=12,
        scrobble_limit=8,
        progress_limit=8,
        playlists_limit=8,
        include="history",
    )

    assert _loads_body(second.body) == first_payload
    dashboardAPI.clear_dashboard_payload_cache()


def test_dashboard_widgets_api_rebuilds_when_version_changes(monkeypatch, tmp_path) -> None:
    import cw_platform.config_base as config_base
    from api import dashboardAPI

    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(dashboard_widgets, "_tracker_feature_items", lambda _kind: {})
    dashboardAPI.clear_dashboard_payload_cache()

    first = dashboardAPI.dashboard_widgets(include="history", history_limit=8)
    second = dashboardAPI.dashboard_widgets(include="history", history_limit=12)

    assert _loads_body(first.body)["version"] != _loads_body(second.body)["version"]
    dashboardAPI.clear_dashboard_payload_cache()


def test_dashboard_widgets_api_version_tracks_crosswatch_tracker_files(monkeypatch, tmp_path) -> None:
    import cw_platform.config_base as config_base
    from api import dashboardAPI

    root = tmp_path / ".cw_provider"
    root.mkdir()
    (tmp_path / "config.json").write_text(
        json.dumps({"crosswatch": {"root_dir": str(root)}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(dashboard_widgets, "_tracker_feature_items", lambda _kind: {})

    first = dashboardAPI.dashboard_widgets(
        history_limit=8,
        ratings_limit=12,
        scrobble_limit=8,
        progress_limit=8,
        playlists_limit=8,
        include="history",
    )
    version = _loads_body(first.body)["version"]
    (root / "history.json").write_text(
        json.dumps({"items": {"movie:tmdb:949": {"type": "movie", "title": "Heat", "ids": {"tmdb": 949}}}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        dashboardAPI,
        "dashboard_widgets_payload",
        lambda *_args, **_kwargs: {"ok": True, "recent_history": {"ok": True, "items": [], "total": 0}},
    )

    second = dashboardAPI.dashboard_widgets(
        history_limit=8,
        ratings_limit=12,
        scrobble_limit=8,
        progress_limit=8,
        playlists_limit=8,
        include="history",
        known_version=version,
    )
    payload = _loads_body(second.body)

    assert payload["ok"] is True
    assert payload.get("not_modified") is not True
    assert payload["version"] != version


def test_dashboard_widget_frontend_uses_cached_payload_version() -> None:
    js = Path("assets/js/dashboard-widgets.js").read_text("utf-8")
    profile_js = Path("assets/js/profile-page.js").read_text("utf-8")

    assert "const DATA_CACHE_KEY" in js
    assert "function readCachedWidgetData()" in js
    assert "function writeCachedWidgetData(payload)" in js
    assert "function clearDashboardWidgetState()" in js
    assert 'window.addEventListener("cw:sync-state-cleared", clearDashboardWidgetState);' in js
    assert "clear: clearDashboardWidgetState" in js
    assert "const WIDGET_FETCH_TIMEOUT_MS = 30 * 1000;" in js
    assert "window.CW.API.j(url, {}, WIDGET_FETCH_TIMEOUT_MS)" in js
    assert 'controller.abort("timeout")' in js
    assert "function isTimeoutError(e)" in js
    assert 'return e === "timeout" || e?.name === "AbortError";' in js
    assert "if (isTimeoutError(e)) break;" in js
    assert "const WIDGET_LOADING_DELAY_MS = 700;" in js
    assert 'const REFRESHABLE_WIDGETS = ["history", "ratings", "scrobble", "progress", "playlists"];' in js
    assert "const latestTotals = { history: 0, ratings: 0, scrobble: 0, progress: 0, playlists: 0 };" in js
    assert "block.scrobble_total" in js
    assert "async function refreshDashboardWidgets({ forceConfig = false, force = false, preserve = true, kinds = null } = {})" in js
    assert 'params.set("known_version", cachedPayload.version)' in js
    assert "if (data?.not_modified && cachedPayload)" in js
    assert "if (requestedKinds.some((kind) => !hasPreservableContent(hosts[kind]))) {" in js
    assert "applyWidgetPayload(cached, active)" in js
    assert "if (!hasLoaded && cachedPayload)" in js
    assert "applyWidgetPayload(cachedPayload, active)" in js
    assert "const preserve = opts.preserve === false ? hasLoaded : true;" in js
    assert "try { await window.CW?.OverviewProfile?.ready; } catch {}\n    if (seq !== loadSeq) return null;\n    if (!isOnMain()) return false;\n    if (!hasLoaded) revealFromCache();" in js
    assert "function scheduleSlowWidgetLoading(hosts, requestedKinds)" in js
    assert "if (!loadedWidgetKinds.has(kind)) setLoading(hosts[kind], kind);" in js
    assert "function hasPreservableContent(host)" in js
    assert "function setWidgetLoadError(host, message, preserve)" in js
    assert "function showWidgetToast(message, ok = false)" in js
    assert "function setExpandBusy(btn, busy)" in js
    assert 'btn.classList.toggle("spinning"' not in js
    assert "function widgetFetchLimit(key)" in js
    assert 'history_limit: String(widgetFetchLimit("history"))' in js
    assert 'history_limit: String(MAX_WIDGET_ITEMS)' not in js
    assert "function expandWidget(kind, opts = {})" in js
    assert "if (!REFRESHABLE_WIDGETS.includes(kind)) return false;" in js
    assert 'refreshDashboardWidgets({ force: true, preserve: true, kinds: [kind] })' in js
    assert "if (ok === null) return;" in js
    assert "visibleCounts[kind] = current;" in js
    assert 'showWidgetToast("Could not load more items.");' in js
    assert "if (!wanted) lastLoadedAt = Date.now();" in js
    assert "window.console.warn(\"[CrossWatch] dashboard widgets refresh failed\"" in js
    assert 'const shouldShowWidgetError = (kind) => active[kind] && (!partialRefresh || requestedKinds.includes(kind));' in js
    assert "if (!partialRefresh) {\n        writeCachedWidgetData(data);\n        widgetsDirty = dirtyVersion !== refreshVersion;\n      }" in js
    assert "const requestedKinds = REFRESHABLE_WIDGETS.filter((key) => active[key] && (!wantedKinds || wantedKinds.has(key)));" in js
    assert "mergeWidgetPayload" not in js
    assert "loadedKinds" not in js
    assert "refreshDashboardWidgets({ preserve: true });" in js
    assert "refreshDashboardWidgets({ forceConfig: true, preserve: true })" in js
    assert 'window.addEventListener("sync-complete", refreshForSyncComplete);' in js
    assert 'window.addEventListener("cw:scrobble-stopped", refreshForScrobbleStopped);' in js
    assert 'markWidgetsDirty(0, { kinds: ["scrobble", "history", "progress"] });' in js
    assert 'window.addEventListener("watchlist:refresh", () => markWidgetsDirty(250));' not in js
    assert "const readAnyCache = (key) =>" in profile_js
    assert 'widgetParams.set("known_version", cached.widgets.version)' in profile_js
    assert "if (widgetsNotModified)" in profile_js


def test_dashboard_widgets_api_reads_scrobble_from_activity_db(monkeypatch, tmp_path) -> None:
    from api import dashboardAPI

    monkeypatch.setattr(activity, "state_dir", lambda: tmp_path / ".cw_state")
    activity.clear_events()
    activity.add_event(
        {
            "kind": "scrobble",
            "method": "webhook",
            "event": "scrobble_stop",
            "status": "ok",
            "source": "plex",
            "target": "trakt",
            "media_type": "movie",
            "title": "Heat",
            "year": 1995,
            "watched_at": 1767225600,
            "captured_at": 1767229200,
            "ids": {"tmdb": 949},
        }
    )

    response = dashboardAPI.dashboard_widgets(
        history_limit=8,
        ratings_limit=12,
        scrobble_limit=8,
        progress_limit=8,
        playlists_limit=8,
        include="scrobble",
    )
    payload = _loads_body(response.body)

    assert payload["ok"] is True
    assert payload["recent_scrobble"]["items"][0]["tmdb"] == "949"


def test_recent_history_widget_filters_by_user_profile_instances() -> None:
    state = {
        "providers": {
            "PLEX": {
                "history": {
                    "baseline": {
                        "items": {
                            "tmdb:1@1": {"type": "movie", "title": "Alice Movie", "ids": {"tmdb": 1}, "watched_at": 1767225600}
                        }
                    }
                },
                "instances": {
                    "PLEX-P02": {
                        "history": {
                            "baseline": {
                                "items": {
                                    "tmdb:2@2": {"type": "movie", "title": "Bob Movie", "ids": {"tmdb": 2}, "watched_at": 1767312000}
                                }
                            }
                        }
                    }
                },
            }
        }
    }

    payload = dashboard_widgets.recent_history_widget(state, user_filter={"PLEX": ["PLEX-P02"]})

    assert payload["total"] == 1
    assert payload["items"][0]["title"] == "Bob Movie"


def test_recent_scrobble_widget_filters_by_user_profile_instances(monkeypatch) -> None:
    monkeypatch.setattr(
        dashboard_widgets,
        "list_events",
        lambda **_kwargs: {
            "ok": True,
            "items": [
                {
                    "id": "a",
                    "kind": "scrobble",
                    "status": "ok",
                    "source": "plex",
                    "source_instance": "default",
                    "media_type": "movie",
                    "title": "Alice Movie",
                    "watched_at": 1767225600,
                },
                {
                    "id": "b",
                    "kind": "scrobble",
                    "status": "ok",
                    "source": "plex",
                    "source_instance": "PLEX-P02",
                    "media_type": "movie",
                    "title": "Bob Movie",
                    "watched_at": 1767312000,
                },
            ],
        },
    )

    payload = dashboard_widgets.recent_scrobble_widget(user_filter={"PLEX": "PLEX-P02"})

    assert payload["total"] == 1
    assert payload["items"][0]["title"] == "Bob Movie"
