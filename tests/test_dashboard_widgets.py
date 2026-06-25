from __future__ import annotations

import pytest

from services import activity, dashboard_widgets


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
        self.calls: list[dict[str, object]] = []

    def resolve(self, **kwargs):
        self.calls.append(dict(kwargs))
        ids = kwargs.get("ids") if isinstance(kwargs.get("ids"), dict) else {}
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
    assert payload["items"][0]["poster"] == "/art/tmdb/movie/10?size=w342"
    assert payload["items"][1]["poster"] == "/art/tmdb/tv/20?size=w342"


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
    assert ratings["items"][0]["poster"] == "/art/tmdb/movie/949?size=w342"
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
    assert payload["items"][0]["poster"] == "/art/tmdb/movie/949?size=w342"


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
    assert payload["items"][0]["poster"] == "/art/tmdb/movie/949?size=w342"
    assert {source["provider"] for source in payload["items"][0]["sources"]} == {"SIMKL", "TRAKT"}


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
    assert payload["items"][0]["poster"] == "/art/tmdb/movie/949?size=w342"
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
