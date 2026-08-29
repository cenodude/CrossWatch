from __future__ import annotations

import json

from cw_platform.event_archive import context


def test_event_context_title_enrichment_reads_collection_state(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(context, "_config_base", lambda: tmp_path)
    context._TITLE_CACHE["index"] = None
    context._TITLE_CACHE["ts"] = 0.0

    state = {
        "providers": {
            "PLEX": {
                "collection": {
                    "baseline": {
                        "items": {
                            "tmdb:10": {
                                "type": "movie",
                                "title": "Owned Movie",
                                "year": 2026,
                                "ids": {"tmdb": "10"},
                            }
                        }
                    }
                }
            }
        }
    }
    (tmp_path / "state.test.json").write_text(json.dumps(state), encoding="utf-8")

    title = context.resolve_title("tmdb:10")

    assert title == {
        "series_title": None,
        "title": "Owned Movie",
        "year": 2026,
        "media_type": "movie",
        "season": None,
        "episode": None,
    }


def test_title_index_cache_timestamp_is_set_after_slow_build(monkeypatch) -> None:
    context._TITLE_CACHE["index"] = None
    context._TITLE_CACHE["ts"] = 0.0
    calls = {"baseline": 0}
    ticks = iter([100.0, 113.5, 114.0])

    def slow_baseline_states():
        calls["baseline"] += 1
        return [
            {
                "providers": {
                    "TRAKT": {
                        "history": {
                            "baseline": {
                                "items": {
                                    "tmdb:10": {
                                        "type": "movie",
                                        "title": "Slow Movie",
                                        "year": 2026,
                                        "ids": {"tmdb": "10"},
                                    }
                                }
                            }
                        }
                    }
                }
            }
        ]

    monkeypatch.setattr(context.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(context, "_baseline_states", slow_baseline_states)
    monkeypatch.setattr(context, "_iter_state_files", lambda *args, **kwargs: iter(()))

    first = context._title_index()
    second = context._title_index()

    assert first is second
    assert calls["baseline"] == 1
