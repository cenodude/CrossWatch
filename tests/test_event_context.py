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
