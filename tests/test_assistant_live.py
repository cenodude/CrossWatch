# tests/test_assistant_live.py
# CrossWatch - Opt-in provider regression checks using synthetic instance data
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os

import pytest

from services import assistant, assistant_tools

pytestmark = pytest.mark.skipif(os.getenv("CW_ASSISTANT_LIVE") != "1", reason="Opt in with CW_ASSISTANT_LIVE=1; provider charges may apply")


@pytest.mark.parametrize("question,tool,fixture", [
    ("List my saved Editor mappings.", "list_saved_mappings", {"items": [{"key": "tmdb:123", "corrected": {"title": "Synthetic Film"}}], "total": 1}),
    ("Show my Anime ID custom mappings.", "list_anime_overrides", {"items": [{"title": "Synthetic Anime", "enabled": True, "match_id": "123", "target_id": "456"}], "total": 1}),
    ("When will my next scheduled sync run? Check the scheduler.", "get_scheduler_details", {"running": False, "next_run_at": 1800000000, "warnings": []}),
    ("Compare saved captures test-before.json and test-after.json for watchlist changes.", "compare_captures", {"summary": {"added": 1, "removed": 0, "updated": 0}, "items": [{"status": "added", "new": {"title": "Synthetic Film"}}]}),
    ("What is stored in pair demo's watchlist? Check the saved items, not live providers.", "search_sync_items", {"pair": {"id": "demo"}, "feature": "watchlist", "caution": "Saved state, not live", "sides": [{"side": "source", "evidence": [{"items": [{"key": "tmdb:123", "title": "Synthetic Film"}]}]}]}),
])
def test_live_provider_selects_relevant_tool(question, tool, fixture, monkeypatch):
    url, model = os.getenv("CW_ASSISTANT_TEST_URL"), os.getenv("CW_ASSISTANT_TEST_MODEL")
    if not url or not model:
        pytest.fail("Set CW_ASSISTANT_TEST_URL and CW_ASSISTANT_TEST_MODEL; optional key is CW_ASSISTANT_TEST_KEY")
    calls = []

    def synthetic(name, args, runner, deadline=None):
        calls.append(name)
        if name == tool:
            return json.dumps(fixture)
        if name == "list_sync_context":
            return json.dumps({"profiles": [], "pairs": [{"id": "demo", "source": {"provider": "PLEX", "instance": "default"}, "target": {"provider": "TRAKT", "instance": "default"}, "features": ["watchlist"]}]})
        return "No additional data in this synthetic test. Use the task-specific tool."

    monkeypatch.setattr(assistant, "_run", synthetic)
    monkeypatch.setattr(assistant, "MAX_ROUNDS", 5)
    monkeypatch.setattr(assistant, "CHAT_DEADLINE_S", 60)
    conf = {"base_url": url.rstrip("/"), "model": model, "api_key": os.getenv("CW_ASSISTANT_TEST_KEY", "")}
    result = assistant.run_bounded(lambda: assistant.chat(conf, [{"role": "user", "content": question}], assistant_tools.TOOLS, lambda name, args: "Unavailable"), 60)
    assert tool in calls, f"Model did not call {tool}"
    assert result["reply"] and result["reply"] != assistant.NO_TEXT
