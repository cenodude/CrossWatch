# tests/test_half_point_ratings.py
# CrossWatch - Half-point ratings across manual actions, widgets and profiles
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import pytest

from api import editorAPI, manualAPI
from services import dashboard_widgets, profile_history


@pytest.mark.parametrize("score", [0.5, 1, 7.5, 9.5, 10])
def test_rating_paths_preserve_half_points(score):
    item = {"type": "movie", "title": "Movie", "ids": {"tmdb": "1"}, "rating": score}
    assert manualAPI._manual_rating(score) == score
    assert editorAPI._normalize_send_item(item, "ratings")["rating"] == score
    assert dashboard_widgets._rating_value(item) == score


@pytest.mark.parametrize("score", [True, 0, 0.25, 7.25, 11, "nan", "inf"])
def test_rating_inputs_reject_invalid_steps(score):
    item = {"type": "movie", "ids": {"tmdb": "1"}, "rating": score}
    assert manualAPI._manual_rating(score) is None
    assert editorAPI._normalize_send_item(item, "ratings") is None


def test_profile_half_point_counts_filter_and_average():
    items = {str(i): {"type": "movie", "title": str(i), "ids": {"tmdb": str(i)}, "rating": score}
             for i, score in enumerate([0.5, 7.5, 8], 1)}
    state = {"providers": {"PLEX": {"ratings": {"baseline": {"items": items}}}}}
    index = profile_history.build_history_index("ratings", state=state, tracker_items={})
    payload = profile_history.build_history_payload(index, resolve_art=False)
    assert payload["counts"]["ratings"]["0.5"] == 1
    assert payload["counts"]["ratings"]["7.5"] == 1
    assert payload["stats"]["average"] == 5.3
    filtered = profile_history.build_history_payload(index, resolve_art=False, rating="7.5")
    assert [row["rating"] for row in filtered["items"]] == [7.5]
