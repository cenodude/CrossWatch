# tests/test_half_point_ratings.py
# CrossWatch - Half-point ratings across manual actions, widgets and profiles
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import json
from types import SimpleNamespace

import pytest

from api import editorAPI, manualAPI
from services import dashboard_widgets, profile_history
from cw_platform.modules_registry import load_sync_ops, sync_provider_names
from cw_platform.rating_scales import rating_comparator, rating_scale, rating_supported


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


@pytest.mark.parametrize("provider", [name for name in sync_provider_names() if load_sync_ops(name).features().get("ratings")])
def test_provider_rating_choices_match_manual_precision(provider):
    spec = load_sync_ops(provider).capabilities()["ratings"]["manual_scale"]
    scale = rating_scale(provider)
    assert scale["values"]
    assert all(rating_supported(provider, value) for value in scale["values"])
    if not spec.get("dynamic"):
        assert scale["values"][-1] == 10
        assert rating_supported(provider, 7.5) is (spec["step"] == 0.5)
    assert not rating_supported(provider, True)
    assert not rating_supported(provider, "nan")


def test_mdblist_half_points_start_at_one():
    from providers.sync.mdblist._ratings import _valid_rating

    assert not rating_supported("MDBLIST", 0.5)
    assert rating_supported("MDBLIST", 1.5)
    assert all(_valid_rating(value) == value for value in rating_scale("MDBLIST")["values"])


def test_stremio_choices_follow_configured_reaction_thresholds():
    assert rating_scale("STREMIO")["labels"] == {"6": "Liked", "8": "Loved"}
    cfg = {"stremio": {"ratings": {"liked_min": 6.7, "loved_min": 9.2}}}
    assert rating_scale("STREMIO", cfg)["values"] == [7, 9.5]
    assert rating_supported("STREMIO", 9.5, cfg)
    assert not rating_supported("STREMIO", 8, cfg)


def test_provider_targets_publish_the_same_scale_used_for_validation(monkeypatch):
    from cw_platform.provider_instances import build_provider_config_view

    cfg = {"stremio": {"ratings": {"liked_min": 6, "loved_min": 8}, "instances": {
        "second": {"ratings": {"liked_min": 6.7, "loved_min": 9.2}}}}}
    ops = SimpleNamespace(features=lambda: {"ratings": True, "watchlist": True}, is_configured=lambda _: True, label=lambda: "Provider")
    for api in (editorAPI, manualAPI):
        monkeypatch.setattr(api, "sync_provider_names", lambda **_: ["SIMKL", "CROSSWATCH", "MDBLIST", "STREMIO"])
        monkeypatch.setattr(api, "load_sync_ops", lambda _: ops)
        monkeypatch.setattr(api, "list_instance_ids", lambda *_: ["default", "second"])
    monkeypatch.setattr(manualAPI, "filter_instances_for_user", lambda _cfg, _user, _provider, instances: instances)
    for rows in (editorAPI._editor_send_targets(cfg, "ratings"), manualAPI._manual_history_targets(cfg)):
        for row in rows:
            view = build_provider_config_view(cfg, row["provider"], row["instance"])
            assert row["rating_scale"] == rating_scale(row["provider"], view)
            if row["provider"] == "STREMIO" and row["instance"] == "second":
                assert row["rating_scale"]["values"] == [7, 9.5]


def test_every_rating_adapter_has_an_explicit_manual_scale():
    from importlib import import_module
    from cw_platform.modules_registry import get_sync_module_path_by_name

    for provider in sync_provider_names():
        ops = load_sync_ops(provider)
        assert ops is not None
        if ops.features().get("ratings"):
            spec = ops.capabilities()["ratings"]["manual_scale"]
            assert isinstance(spec, dict), provider
            manifest = import_module(get_sync_module_path_by_name(provider)).get_manifest()
            assert manifest["capabilities"]["ratings"]["manual_scale"] == spec, provider
            if spec.get("dynamic"):
                assert callable(getattr(ops, "rating_scale", None)), provider
            else:
                assert spec["step"] in (0.5, 1, 2), provider
            assert rating_scale(provider)["values"], provider
        else:
            assert rating_scale(provider)["values"] == [], provider
    assert rating_scale("NEW_UNREGISTERED_PROVIDER")["values"] == []
    assert not rating_supported("NEW_UNREGISTERED_PROVIDER", 8)


def test_manual_scale_comes_from_adapter_without_provider_branches(monkeypatch):
    ops = SimpleNamespace(features=lambda: {"ratings": True}, capabilities=lambda: {"ratings": {
        "manual_scale": {"step": 2, "min": 4, "max": 5, "display_factor": 0.5, "round_step": 2}}})
    monkeypatch.setattr("cw_platform.rating_scales.load_sync_ops", lambda _: ops)
    assert rating_scale("NEW_PROVIDER") == {"values": [4, 6, 8, 10], "max": 5, "display_factor": 0.5, "round_step": 2}
    assert rating_supported("NEW_PROVIDER", 6)
    assert not rating_supported("NEW_PROVIDER", 7)


def test_missing_manual_scale_does_not_invent_choices(monkeypatch):
    ops = SimpleNamespace(features=lambda: {"ratings": True}, capabilities=lambda: {"ratings": {"step": 0.1}})
    monkeypatch.setattr("cw_platform.rating_scales.load_sync_ops", lambda _: ops)
    assert rating_scale("NEW_PROVIDER")["values"] == []


def test_dynamic_scale_receives_the_scoped_config(monkeypatch):
    seen = []
    cfg = {"new_provider": {"threshold": 7}}

    def resolve(view):
        seen.append(view)
        return {"values": [view["new_provider"]["threshold"]], "labels": {"7": "Liked"}, "max": 10, "display_factor": 1}

    ops = SimpleNamespace(features=lambda: {"ratings": True}, capabilities=lambda: {"ratings": {"manual_scale": {"dynamic": True}}}, rating_scale=resolve)
    monkeypatch.setattr("cw_platform.rating_scales.load_sync_ops", lambda _: ops)
    assert rating_scale("NEW_PROVIDER", cfg)["values"] == [7]
    assert seen == [cfg]


@pytest.mark.parametrize("liked,loved", [(6, 9), (6.7, 9.2), (8, 6), (10, 10), (0, 0)])
def test_stremio_manual_choices_share_adapter_thresholds(liked, loved):
    from providers.sync.stremio._ratings import _status_for_rating

    cfg = {"stremio": {"ratings": {"liked_min": liked, "loved_min": loved}}}
    scale = rating_scale("STREMIO", cfg)
    adapter = SimpleNamespace(config=cfg, instance_id="default")
    for band in scale["bands"]:
        value = band["value"]
        label = scale["labels"][str(int(value) if value.is_integer() else value)].lower()
        assert _status_for_rating(adapter, value) == label
        assert _status_for_rating(adapter, min(10, (band["min"] + band["max"]) / 2)) == label


@pytest.mark.parametrize("provider,source,saved,expected", [
    ("SIMKL", 7.5, 8, True), ("SIMKL", 7.5, 7, False),
    ("TRAKT", 6.5, 7, True), ("TRAKT", 6.5, 6, False),
    ("TMDB", 7.5, 8, False), ("FLOPPY", 7.5, 7.6, False),
    ("SILO", 7, 8, True), ("SILO", 7, 6, False),
    ("STREMIO", 7, 6, True), ("STREMIO", 8, 6, False),
    ("UNKNOWN", 7.5, 8, False), ("UNKNOWN", 7.5, 7.5, True),
])
def test_rating_agreement_allows_only_expected_provider_conversions(provider, source, saved, expected):
    ratings = {("CROSSWATCH", "default"): source, (provider, "default"): saved}
    agrees = rating_comparator({})
    assert agrees(ratings) is expected
    assert agrees(dict(reversed(list(ratings.items())))) is expected
    assert ratings[("CROSSWATCH", "default")] == source


def test_coarse_provider_does_not_hide_disagreement_between_precise_providers():
    assert not rating_comparator({})({("CROSSWATCH", "default"): 7.5, ("TMDB", "default"): 8, ("SIMKL", "default"): 8})


def test_rating_agreement_uses_each_instances_reaction_thresholds():
    cfg = {"stremio": {"instances": {"second": {"ratings": {"liked_min": 6, "loved_min": 9}}}}}
    agrees = rating_comparator(cfg)
    assert agrees({("CROSSWATCH", "default"): 8, ("STREMIO", "second"): 6})
    assert not agrees({("CROSSWATCH", "default"): 8, ("STREMIO", "default"): 6})


@pytest.mark.parametrize("source", ["state", "manual"])
def test_editor_rows_publish_the_selected_instances_scale(monkeypatch, source):
    cfg = {"stremio": {"ratings": {"liked_min": 6, "loved_min": 8}, "instances": {
        "second": {"ratings": {"liked_min": 6.7, "loved_min": 9.2}}}}}
    monkeypatch.setattr(editorAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(editorAPI, "_load_current_state_features", lambda *_: {})
    monkeypatch.setattr(editorAPI, "_load_policy", lambda: {})
    monkeypatch.setattr(editorAPI, "_state_exists", lambda: False)
    monkeypatch.setattr(editorAPI, "_policy_exists", lambda: False)
    result = editorAPI.api_editor_get_state(kind="ratings", source=source, provider="STREMIO", provider_instance="second", request=None)
    assert result["rating_scale"]["values"] == [7, 9.5]
    assert result["rating_scale"]["labels"] == {"7": "Liked", "9.5": "Loved"}


@pytest.mark.parametrize("provider,score,allowed", [("SIMKL", 7.5, False), ("TRAKT", 0.5, False), ("SIMKL", 8, True), ("CROSSWATCH", 7.5, True), ("SILO", 7, False), ("SILO", 8, True)])
def test_editor_send_checks_target_scale_before_writing(monkeypatch, provider, score, allowed):
    writes = []
    monkeypatch.setattr(editorAPI, "load_config", lambda: {})
    monkeypatch.setattr(editorAPI, "_editor_send_targets", lambda *_: [{"provider": provider, "instance": "default"}])
    monkeypatch.setattr(editorAPI, "_filter_targets_for_request", lambda _cfg, _request, targets: targets)
    monkeypatch.setattr(editorAPI, "load_sync_ops", lambda _: object())
    monkeypatch.setattr(editorAPI, "build_provider_config_view", lambda *_: {})
    monkeypatch.setattr(editorAPI, "remember_sent_items", lambda *_, **__: None)
    monkeypatch.setattr("services.editor_removal.forget_live_reads", lambda *_: None)

    def apply(**kwargs):
        writes.extend(kwargs["items"])
        return {"ok": True, "confirmed": 1}

    monkeypatch.setattr(editorAPI, "apply_add", apply)
    result = editorAPI.api_editor_send({"kind": "ratings", "items": [{"type": "movie", "ids": {"tmdb": "1"}, "rating": score}], "providers": [{"provider": provider}]}, request=None)
    assert result["ok"] is allowed
    if allowed:
        assert writes[0]["rating"] == score
    else:
        assert not writes
        assert result["results"][0]["error"] == "unsupported_rating"


@pytest.mark.parametrize("provider,score,allowed", [("SIMKL", 7.5, False), ("CROSSWATCH", 7.5, True), ("TRAKT", 8, True)])
def test_quick_add_checks_target_scale_before_writing(monkeypatch, provider, score, allowed):
    writes = []
    target = {"provider": provider, "instance": "default", "ratings_enabled": True}
    monkeypatch.setattr("cw_platform.config_base.load_config", lambda: {})
    monkeypatch.setattr(manualAPI, "_manual_history_targets", lambda *_: [target])
    monkeypatch.setattr(manualAPI, "_title_targets", lambda _cfg, rows, _request, **_: rows)
    monkeypatch.setattr(manualAPI, "request_user", lambda _: None)
    monkeypatch.setattr(manualAPI, "build_provider_config_view", lambda *_: {})
    monkeypatch.setattr(manualAPI, "_manual_external_ids", lambda *_: {"tmdb": "1"})
    monkeypatch.setattr(manualAPI, "_remember_sent", lambda *_, **__: None)

    def add(_cfg, items, feature):
        writes.extend(items)
        return {"ok": True}

    monkeypatch.setattr(manualAPI, "load_sync_ops", lambda _: SimpleNamespace(add=add))
    response = manualAPI.api_manual_watched({"item": {"type": "movie", "tmdb": "1", "title": "Movie"}, "providers": [target], "actions": {"rating": True}, "rating": score}, request=None)
    result = json.loads(response.body)
    assert result["ok"] is allowed
    if allowed:
        assert writes[0]["rating"] == score
    else:
        assert not writes
        assert result["results"][0]["rating"]["error"] == "unsupported_rating"


def test_quick_add_preflights_all_ratings_before_any_feature_write(monkeypatch):
    targets = [{"provider": provider, "instance": "default", "ratings_enabled": True,
                "history_enabled": True, "watchlist_enabled": True} for provider in ("CROSSWATCH", "SIMKL")]
    writes = []
    monkeypatch.setattr("cw_platform.config_base.load_config", lambda: {})
    monkeypatch.setattr(manualAPI, "_manual_history_targets", lambda *_: targets)
    monkeypatch.setattr(manualAPI, "_title_targets", lambda _cfg, rows, _request, **_: rows)
    monkeypatch.setattr(manualAPI, "request_user", lambda _: None)
    monkeypatch.setattr(manualAPI, "build_provider_config_view", lambda *_: {})
    monkeypatch.setattr(manualAPI, "_manual_external_ids", lambda *_: {"tmdb": "1"})
    monkeypatch.setattr(manualAPI, "_remember_sent", lambda *_, **__: None)
    monkeypatch.setattr(manualAPI, "load_sync_ops", lambda _: SimpleNamespace(add=lambda *args: writes.append(args)))
    payload = {"item": {"type": "movie", "tmdb": "1", "title": "Movie"}, "providers": targets,
               "actions": {"history": True, "watchlist": True, "rating": True}, "rating": 7.5}
    for _ in range(2):
        result = json.loads(manualAPI.api_manual_watched(payload, request=None).body)
        assert result["error"] == "unsupported_rating"
        assert result["results"][0]["provider"] == "SIMKL"
    assert writes == []


def test_editor_send_keeps_valid_ratings_in_mixed_batch(monkeypatch):
    writes, remembered = [], []
    monkeypatch.setattr(editorAPI, "load_config", lambda: {})
    monkeypatch.setattr(editorAPI, "_editor_send_targets", lambda *_: [{"provider": "SIMKL", "instance": "default"}])
    monkeypatch.setattr(editorAPI, "_filter_targets_for_request", lambda _cfg, _request, targets: targets)
    monkeypatch.setattr(editorAPI, "load_sync_ops", lambda _: object())
    monkeypatch.setattr(editorAPI, "build_provider_config_view", lambda *_: {})
    monkeypatch.setattr(editorAPI, "remember_sent_items", lambda *args, **_: remembered.append(args))
    monkeypatch.setattr("services.editor_removal.forget_live_reads", lambda *_: None)

    def apply(**kwargs):
        writes.extend(kwargs["items"])
        return {"ok": True, "confirmed": len(kwargs["items"])}

    monkeypatch.setattr(editorAPI, "apply_add", apply)
    items = [{"type": "movie", "ids": {"tmdb": str(i)}, "rating": score} for i, score in enumerate([8, 7.5, 9], 1)]
    result = editorAPI.api_editor_send({"kind": "ratings", "items": items, "providers": [{"provider": "SIMKL"}]}, request=None)
    assert not result["ok"]
    assert result["confirmed"] == 2
    assert result["errors"] == 1
    assert [item["rating"] for item in writes] == [8, 9]
    assert [item["rating"] for item in result["results"][0]["rejected"]] == [7.5]
    assert remembered[0][3] == writes
    assert remembered[0][4] == {"ok": True, "confirmed": 2}
