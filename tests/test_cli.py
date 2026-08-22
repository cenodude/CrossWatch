# tests/test_cli.py
# CrossWatch - CLI unit tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cli._app import hoist_globals
from cli._context import Ctx
from cli._errors import ApiError, CLIError, LocalUnsupported, TransportUnavailable
from cli._render import Output, _to_yaml
from cli._transport import Transport
from cli._util import (
    coerce_bool,
    dotted_delete,
    dotted_get,
    dotted_payload,
    find_pair,
    fmt_duration,
    pair_features,
    pair_label,
    parse_value,
    split_path,
    strip_ansi,
)


class FakeTransport(Transport):
    name = "fake"

    def __init__(self, routes: dict[tuple[str, str], Any] | None = None) -> None:
        self.routes = routes or {}
        self.calls: list[tuple[str, str, Any]] = []
        self.param_calls: list[tuple[str, str, dict[str, Any] | None, Any]] = []

    def request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: Any = None) -> Any:
        self.calls.append((method.upper(), path, json_body))
        self.param_calls.append((method.upper(), path, params, json_body))
        key = (method.upper(), path)
        if key not in self.routes:
            raise ApiError(404, {"error": "not found"}, method=method, path=path)
        value = self.routes[key]
        if isinstance(value, Exception):
            raise value
        return value


def _ctx(http: Transport | None = None, local: Transport | None = None, *, force_local: bool = False) -> Ctx:
    state = Ctx(url="http://cw.test", force_local=force_local, out=Output("json"))
    state._http = http  # type: ignore[assignment]
    state._local = local  # type: ignore[assignment]
    return state


def test_split_path_handles_dots_slashes_and_indexes() -> None:
    assert split_path("sync.anime.enabled") == ["sync", "anime", "enabled"]
    assert split_path("sync/anime/enabled") == ["sync", "anime", "enabled"]
    assert split_path("pairs[0].features.history") == ["pairs", "[0]", "features", "history"]
    with pytest.raises(CLIError):
        split_path("   ")


def test_dotted_get_walks_dicts_and_lists() -> None:
    data = {"a": {"b": [{"c": 1}]}}
    assert dotted_get(data, "a.b[0].c") == 1
    assert dotted_get(data, "a.b[9].c", "fallback") == "fallback"
    assert dotted_get(data, "a.missing", None) is None


def test_dotted_payload_builds_a_merge_patch() -> None:
    assert dotted_payload("sync.anime.enabled", True) == {"sync": {"anime": {"enabled": True}}}
    with pytest.raises(CLIError):
        dotted_payload("pairs[0].enabled", True)


def test_dotted_delete_removes_only_what_exists() -> None:
    data = {"a": {"b": 1, "c": 2}}
    assert dotted_delete(data, "a.b") is True
    assert data == {"a": {"c": 2}}
    assert dotted_delete(data, "a.nope") is False
    assert dotted_delete(data, "nope.deeper") is False


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("yes", True),
        ("off", False),
        ("42", 42),
        ("-7", -7),
        ("3.5", 3.5),
        ("null", None),
        ("plain text", "plain text"),
        ("", ""),
    ],
)
def test_parse_value_detects_scalars(raw: str, expected: Any) -> None:
    assert parse_value(raw) == expected


def test_parse_value_json_mode() -> None:
    assert parse_value('["a","b"]', as_json=True) == ["a", "b"]
    with pytest.raises(CLIError):
        parse_value("{not json}", as_json=True)


def test_coerce_bool_defaults() -> None:
    assert coerce_bool(None, True) is True
    assert coerce_bool("ON") is True
    assert coerce_bool("nope") is False


def test_fmt_duration_scales() -> None:
    assert fmt_duration(45) == "45s"
    assert fmt_duration(90) == "1m 30s"
    assert fmt_duration(3600) == "1h"
    assert fmt_duration(90000) == "1d 1h"


def test_pair_helpers() -> None:
    pair = {
        "id": "abc123",
        "source": "plex",
        "target": "trakt",
        "mode": "two-way",
        "features": {"watchlist": {"enable": True}, "ratings": {"enable": False}, "history": True},
    }
    assert pair_label(pair) == "PLEX <-> TRAKT"
    assert pair_features(pair) == ["history", "watchlist"]


def test_find_pair_matches_exact_prefix_and_label() -> None:
    pairs = [
        {"id": "abc123", "source": "plex", "target": "trakt"},
        {"id": "def456", "source": "simkl", "target": "trakt"},
    ]
    assert find_pair(pairs, "abc123")["id"] == "abc123"
    assert find_pair(pairs, "def")["id"] == "def456"
    assert find_pair(pairs, "PLEX -> TRAKT")["id"] == "abc123"
    with pytest.raises(CLIError):
        find_pair(pairs, "nothing")


def test_find_pair_rejects_ambiguous_prefix() -> None:
    pairs = [{"id": "abc1", "source": "a", "target": "b"}, {"id": "abc2", "source": "c", "target": "d"}]
    with pytest.raises(CLIError) as err:
        find_pair(pairs, "abc")
    assert "ambiguous" in err.value.message


def test_hoist_globals_moves_flags_in_front_of_the_command() -> None:
    assert hoist_globals(["pair", "list", "-o", "json"]) == ["-o", "json", "pair", "list"]
    assert hoist_globals(["sync", "run", "--local", "--pair", "x"]) == ["--local", "sync", "run", "--pair", "x"]
    assert hoist_globals(["--output=json", "status"]) == ["--output=json", "status"]


def test_hoist_globals_leaves_subcommand_timeout_alone() -> None:
    assert hoist_globals(["sync", "run", "--timeout", "30"]) == ["sync", "run", "--timeout", "30"]
    assert hoist_globals(["-U", "http://x", "sync", "run", "--timeout", "30"]) == [
        "-U",
        "http://x",
        "sync",
        "run",
        "--timeout",
        "30",
    ]


def test_hoist_globals_stops_at_double_dash() -> None:
    assert hoist_globals(["logs", "tail", "--", "-o", "json"]) == ["logs", "tail", "--", "-o", "json"]


def test_api_error_maps_status_to_exit_code() -> None:
    assert ApiError(401, {}).exit_code == 4
    assert ApiError(403, {}).exit_code == 4
    assert ApiError(404, {}).exit_code == 5
    assert ApiError(409, {}).exit_code == 6
    assert ApiError(500, {}).exit_code == 1


def test_api_error_uses_server_detail() -> None:
    err = ApiError(400, {"error": "Sync already running"}, method="POST", path="/api/run")
    assert "Sync already running" in err.message
    assert "POST /api/run" in err.message


def test_context_falls_back_to_local_when_service_is_down() -> None:
    http = FakeTransport({("GET", "/api/status"): TransportUnavailable("down")})
    local = FakeTransport({("GET", "/api/status"): {"ok": True, "mode": "local"}})
    state = _ctx(http, local)

    assert state.get("/api/status") == {"ok": True, "mode": "local"}
    assert state.mode == "local (fallback)"


def test_context_reports_unreachable_when_local_cannot_serve_it() -> None:
    http = FakeTransport({("POST", "/api/run"): TransportUnavailable("Cannot reach CrossWatch at http://cw.test")})
    local = FakeTransport({("POST", "/api/run"): LocalUnsupported("Starting a sync")})
    state = _ctx(http, local)

    with pytest.raises(TransportUnavailable) as err:
        state.post("/api/run")
    assert "Cannot reach CrossWatch" in err.value.message
    assert err.value.exit_code == 3


def test_context_does_not_fall_back_on_auth_errors() -> None:
    http = FakeTransport({("GET", "/api/status"): ApiError(401, {"error": "Unauthorized"})})
    local = FakeTransport({("GET", "/api/status"): {"ok": True}})
    state = _ctx(http, local)

    with pytest.raises(ApiError) as err:
        state.get("/api/status")
    assert err.value.status == 401


def test_force_local_never_touches_http() -> None:
    http = FakeTransport({("GET", "/api/status"): {"from": "http"}})
    local = FakeTransport({("GET", "/api/status"): {"from": "local"}})
    state = _ctx(http, local, force_local=True)

    assert state.get("/api/status") == {"from": "local"}
    assert http.calls == []
    with pytest.raises(LocalUnsupported):
        state.require_service("Starting a sync")


def test_strip_ansi_removes_colour_codes() -> None:
    assert strip_ansi("\x1b[92m[TRAKT]\x1b[0m done") == "[TRAKT] done"
    assert strip_ansi('[WATCH] <span class="c94">INFO</span> route started') == "[WATCH] INFO route started"


def test_yaml_rendering_round_trips_simple_structures() -> None:
    rendered = _to_yaml({"enabled": True, "count": 3, "name": "sync", "empty": None, "items": ["a", "b"]})
    assert "enabled: true" in rendered
    assert "count: 3" in rendered
    assert "empty: null" in rendered
    assert "- a" in rendered


def test_local_transport_serves_config_and_pairs(config_base: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    from cw_platform import config_base as cfg_base

    importlib.reload(cfg_base)
    (config_base / "config.json").write_text(
        json.dumps({"pairs": [{"id": "p1", "source": "plex", "target": "trakt", "enabled": True}]}),
        encoding="utf-8",
    )

    from cli import _local

    importlib.reload(_local)
    transport = _local.LocalTransport()

    pairs = transport.request("GET", "/api/pairs")
    assert [p["id"] for p in pairs] == ["p1"]

    assert transport.request("PUT", "/api/pairs/p1", json_body={"enabled": False})["ok"] is True
    assert transport.request("GET", "/api/pairs")[0]["enabled"] is False

    assert transport.request("PUT", "/api/pairs/nope", json_body={"enabled": False})["ok"] is False

    with pytest.raises(LocalUnsupported):
        transport.request("POST", "/api/run")


def test_local_transport_config_set_deep_merges(config_base: Path) -> None:
    import importlib

    from cw_platform import config_base as cfg_base

    importlib.reload(cfg_base)
    (config_base / "config.json").write_text(json.dumps({"sync": {"keep": 1}}), encoding="utf-8")

    from cli import _local

    importlib.reload(_local)
    transport = _local.LocalTransport()

    transport.request("POST", "/api/config", json_body={"sync": {"anime": {"enabled": True}}})
    cfg = transport.request("GET", "/api/config")

    assert cfg["sync"]["keep"] == 1
    assert cfg["sync"]["anime"]["enabled"] is True


def test_local_transport_config_unset_removes_keys(config_base: Path) -> None:
    import importlib

    from cw_platform import config_base as cfg_base

    importlib.reload(cfg_base)
    (config_base / "config.json").write_text(
        json.dumps({"sync": {"keep": 1, "drop": 2}, "pairs": [{"id": "a"}, {"id": "b"}]}),
        encoding="utf-8",
    )

    from cli import _local

    importlib.reload(_local)
    transport = _local.LocalTransport()

    assert transport.request("POST", "/api/config/unset", json_body={"paths": ["sync.drop"]})["ok"] is True
    cfg = transport.request("GET", "/api/config")
    assert cfg["sync"]["keep"] == 1
    assert "drop" not in cfg["sync"]

    missing = transport.request("POST", "/api/config/unset", json_body={"paths": ["sync.gone"]})
    assert missing["ok"] is False
    assert missing["error"] == "not_found"

    protected = transport.request("POST", "/api/config/unset", json_body={"paths": ["app_auth.enabled"]})
    assert protected["ok"] is False
    assert protected["error"] == "protected_path"


def test_config_path_helpers_match_the_cli_parser() -> None:
    from api.configAPI import _delete_config_path, _split_config_path

    assert _split_config_path("sync.anime.enabled") == split_path("sync.anime.enabled")
    assert _split_config_path("pairs[0].features") == split_path("pairs[0].features")

    data = {"pairs": [{"id": "a"}, {"id": "b"}], "sync": {"x": 1}}
    assert _delete_config_path(data, _split_config_path("pairs[0]")) is True
    assert [p["id"] for p in data["pairs"]] == ["b"]
    assert _delete_config_path(data, _split_config_path("sync.missing")) is False
    assert _delete_config_path(data, _split_config_path("pairs[9]")) is False


def test_log_control_lines_are_filtered() -> None:
    from cli._util import is_log_control

    assert is_log_control("::CLEAR::") is True
    assert is_log_control("  ::CLEAR::  ") is True
    assert is_log_control("[SYNC] exit code: 0") is False


def test_entry_point_is_lowercase_cw_py() -> None:
    import os

    cli_dir = Path(__file__).resolve().parents[1] / "cli"
    names = os.listdir(cli_dir)

    assert "cw.py" in names
    assert "CW.py" not in names


def test_entry_point_exposes_app_and_main() -> None:
    from cli import cw

    assert callable(cw.main)
    assert cw.app is not None


def test_cli_home_uses_runtime_dir_in_the_container(monkeypatch: pytest.MonkeyPatch) -> None:
    from cli._settings import cli_home

    monkeypatch.delenv("CW_CLI_HOME", raising=False)
    monkeypatch.setenv("RUNTIME_DIR", "/config")

    assert cli_home() == Path("/config/.cw_cli")


def test_cli_home_env_override_still_wins(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from cli._settings import cli_home

    custom = tmp_path / "custom-cli"
    monkeypatch.setenv("CW_CLI_HOME", str(custom))
    monkeypatch.setenv("RUNTIME_DIR", "/config")

    assert cli_home() == custom


def test_entry_point_is_directly_executable_in_the_container() -> None:
    entrypoint = (Path(__file__).resolve().parents[1] / "cli" / "cw.py").read_text(encoding="utf-8").splitlines()

    assert entrypoint[0] == "#!/usr/bin/env python3"


def test_container_wrapper_points_at_the_entry_point() -> None:
    wrapper = (Path(__file__).resolve().parents[1] / "docker" / "cw").read_text(encoding="utf-8")

    assert 'APP_DIR="${APP_DIR:-/app}"' in wrapper
    assert 'CW_CLI_HOME="${CW_CLI_HOME:-${RUNTIME_DIR:-/config}/.cw_cli}"' in wrapper
    assert "/cli/cw.py" in wrapper
    assert "CW.py" not in wrapper


def test_container_installs_cw_command_on_the_path() -> None:
    dockerfile = (Path(__file__).resolve().parents[1] / "Dockerfile").read_text(encoding="utf-8")

    assert "COPY docker/cw            /usr/local/bin/cw" in dockerfile
    assert "chmod +x" in dockerfile
    assert "/usr/local/bin/cw" in dockerfile


def test_new_modules_carry_the_project_header() -> None:
    root = Path(__file__).resolve().parents[1]
    modules = sorted((root / "cli").rglob("*.py")) + [root / "api" / "apiTokensAPI.py"]

    assert modules
    for module in modules:
        lines = module.read_text(encoding="utf-8").splitlines()
        if lines and lines[0].startswith("#!"):
            lines = lines[1:]
        lines = lines[:3]
        relative = module.relative_to(root).as_posix()
        assert lines[0] == f"# /{relative}", relative
        assert lines[1].startswith("# CrossWatch - "), relative
        assert lines[2].startswith("# Copyright (c) 2025-2026 CrossWatch / Cenodude"), relative


@pytest.fixture()
def clean_cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CW_CLI_HOME", str(tmp_path / "cli-home"))
    for name in ("CW_URL", "CROSSWATCH_URL", "CW_TOKEN", "CROSSWATCH_TOKEN"):
        monkeypatch.delenv(name, raising=False)


def test_build_records_the_options_it_was_given() -> None:
    state = Ctx.build(url="http://cw.test", output="json", local=True, quiet=True)

    assert state.options["url"] == "http://cw.test"
    assert state.options["local"] is True
    assert state.options["output"] == "json"
    assert state.options["quiet"] is True


def test_dispatch_inherits_session_globals(clean_cli_env, capsys: pytest.CaptureFixture[str]) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://inherited.test", output="json", local=True)
    assert run_isolated(state, ["config", "path"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["endpoint"] == "http://inherited.test"


def test_dispatch_lets_a_flag_override_just_that_command(clean_cli_env, capsys: pytest.CaptureFixture[str]) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://inherited.test", output="json", local=True)

    assert run_isolated(state, ["-U", "http://override.test", "config", "path"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["endpoint"] == "http://override.test"

    assert state.url == "http://inherited.test"
    assert state.options["local"] is True

    assert run_isolated(state, ["config", "path"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["endpoint"] == "http://inherited.test"


def test_dispatch_without_flags_reuses_the_same_context(clean_cli_env) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://inherited.test", output="json", local=True)
    state._fell_back = True

    assert run_isolated(state, ["config", "path"]) == 0
    assert state._fell_back is True


def test_analyzer_problems_prints_titles_for_missing_items(
    clean_cli_env,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="plain")
    state._http = FakeTransport(
        {
            (
                "GET",
                "/api/analyzer/problems",
            ): {
                "problems": [
                    {
                        "severity": "warn",
                        "type": "missing_peer",
                        "provider": "NUVIO",
                        "feature": "history",
                        "key": "tmdb:329491",
                        "title": "Episode 1",
                        "series_title": "DAHMER - Monster: The Jeffrey Dahmer Story",
                        "item_type": "episode",
                        "season": 1,
                        "episode": 1,
                        "targets": ["FLOPPY"],
                    }
                ]
            },
        }
    )

    assert run_isolated(state, ["analyzer", "problems"]) == 0
    out = capsys.readouterr().out

    assert "DAHMER - Monster: The Jeffrey Dahmer Story - S01E01" in out
    assert "Missing at FLOPPY" in out


def test_watcher_now_reads_currently_watching_payload(
    clean_cli_env,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="plain")
    state._http = FakeTransport(
        {
            (
                "GET",
                "/api/watch/currently_watching",
            ): {
                "ok": True,
                "currently_watching": {
                    "title": "Heat",
                    "media_type": "movie",
                    "source": "PLEX",
                    "account": "Pascal",
                    "progress_percent": 42,
                    "state": "playing",
                },
                "streams_count": 1,
            },
        }
    )

    assert run_isolated(state, ["watcher", "now"]) == 0
    out = capsys.readouterr().out

    assert "Heat" in out
    assert "PLEX" in out
    assert "42%" in out


def test_watcher_logs_uses_finite_watch_log_endpoint(
    clean_cli_env,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="plain")
    state._http = FakeTransport(
        {
            ("GET", "/api/watch/logs"): {
                "tags": ["WATCH"],
                "tail": 20,
                "lines": ['[WATCH] <span class="c94">INFO</span> route started'],
            },
        }
    )

    assert run_isolated(state, ["watcher", "logs", "--lines", "20"]) == 0
    out = capsys.readouterr().out
    assert "[WATCH] INFO route started" in out
    assert "<span" not in out
    assert state._http.calls == [("GET", "/api/watch/logs", None)]
    assert state._http.param_calls == [
        ("GET", "/api/watch/logs", {"tail": 20, "tags": "WATCH,WATCHM"}, None)
    ]


def _manifest(name: str, flow: str, fields: list[dict[str, Any]] | None = None):
    from cli.commands.auth import Manifest

    return Manifest(name=name, label=name.title(), flow=flow, fields=fields or [])


def test_every_manifest_provider_has_cli_endpoints() -> None:
    from cli.commands.auth import ENDPOINTS
    from providers.auth.registry import auth_providers_manifests

    shipped = {str(m.get("name") or "").upper() for m in auth_providers_manifests()}

    assert shipped, "no auth providers discovered"
    missing = sorted(shipped - set(ENDPOINTS))
    assert missing == [], f"providers with no CLI endpoints: {missing}"

    stale = sorted(set(ENDPOINTS) - shipped)
    assert stale == [], f"CLI endpoints for providers that no longer exist: {stale}"


def test_every_provider_is_loginable() -> None:
    from cli.commands.auth import ENDPOINTS

    for name, endpoints in ENDPOINTS.items():
        assert endpoints.submit or endpoints.start, f"{name} has no way to log in"
        assert endpoints.disconnect, f"{name} has no way to log out"


def test_field_body_strips_the_provider_prefix() -> None:
    from cli.commands.auth import _nest

    values = {"jellyfin.server": "http://jf", "jellyfin.username": "bob"}
    assert _nest(values, drop_prefix=True) == {"server": "http://jf", "username": "bob"}


def test_field_body_keeps_nested_keys() -> None:
    from cli.commands.auth import _nest

    values = {"tautulli.server_url": "http://t", "tautulli.history.user_id": "7"}
    assert _nest(values, drop_prefix=True) == {"server_url": "http://t", "history": {"user_id": "7"}}


def test_config_body_keeps_the_provider_prefix() -> None:
    from cli.commands.auth import _nest

    values = {"simkl.client_id": "abc", "simkl.client_secret": "xyz"}
    assert _nest(values, drop_prefix=False) == {"simkl": {"client_id": "abc", "client_secret": "xyz"}}


def test_parse_field_args_requires_key_equals_value() -> None:
    from cli.commands.auth import _parse_field_args

    assert _parse_field_args(["a=1", "b=with=equals"]) == {"a": "1", "b": "with=equals"}
    assert _parse_field_args(["a="]) == {"a": ""}
    with pytest.raises(CLIError):
        _parse_field_args(["nope"])


def test_collect_fields_accepts_long_and_short_keys() -> None:
    from cli.commands.auth import _collect_fields

    manifest = _manifest(
        "JELLYFIN",
        "token",
        [
            {"key": "jellyfin.server", "label": "Server", "type": "text", "required": True},
            {"key": "jellyfin.username", "label": "User", "type": "text"},
        ],
    )
    values = _collect_fields(manifest, {"jellyfin.server": "http://jf", "username": "bob"}, interactive=False)
    assert values == {"jellyfin.server": "http://jf", "jellyfin.username": "bob"}


def test_collect_fields_coerces_bools() -> None:
    from cli.commands.auth import _collect_fields

    manifest = _manifest("FLOPPY", "api_key", [{"key": "floppy.verify_ssl", "label": "Verify", "type": "bool"}])
    assert _collect_fields(manifest, {"floppy.verify_ssl": "no"}, interactive=False) == {"floppy.verify_ssl": False}


def test_collect_fields_rejects_unknown_fields_first() -> None:
    from cli.commands.auth import _collect_fields

    manifest = _manifest("JELLYFIN", "token", [{"key": "jellyfin.server", "label": "Server", "required": True}])
    with pytest.raises(CLIError) as err:
        _collect_fields(manifest, {"bogus": "x"}, interactive=False)
    assert "no field named" in err.value.message


def test_collect_fields_requires_required_fields_when_not_prompting() -> None:
    from cli.commands.auth import _collect_fields

    manifest = _manifest("TAUTULLI", "api_keys", [{"key": "tautulli.server_url", "label": "Server", "required": True}])
    with pytest.raises(CLIError) as err:
        _collect_fields(manifest, {}, interactive=False)
    assert err.value.exit_code == 2
    assert "--field tautulli.server_url=" in err.value.hint


def test_collect_fields_skips_optional_fields_when_not_prompting() -> None:
    from cli.commands.auth import _collect_fields

    manifest = _manifest("MDBLIST", "device_code", [{"key": "mdblist.api_key", "label": "Key"}])
    assert _collect_fields(manifest, {}, interactive=False) == {}


def test_every_command_group_is_registered() -> None:
    from cli._app import describe_commands

    names = {name for name, _ in describe_commands()}
    expected = {
        "status", "version", "health", "shell", "insights", "stats",
        "pair", "sync", "config", "auth", "watcher", "scheduler", "logs",
        "analyzer", "events", "capture", "backup", "watchlist", "progress",
        "editor", "playlist", "export", "import", "metadata", "manual",
        "anime", "instance", "user-profile", "scrobbler", "activity", "maintenance",
    }
    assert expected <= names, f"missing groups: {sorted(expected - names)}"


def test_shell_knows_every_group() -> None:
    from cli._app import describe_commands, describe_group
    from cli.commands.shell import GROUPS

    groups = {name for name, _ in describe_commands() if describe_group(name)}
    assert set(GROUPS) <= groups, f"shell lists unknown groups: {sorted(set(GROUPS) - groups)}"
    assert groups <= set(GROUPS), f"shell is missing groups: {sorted(groups - set(GROUPS))}"


def test_error_text_prefers_the_server_message() -> None:
    from cli._util import error_text

    assert error_text({"message": "real reason", "error": "code"}) == "real reason"
    assert error_text({"error": "code"}) == "code"
    assert error_text({"found": False}) == "not found"
    assert error_text({}, "fallback") == "fallback"
    assert error_text(None, "fallback") == "fallback"


def test_api_error_prefers_the_server_message() -> None:
    err = ApiError(400, {"error": "invalid_rule", "message": "match_provider must be one of: tvdb"})
    assert "match_provider must be one of" in err.message


def test_auth_field_nesting_round_trip() -> None:
    from cli.commands.auth import _nest

    assert _nest({"a.b.c": 1}, drop_prefix=True) == {"b": {"c": 1}}
    assert _nest({"a.b.c": 1}, drop_prefix=False) == {"a": {"b": {"c": 1}}}
    assert _nest({"solo": 1}, drop_prefix=True) == {}
