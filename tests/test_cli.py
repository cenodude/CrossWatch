# tests/test_cli.py
# CrossWatch - CLI unit tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from types import SimpleNamespace
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
        if callable(value):
            return value(method.upper(), path, params, json_body)
        return value


def _ctx(http: Transport | None = None, local: Transport | None = None, *, force_local: bool = False) -> Ctx:
    state = Ctx(url="http://cw.test", force_local=force_local, out=Output("json"))
    state._http = http  # type: ignore[assignment]
    state._local = local  # type: ignore[assignment]
    return state


@pytest.mark.parametrize("payload", [b"PK\x03\x04\xff\x00\x80", "imdb_id,type\ntt0068646,movie\n"])
def test_export_file_preserves_download_bytes(tmp_path, capsys, payload):
    from cli.commands.transfer import export_file

    http = FakeTransport({("GET", "/api/export/file"): payload})
    state = _ctx(http=http)
    target = tmp_path / "export.bin"
    export_file(SimpleNamespace(obj=state), output=target, provider="CROSSWATCH", feature="history",
                export_format="trakt", media_types="movie", instance="default")
    expected = payload if isinstance(payload, bytes) else payload.encode("utf-8")
    assert target.read_bytes() == expected
    assert json.loads(capsys.readouterr().out)["bytes"] == len(expected)
    assert http.param_calls[0][2]["format"] == "trakt"


@pytest.mark.parametrize("item_key,path", [
    ("tmdb:121#show", "/api/events/item/tmdb%3A121%23show"),
    ("tmdb:121#s01e02", "/api/events/item/tmdb%3A121%23s01e02"),
    ("tmdb:121", "/api/events/item/tmdb%3A121"),
])
def test_events_item_encodes_key_in_path(capsys, item_key, path):
    from cli.commands.events import events_item

    http = FakeTransport({("GET", path): {"events": []}})
    events_item(SimpleNamespace(obj=_ctx(http=http)), item_key=item_key, limit=5)
    assert http.param_calls == [("GET", path, {"limit": 5}, None)]


def test_http_transport_returns_zip_without_decoding(monkeypatch):
    from cli._transport import HttpTransport

    response = SimpleNamespace(status_code=200, headers={"content-type": "application/zip"}, content=b"PK\xff\x00")
    transport = HttpTransport("http://cw.test")
    monkeypatch.setattr(transport.session, "request", lambda *_args, **_kwargs: response)
    try:
        assert transport.get("/api/export/file") == response.content
    finally:
        transport.close()


def test_export_preview_shows_import_instructions_before_rows():
    from cli.commands.transfer import export_preview

    payload = {"warnings": ["Extract the ZIP and import your watchlist last."], "items": [{"title": "Movie"}]}
    state = _ctx(http=FakeTransport({("GET", "/api/export/sample"): payload}))
    warnings = []
    records = []
    state.out = SimpleNamespace(json_mode=False, warn=warnings.append, records=lambda rows, *_args, **_kwargs: records.extend(rows))
    export_preview(SimpleNamespace(obj=state), provider="CROSSWATCH", feature="history",
                   export_format="trakt", media_types="movie", instance="default")
    assert warnings == payload["warnings"]
    assert records == payload["items"]


def test_split_path_handles_dots_slashes_and_indexes() -> None:
    assert split_path("sync.anime.enabled") == ["sync", "anime", "enabled"]
    assert split_path("sync/anime/enabled") == ["sync", "anime", "enabled"]
    assert split_path("pairs[0].features.history") == ["pairs", "[0]", "features", "history"]
    with pytest.raises(CLIError):
        split_path("   ")


def test_setup_required_error_points_to_local_auth_setup() -> None:
    err = ApiError(403, {"error": "Authentication setup required"}, method="GET", path="/api/editor")

    assert "Authentication setup required" in err.message
    assert "cw --local auth setup --username admin" in err.hint


def test_unauthorized_error_explains_local_token_create_saves_by_default() -> None:
    err = ApiError(401, {"error": "Unauthorized"}, method="GET", path="/api/editor")

    assert "cw --local auth token create" in err.hint
    assert "saves the token by default" in err.hint
    assert "CW_TOKEN" in err.hint


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


def test_find_pair_matches_one_based_index_after_exact_ids() -> None:
    pairs = [
        {"id": "1", "source": "plex", "target": "trakt"},
        {"id": "def456", "source": "simkl", "target": "trakt"},
    ]

    assert find_pair(pairs, "1")["id"] == "1"
    assert find_pair(pairs, "2")["id"] == "def456"


def test_find_pair_rejects_ambiguous_prefix() -> None:
    pairs = [{"id": "abc1", "source": "a", "target": "b"}, {"id": "abc2", "source": "c", "target": "d"}]
    with pytest.raises(CLIError) as err:
        find_pair(pairs, "abc")
    assert "ambiguous" in err.value.message


def test_scheduler_pair_selector_accepts_comma_separated_values() -> None:
    from cli.commands.scheduler import _resolve_pair_selectors

    pairs = [
        {"id": "pair_a", "source": "plex", "target": "simkl"},
        {"id": "pair_b", "source": "plex", "target": "mdblist"},
        {"id": "pair_c", "source": "plex", "target": "trakt"},
    ]

    resolved = _resolve_pair_selectors(pairs, "1, pair_c, 1")

    assert [pair["id"] for pair in resolved] == ["pair_a", "pair_c"]


def test_scheduler_append_pair_jobs_creates_jobs_for_each_pair_and_time() -> None:
    from cli.commands.scheduler import _append_pair_jobs

    scfg = {"advanced": {"jobs": [{"id": "pair_a_0330", "pair_id": "pair_a"}]}}
    pairs = [{"id": "pair_a"}, {"id": "pair_b"}]

    created = _append_pair_jobs(scfg, pairs, ["03:30", "12:00"], days="weekdays")

    assert [job["id"] for job in created] == ["pair_a_0330_2", "pair_a_1200", "pair_b_0330", "pair_b_1200"]
    assert [job["pair_id"] for job in created] == ["pair_a", "pair_a", "pair_b", "pair_b"]
    assert all(job["days"] == [1, 2, 3, 4, 5] for job in created)
    assert scfg["advanced"]["enabled"] is True


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


def test_status_reports_unknown_for_state_local_mode_cannot_see() -> None:
    from cli.commands.status import _collect

    local = FakeTransport(
        {
            ("GET", "/api/status"): {"ok": True, "mode": "local", "degraded": True},
            ("GET", "/api/pairs"): [{"id": "p1", "enabled": True}],
            ("GET", "/api/scheduling/status"): {"ok": True, "running": None, "config": {"enabled": True}, "next_run_at": 0},
            ("GET", "/api/watch/status"): LocalUnsupported("'GET /api/watch/status'"),
            ("GET", "/api/run/cancel"): LocalUnsupported("'GET /api/run/cancel'"),
        }
    )
    data = _collect(_ctx(FakeTransport(), local, force_local=True))

    assert data["watcher_alive"] is None
    assert data["scheduler_running"] is None
    assert data["sync_running"] is None
    assert data["cancel_requested"] is None
    assert data["can_run"] is None
    assert data["scheduler_enabled"] is True
    assert data["pairs_enabled"] == 1


def test_status_keeps_real_booleans_from_the_service() -> None:
    from cli.commands.status import _collect

    http = FakeTransport(
        {
            ("GET", "/api/status"): {"can_run": True},
            ("GET", "/api/scheduling/status"): {"running": True, "config": {"enabled": True}},
            ("GET", "/api/watch/status"): {"alive": False, "groups": [], "sinks": []},
            ("GET", "/api/run/cancel"): {"running": False, "cancel_requested": False},
        }
    )
    data = _collect(_ctx(http, FakeTransport()))

    assert data["watcher_alive"] is False
    assert data["scheduler_running"] is True
    assert data["sync_running"] is False
    assert data["cancel_requested"] is False
    assert data["can_run"] is True


def test_local_scheduling_status_does_not_claim_the_worker_is_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    from cli._local import LocalTransport

    transport = LocalTransport()
    monkeypatch.setattr(transport, "_scheduling_get", lambda **_: {"enabled": False})

    assert transport._scheduling_status()["running"] is None


def test_state_text_renders_unknown_only_when_asked() -> None:
    from cli._render import state_text

    assert state_text(None, on="running", off="stopped", unknown="unknown").plain == "unknown"
    assert state_text(None, on="running", off="stopped").plain == "stopped"
    assert state_text(False, on="running", off="stopped", unknown="unknown").plain == "stopped"


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


def test_local_transport_can_setup_app_auth(config_base: Path) -> None:
    import importlib

    from cw_platform import config_base as cfg_base

    importlib.reload(cfg_base)
    (config_base / "config.json").write_text(
        json.dumps(
            {
                "app_auth": {
                    "enabled": False,
                    "username": "",
                    "password": {"scheme": "pbkdf2_sha256", "iterations": 260_000, "salt": "", "hash": ""},
                    "api_tokens": [{"id": "legacy1", "token_hash": {"scheme": "pbkdf2_sha256", "iterations": 1, "salt": "a", "hash": "b"}}],
                }
            }
        ),
        encoding="utf-8",
    )

    from cli import _local

    importlib.reload(_local)
    transport = _local.LocalTransport()

    result = transport.request(
        "POST",
        "/api/app-auth/credentials",
        json_body={"enabled": True, "username": "admin", "password": "secrett1"},
    )
    cfg = cfg_base.load_config()

    assert result["ok"] is True
    assert cfg["app_auth"]["enabled"] is True
    assert cfg["app_auth"]["username"] == "admin"
    assert cfg["app_auth"]["password"]["hash"]
    assert cfg["app_auth"]["api_tokens"] == []


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


def test_sync_list_prints_pair_order_source_target_and_features(
    clean_cli_env,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="plain")
    state._http = FakeTransport(
        {
            (
                "GET",
                "/api/pairs",
            ): [
                {
                    "id": "plex-trakt",
                    "source": "plex",
                    "target": "trakt",
                    "source_instance": "kids",
                    "mode": "one-way",
                    "enabled": True,
                    "features": {"watchlist": {"enable": True}, "ratings": {"enable": False}, "history": True},
                },
                {
                    "id": "simkl-trakt",
                    "source": "simkl",
                    "target": "trakt",
                    "mode": "two-way",
                    "enabled": False,
                    "features": {"progress": {"enable": True}},
                },
            ],
        }
    )

    assert run_isolated(state, ["sync", "list"]) == 0
    out = capsys.readouterr().out

    assert "1\tplex-trakt\tPLEX:kids\tTRAKT\tone-way" in out
    assert "history, watchlist" in out
    assert "2\tsimkl-trakt\tSIMKL\tTRAKT\ttwo-way" in out
    assert "progress" in out


def test_sync_run_without_selector_posts_all_pairs(clean_cli_env) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="plain")
    state._http = FakeTransport({("POST", "/api/run"): {"ok": True, "run_id": "run-1"}})

    assert run_isolated(state, ["sync", "run"]) == 0
    assert state._http.calls == [("POST", "/api/run", {"source": "cli"})]


def test_sync_run_positional_pair_index_posts_pair_id(clean_cli_env) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="plain")
    state._http = FakeTransport(
        {
            (
                "GET",
                "/api/pairs",
            ): [
                {"id": "plex-trakt", "source": "plex", "target": "trakt"},
                {"id": "simkl-trakt", "source": "simkl", "target": "trakt"},
            ],
            ("POST", "/api/run"): {"ok": True, "run_id": "run-2"},
        }
    )

    assert run_isolated(state, ["sync", "run", "2"]) == 0
    assert state._http.calls == [
        ("GET", "/api/pairs", None),
        ("POST", "/api/run", {"source": "cli", "pair_id": "simkl-trakt"}),
    ]


def test_shell_sync_group_uses_new_list_and_run_selector(clean_cli_env) -> None:
    from cli.commands.shell import Shell

    state = Ctx.build(url="http://cw.test", output="plain")
    state._http = FakeTransport(
        {
            (
                "GET",
                "/api/pairs",
            ): [
                {"id": "plex-trakt", "source": "plex", "target": "trakt", "features": {"watchlist": True}},
                {"id": "simkl-trakt", "source": "simkl", "target": "trakt", "features": {"history": True}},
            ],
            ("POST", "/api/run"): {"ok": True, "run_id": "run-shell"},
        }
    )
    shell = Shell(state)

    assert shell.handle("sync") is True
    assert shell.group == "sync"
    assert shell.handle("list") is True
    assert shell.handle("run 2") is True

    assert state._http.calls == [
        ("GET", "/api/pairs", None),
        ("GET", "/api/pairs", None),
        ("POST", "/api/run", {"source": "cli", "pair_id": "simkl-trakt"}),
    ]


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


def test_editor_list_reads_items_dict_payload(
    clean_cli_env,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="plain")
    state._http = FakeTransport(
        {
            ("GET", "/api/editor"): {
                "kind": "watchlist",
                "source": "state",
                "provider": "PLEX",
                "provider_instance": "default",
                "count": 1,
                "items": {
                    "tmdb:454639": {
                        "type": "movie",
                        "title": "Masters of the Universe",
                        "ids": {"tmdb": 454639},
                    }
                },
            },
        }
    )

    assert run_isolated(state, ["editor", "list", "--provider", "plex", "--profile", "default"]) == 0
    out = capsys.readouterr().out

    assert "PLEX" in out
    assert "tmdb:454639" in out
    assert "Masters of the Universe" in out
    assert "454639" in out
    assert state._http.param_calls == [
        (
            "GET",
            "/api/editor",
            {"kind": "watchlist", "source": "state", "provider": "PLEX", "provider_instance": "default"},
            None,
        )
    ]


def test_editor_sources_prints_provider_names_from_string_payload(
    clean_cli_env,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="plain")
    state._http = FakeTransport(
        {
            ("GET", "/api/editor/state/providers"): {"providers": ["PLEX", "TRAKT"]},
            ("GET", "/api/editor"): {
                "kind": "watchlist",
                "source": "state",
                "provider_instance": "default",
                "count": 3,
                "items": {},
            },
        }
    )

    assert run_isolated(state, ["editor", "sources"]) == 0
    out = capsys.readouterr().out

    assert "PLEX" in out
    assert "TRAKT" in out
    assert "3" in out
    assert "-         -         -" not in out


def test_editor_send_loads_selected_items_before_posting(
    clean_cli_env,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="plain")
    state._http = FakeTransport(
        {
            ("GET", "/api/editor"): {
                "items": {
                    "tmdb:454639": {
                        "type": "movie",
                        "title": "Masters of the Universe",
                        "ids": {"tmdb": 454639},
                    }
                },
            },
            ("POST", "/api/editor/send"): {"ok": True, "sent": 1},
        }
    )

    assert run_isolated(
        state,
        ["editor", "send", "tmdb:454639", "--provider", "trakt", "--source-provider", "plex", "--yes"],
    ) == 0
    out = capsys.readouterr().out

    assert "Sent 1 item(s) to TRAKT." in out
    assert state._http.param_calls[0] == (
        "GET",
        "/api/editor",
        {"kind": "watchlist", "source": "state", "provider": "PLEX"},
        None,
    )
    body = state._http.calls[-1][2]
    assert body["providers"] == [{"provider": "TRAKT", "instance": "default"}]
    assert body["items"][0]["key"] == "tmdb:454639"
    assert body["items"][0]["ids"]["tmdb"] == 454639


def test_rows_from_payload_handles_dicts_and_strings() -> None:
    from cli._util import rows_from_payload

    assert rows_from_payload({"items": {"tmdb:1": {"title": "Heat"}}}, "items") == [
        {"title": "Heat", "key": "tmdb:1"}
    ]
    assert rows_from_payload({"providers": ["PLEX"]}, "providers") == [
        {"value": "PLEX", "name": "PLEX", "provider": "PLEX"}
    ]


def test_metadata_resolve_uses_api_entity_field(
    clean_cli_env,
) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="json")
    state._http = FakeTransport({("POST", "/api/metadata/resolve"): {"ok": True, "result": {}}})

    assert run_isolated(state, ["metadata", "resolve", "tmdb=1399", "--type", "tv"]) == 0

    assert state._http.calls == [
        ("POST", "/api/metadata/resolve", {"ids": {"tmdb": "1399"}, "entity": "tv"})
    ]


def test_manual_watched_sends_editor_style_payload(
    clean_cli_env,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="plain")
    state._http = FakeTransport({("POST", "/api/manual/watched"): {"ok": True, "results": []}})

    assert run_isolated(
        state,
        [
            "manual",
            "watched",
            "--field",
            "tmdb=603",
            "--field",
            "type=movie",
            "--field",
            "rating=9",
            "--provider",
            "trakt",
            "--at",
            "2026-08-22",
        ],
    ) == 0

    body = state._http.calls[-1][2]
    assert body["item"]["tmdb"] == "603"
    assert body["providers"] == [{"provider": "TRAKT", "instance": "default"}]
    assert body["actions"] == {"history": True, "watchlist": False, "rating": True}
    assert body["date_mode"] == "custom"
    assert body["watched_on"] == "2026-08-22"
    assert body["rating"] == "9"
    assert "Recorded." in capsys.readouterr().out


def test_backup_retention_uses_max_backups(
    clean_cli_env,
) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="json")
    state._http = FakeTransport({("POST", "/api/backups/retention"): {"ok": True, "result": {}}})

    assert run_isolated(state, ["backup", "retention", "7"]) == 0

    assert state._http.calls == [("POST", "/api/backups/retention", {"max_backups": 7})]


def test_capture_diff_reads_nested_diff_rows(
    clean_cli_env,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="plain")
    state._http = FakeTransport(
        {
            ("GET", "/api/snapshots/diff"): {
                "ok": True,
                "diff": {
                    "summary": {"added": 1, "removed": 0, "changed": 0},
                    "items": [{"kind": "added", "title": "Heat", "type": "movie"}],
                },
            },
        }
    )

    assert run_isolated(state, ["capture", "diff", "old.json", "new.json"]) == 0

    out = capsys.readouterr().out
    assert "Heat" in out
    assert "added" in out


def test_scrobbler_event_routes_reads_watcher_and_webhook_routes(
    clean_cli_env,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="plain")
    state._http = FakeTransport(
        {
            ("GET", "/api/scrobble/event_routes"): {
                "ok": True,
                "watcher_routes": [{"source": "watcher", "provider": "plex", "sink": "trakt", "label": "Plex -> Trakt"}],
                "webhook_routes": [{"source": "webhook", "provider": "jellyfin", "label": "Jellyfin webhook"}],
            },
        }
    )

    assert run_isolated(state, ["scrobbler", "event-routes"]) == 0

    out = capsys.readouterr().out
    assert "Plex -> Trakt" in out
    assert "Jellyfin webhook" in out


def test_scrobbler_profile_webhook_regenerate_uses_provider_payload(
    clean_cli_env,
) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="json")
    state._http = FakeTransport({("POST", "/api/scrobbler/webhooks/profile/regenerate"): {"ok": True}})

    assert run_isolated(state, ["scrobbler", "webhook", "regenerate", "--profile", "plex:P2", "--yes"]) == 0

    assert state._http.calls == [
        ("POST", "/api/scrobbler/webhooks/profile/regenerate", {"provider": "plex", "provider_instance": "P2"})
    ]


def test_progress_watched_loads_selected_records_before_bulk_action(
    clean_cli_env,
) -> None:
    from cli._app import run_isolated

    record = {
        "key": "tmdb:603",
        "provider": "trakt",
        "instance_id": "default",
        "remote_id": "abc",
        "can_mark_watched": True,
    }
    state = Ctx.build(url="http://cw.test", output="json")
    state._http = FakeTransport(
        {
            ("GET", "/api/playback_progress/items"): {"items": [record]},
            ("POST", "/api/playback_progress/actions/bulk"): {"successful": 1, "failed": 0},
        }
    )

    assert run_isolated(state, ["progress", "watched", "tmdb:603", "--yes"]) == 0

    assert state._http.param_calls[0] == (
        "GET",
        "/api/playback_progress/items",
        {"page_size": 250, "user_profile": ""},
        None,
    )
    assert state._http.calls[-1] == (
        "POST",
        "/api/playback_progress/actions/bulk",
        {"action": "mark_watched", "items": [record]},
    )


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


def test_maintenance_cleanup_features_accepts_repeated_and_csv_values() -> None:
    from cli.commands.maintenance import _split_cleanup_features

    assert _split_cleanup_features(["watchlist,ratings", "history"]) == ["watchlist", "ratings", "history"]
    assert _split_cleanup_features([], all_features=True) == ["watchlist", "ratings", "history", "progress", "collection"]
    with pytest.raises(CLIError):
        _split_cleanup_features(["bogus"])


def test_maintenance_provider_cleanup_posts_snapshot_tool_clear() -> None:
    from cli.commands.maintenance import maintenance_provider_cleanup

    http = FakeTransport({("POST", "/api/snapshots/tools/clear"): {"ok": True, "progress_id": "job-1"}})
    state = _ctx(http)

    maintenance_provider_cleanup(
        SimpleNamespace(obj=state),
        provider="plex",
        instance="kids",
        feature=["watchlist,history"],
        all_features=False,
        targets=False,
        wait=False,
        timeout=10,
        yes=True,
    )

    assert http.calls == [
        (
            "POST",
            "/api/snapshots/tools/clear",
            {
                "provider": "PLEX",
                "instance": "kids",
                "features": ["watchlist", "history"],
                "progress_id": http.calls[0][2]["progress_id"],
                "background": True,
            },
        )
    ]


def test_maintenance_provider_cleanup_lists_manifest_targets() -> None:
    from cli.commands.maintenance import maintenance_provider_cleanup

    http = FakeTransport(
        {
            (
                "GET",
                "/api/snapshots/manifest",
            ): {
                "providers": [
                    {
                        "id": "PLEX",
                        "label": "Plex",
                        "configured": True,
                        "features": {"watchlist": True, "ratings": False, "history": True, "progress": True},
                        "instances": [{"id": "default", "label": "Default", "configured": True}],
                    }
                ]
            }
        }
    )
    state = _ctx(http)

    maintenance_provider_cleanup(
        SimpleNamespace(obj=state),
        provider="",
        instance="default",
        feature=[],
        all_features=False,
        targets=False,
        wait=True,
        timeout=10,
        yes=False,
    )

    assert http.param_calls[0][:3] == ("GET", "/api/snapshots/manifest", None)


def test_maintenance_factory_reset_requires_typed_confirmation() -> None:
    from cli.commands.maintenance import maintenance_factory_reset

    http = FakeTransport({("POST", "/api/maintenance/reset-all-default"): {"ok": True}})
    state = _ctx(http)

    with pytest.raises(CLIError):
        maintenance_factory_reset(SimpleNamespace(obj=state), confirm="", restart=False)

    assert http.calls == []


def test_maintenance_factory_reset_posts_reset_endpoint() -> None:
    from cli.commands.maintenance import maintenance_factory_reset

    http = FakeTransport({("POST", "/api/maintenance/reset-all-default"): {"ok": True, "backup": "/config/config.json.bak"}})
    state = _ctx(http)

    maintenance_factory_reset(SimpleNamespace(obj=state), confirm="RESET", restart=True)

    assert http.calls == [("POST", "/api/maintenance/reset-all-default", {"restart": True})]


def test_maintenance_reset_pair_posts_selected_features() -> None:
    from cli.commands.maintenance import maintenance_reset_pair

    pairs = {"pairs": [{"id": "p1", "label": "PLEX <-> SIMKL", "mode": "two-way", "features": ["history", "watchlist"]}]}
    http = FakeTransport({
        ("GET", "/api/maintenance/support/scopes"): pairs,
        ("POST", "/api/maintenance/clear-pair-state"): {"ok": True, "removed": {"baselines": 2}},
    })
    state = _ctx(http)

    maintenance_reset_pair(SimpleNamespace(obj=state), pair="p1", feature=["history"], keep_retry=True, yes=True)
    maintenance_reset_pair(SimpleNamespace(obj=state), pair="p1", feature=[], keep_retry=False, yes=True)

    posts = [call for call in http.calls if call[0] == "POST"]
    assert posts == [
        ("POST", "/api/maintenance/clear-pair-state", {"pair_id": "p1", "features": ["history"], "clear_retry": False}),
        ("POST", "/api/maintenance/clear-pair-state", {"pair_id": "p1", "features": ["history", "watchlist"], "clear_retry": True}),
    ]


def test_maintenance_reset_pair_lists_pairs_and_rejects_unknown_input() -> None:
    from cli.commands.maintenance import maintenance_reset_pair

    http = FakeTransport({("GET", "/api/maintenance/support/scopes"): {"pairs": [{"id": "p1", "features": ["history"]}]}})
    state = _ctx(http)

    maintenance_reset_pair(SimpleNamespace(obj=state), pair="", feature=[], keep_retry=False, yes=True)
    with pytest.raises(CLIError):
        maintenance_reset_pair(SimpleNamespace(obj=state), pair="nope", feature=[], keep_retry=False, yes=True)
    with pytest.raises(CLIError):
        maintenance_reset_pair(SimpleNamespace(obj=state), pair="p1", feature=["bogus"], keep_retry=False, yes=True)

    assert not [call for call in http.calls if call[0] == "POST"]


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


def test_scheduler_set_every_configures_standard_scheduler() -> None:
    from cli.commands.scheduler import scheduler_set

    http = FakeTransport(
        {
            ("GET", "/api/scheduling"): {"enabled": False, "advanced": {"enabled": True, "jobs": []}},
            ("POST", "/api/scheduling"): {"ok": True, "next_run_at": 123},
        }
    )
    state = _ctx(http=http)

    scheduler_set(SimpleNamespace(obj=state), "every", "6h", dry_run=False)

    assert http.calls[-1] == (
        "POST",
        "/api/scheduling",
        {
            "enabled": True,
            "advanced": {"enabled": False, "jobs": []},
            "mode": "every_n_hours",
            "every_n_hours": 6,
        },
    )


def test_scheduler_add_repeated_times_creates_multiple_advanced_jobs() -> None:
    from cli.commands.scheduler import scheduler_add

    http = FakeTransport(
        {
            ("GET", "/api/pairs"): [{"id": "simkl-plex", "source": "SIMKL", "target": "PLEX"}],
            ("GET", "/api/scheduling"): {"enabled": False, "advanced": {"enabled": False, "jobs": []}},
            ("POST", "/api/scheduling"): {"ok": True, "next_run_at": 123},
        }
    )
    state = _ctx(http=http)

    scheduler_add(SimpleNamespace(obj=state), "simkl", at=["00:00", "06:00"], days="weekdays", job_id="", after="", paused=False, dry_run=False)

    posted = http.calls[-1][2]
    jobs = posted["advanced"]["jobs"]
    assert posted["advanced"]["enabled"] is True
    assert [j["id"] for j in jobs] == ["simkl_plex_0000", "simkl_plex_0600"]
    assert [j["at"] for j in jobs] == ["00:00", "06:00"]
    assert jobs[0]["days"] == [1, 2, 3, 4, 5]
    assert jobs[0]["pair_id"] == "simkl-plex"


def test_scheduler_edit_updates_existing_job() -> None:
    from cli.commands.scheduler import scheduler_edit

    http = FakeTransport(
        {
            ("GET", "/api/scheduling"): {
                "enabled": False,
                "advanced": {
                    "enabled": True,
                    "jobs": [{"id": "night", "pair_id": "a", "at": "00:00", "days": [], "after": None, "active": True}],
                },
            },
            ("POST", "/api/scheduling"): {"ok": True, "next_run_at": 123},
        }
    )
    state = _ctx(http=http)

    scheduler_edit(SimpleNamespace(obj=state), "night", pair_id="", at="03:30", days="weekends", after="", dry_run=False)

    job = http.calls[-1][2]["advanced"]["jobs"][0]
    assert job["at"] == "03:30"
    assert job["days"] == [6, 7]
    assert job["after"] is None


def test_scheduler_pause_resume_and_delete_mutate_jobs() -> None:
    from cli.commands.scheduler import scheduler_delete, scheduler_pause, scheduler_resume

    base = {
        "enabled": False,
        "advanced": {
            "enabled": True,
            "jobs": [
                {"id": "first", "pair_id": "a", "at": "00:00", "days": [], "active": True},
                {"id": "second", "pair_id": "b", "at": "06:00", "days": [], "active": True},
            ],
        },
    }
    http = FakeTransport({("GET", "/api/scheduling"): base, ("POST", "/api/scheduling"): {"ok": True}})
    state = _ctx(http=http)

    scheduler_pause(SimpleNamespace(obj=state), "first", dry_run=False)
    assert http.calls[-1][2]["advanced"]["jobs"][0]["active"] is False

    http.routes[("GET", "/api/scheduling")] = http.calls[-1][2]
    scheduler_resume(SimpleNamespace(obj=state), "first", dry_run=False)
    assert http.calls[-1][2]["advanced"]["jobs"][0]["active"] is True

    http.routes[("GET", "/api/scheduling")] = http.calls[-1][2]
    scheduler_delete(SimpleNamespace(obj=state), "second", yes=True, dry_run=False)
    assert [j["id"] for j in http.calls[-1][2]["advanced"]["jobs"]] == ["first"]


def test_sync_once_filters_selected_pairs_and_returns_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    import typer
    import cw_platform.config_base as config_base
    import cw_platform.orchestrator as orchestrator
    from cli.commands.sync import sync_once

    seen: dict[str, Any] = {}

    class FakeOrchestrator:
        def __init__(self, cfg: dict[str, Any]) -> None:
            seen["pairs"] = cfg.get("pairs")

        def run(self, **kwargs: Any) -> dict[str, Any]:
            seen["kwargs"] = kwargs
            return {"ok": True, "pairs": 2, "updated": 0, "added": 0, "removed": 0, "skipped": 0, "unresolved": 0, "errors": 0}

    monkeypatch.setattr(config_base, "load_config", lambda: {
        "pairs": [
            {"id": "a", "source": "PLEX", "target": "TRAKT", "enabled": True},
            {"id": "b", "source": "SIMKL", "target": "PLEX", "enabled": True},
            {"id": "c", "source": "MDBLIST", "target": "TRAKT", "enabled": True},
        ],
    })
    monkeypatch.setattr(orchestrator, "Orchestrator", FakeOrchestrator)
    state = _ctx(http=FakeTransport())

    with pytest.raises(typer.Exit) as err:
        sync_once(
            SimpleNamespace(obj=state),
            target="",
            pair=["a", "b"],
            feature=[],
            dry_run=False,
            no_state_json=False,
            fail_on_unresolved=False,
            fail_on_skipped=False,
            json_output=True,
            plan_out="",
        )

    assert err.value.exit_code == 0
    assert [p["id"] for p in seen["pairs"]] == ["a", "b"]
    assert seen["kwargs"]["write_state_json"] is True


def test_sync_once_feature_filter_and_strict_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    import typer
    import cw_platform.config_base as config_base
    import cw_platform.orchestrator as orchestrator
    from cli.commands.sync import sync_once

    seen: dict[str, Any] = {}

    class FakeOrchestrator:
        def __init__(self, cfg: dict[str, Any]) -> None:
            seen["pairs"] = cfg.get("pairs")

        def run(self, **_: Any) -> dict[str, Any]:
            return {"ok": True, "pairs": 1, "updated": 0, "added": 0, "removed": 0, "skipped": 0, "unresolved": 1, "errors": 0}

    monkeypatch.setattr(config_base, "load_config", lambda: {
        "pairs": [
            {
                "id": "a",
                "source": "PLEX",
                "target": "TRAKT",
                "enabled": True,
                "features": {"watchlist": {"enable": True}, "ratings": {"enable": True}},
            },
        ],
    })
    monkeypatch.setattr(orchestrator, "Orchestrator", FakeOrchestrator)
    state = _ctx(http=FakeTransport())

    with pytest.raises(typer.Exit) as err:
        sync_once(
            SimpleNamespace(obj=state),
            target="a",
            pair=[],
            feature=["watchlist"],
            dry_run=False,
            no_state_json=True,
            fail_on_unresolved=True,
            fail_on_skipped=False,
            json_output=True,
            plan_out="",
        )

    assert err.value.exit_code == 1
    assert seen["pairs"][0]["features"] == {"watchlist": {"enable": True}}


def test_sync_once_no_enabled_pairs_exits_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    import typer
    import cw_platform.config_base as config_base
    from cli.commands.sync import sync_once

    monkeypatch.setattr(config_base, "load_config", lambda: {"pairs": [{"id": "a", "enabled": False}]})
    state = _ctx(http=FakeTransport())

    with pytest.raises(typer.Exit) as err:
        sync_once(
            SimpleNamespace(obj=state),
            target="",
            pair=[],
            feature=[],
            dry_run=False,
            no_state_json=False,
            fail_on_unresolved=False,
            fail_on_skipped=False,
            json_output=True,
            plan_out="",
        )

    assert err.value.exit_code == 0


def test_sync_once_plan_out_requires_dry_run(tmp_path: Path) -> None:
    from cli.commands.sync import sync_once

    with pytest.raises(CLIError) as err:
        sync_once(
            SimpleNamespace(obj=_ctx(http=FakeTransport())),
            target="",
            pair=[],
            feature=[],
            dry_run=False,
            no_state_json=False,
            fail_on_unresolved=False,
            fail_on_skipped=False,
            json_output=True,
            plan_out=str(tmp_path / "plan.json"),
        )

    assert err.value.exit_code == 2
    assert not (tmp_path / "plan.json").exists()


def test_sync_once_plan_out_writes_review_rows_per_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import typer
    import cw_platform.config_base as config_base
    from cli.commands.sync import sync_once
    from services import interactive_sync as review
    from services.interactive_sync_store import ReviewStore

    planned: list[str] = []

    def row(rid: str, operation: str, result: str) -> dict[str, Any]:
        return {
            "id": rid, "key": f"imdb:{rid}", "feature": "history", "provider": "TRAKT", "instance": "default",
            "source": "PLEX", "source_instance": "default", "operation": operation, "result": result,
            "reason": "", "selectable": result != "blocked",
            "item": {"type": "movie", "title": rid, "ids": {"imdb": rid}},
        }

    def fake_refresh(session: Any, cfg: dict[str, Any], choices: dict[str, str], **kwargs: Any) -> None:
        planned.append(session.pair_id)
        assert kwargs == {"complete_empty": False}
        store = ReviewStore()
        store.rows["add"] = row("add", "add", "add")
        store.rows["gone"] = row("gone", "remove", "remove")
        store.rows["lost"] = row("lost", "add", "unresolved")
        store.finish()
        session.store = store
        session.summary = {"ok": True, "errors": 0, "blocked": 2, "cancelled": False, "pairs": 1}
        session.revision = 1

    monkeypatch.setattr(config_base, "load_config", lambda: {
        "pairs": [
            {"id": "a", "source": "PLEX", "target": "TRAKT", "enabled": True},
            {"id": "b", "source": "PLEX", "target": "SIMKL", "enabled": True},
            {"id": "c", "source": "PLEX", "target": "MDBLIST", "enabled": False},
        ],
    })
    monkeypatch.setattr(review, "refresh", fake_refresh)
    target = tmp_path / "plan.json"

    with pytest.raises(typer.Exit) as err:
        sync_once(
            SimpleNamespace(obj=_ctx(http=FakeTransport())),
            target="",
            pair=[],
            feature=[],
            dry_run=True,
            no_state_json=False,
            fail_on_unresolved=True,
            fail_on_skipped=False,
            json_output=True,
            plan_out=str(target),
        )

    assert err.value.exit_code == 1
    assert planned == ["a", "b"]
    plan = json.loads(target.read_text(encoding="utf-8"))
    assert plan["dry_run"] is True
    assert [p["pair_id"] for p in plan["pairs"]] == ["a", "b"]
    assert [r["result"] for r in plan["pairs"][0]["rows"]] == ["add", "remove", "unresolved"]
    assert plan["pairs"][0]["pair"]["target"] == "TRAKT"
    assert plan["pairs"][1]["exported_rows"] == 3
    assert not list(tmp_path.glob("*.part"))
    summary = json.loads(capsys.readouterr().out)
    assert (summary["added"], summary["removed"], summary["unresolved"], summary["blocked"]) == (2, 2, 2, 4)
    assert summary["plan"] == {"path": str(target), "rows": 6, "written": True}


def _plan_once_args(target: Path, **overrides: Any) -> dict[str, Any]:
    return {
        "target": "", "pair": [], "feature": [], "dry_run": True, "no_state_json": False,
        "fail_on_unresolved": False, "fail_on_skipped": False, "json_output": True, "plan_out": str(target),
        **overrides,
    }


def test_sync_once_plan_out_keeps_the_old_plan_when_planning_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import typer
    import cw_platform.config_base as config_base
    from cli.commands.sync import sync_once
    from services import interactive_sync as review
    from services.interactive_sync_store import ReviewStore

    def failing_refresh(session: Any, cfg: dict[str, Any], choices: dict[str, str], **kwargs: Any) -> None:
        session.store = ReviewStore()
        session.store.finish()
        session.summary = {"ok": False, "errors": 1, "blocked": 0, "cancelled": False, "pairs": 1}

    monkeypatch.setattr(config_base, "load_config", lambda: {"pairs": [{"id": "a", "source": "PLEX", "target": "TRAKT", "enabled": True}]})
    monkeypatch.setattr(review, "refresh", failing_refresh)
    target = tmp_path / "plan.json"
    target.write_text("previous", encoding="utf-8")

    with pytest.raises(typer.Exit) as err:
        sync_once(SimpleNamespace(obj=_ctx(http=FakeTransport())), **_plan_once_args(target))

    assert err.value.exit_code == 1
    assert target.read_text(encoding="utf-8") == "previous"
    assert not list(tmp_path.glob("*.part"))
    summary = json.loads(capsys.readouterr().out)
    assert summary["ok"] is False and summary["plan"]["written"] is False


def test_sync_once_plan_out_writes_an_empty_plan_when_no_pair_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import typer
    import cw_platform.config_base as config_base
    from cli.commands.sync import sync_once

    monkeypatch.setattr(config_base, "load_config", lambda: {"pairs": [{"id": "a", "enabled": False}]})
    target = tmp_path / "plan.json"
    target.write_text("previous", encoding="utf-8")

    with pytest.raises(typer.Exit) as err:
        sync_once(SimpleNamespace(obj=_ctx(http=FakeTransport())), **_plan_once_args(target))

    assert err.value.exit_code == 0
    assert json.loads(target.read_text(encoding="utf-8"))["pairs"] == []
    assert json.loads(capsys.readouterr().out)["plan"] == {"path": str(target), "rows": 0, "written": True}


def test_sync_once_plan_out_rejects_fail_on_skipped_and_unwritable_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import cw_platform.config_base as config_base
    from cli.commands.sync import sync_once

    monkeypatch.setattr(config_base, "load_config", lambda: {"pairs": [{"id": "a", "source": "PLEX", "target": "TRAKT", "enabled": True}]})
    state = SimpleNamespace(obj=_ctx(http=FakeTransport()))
    target = tmp_path / "plan.json"

    with pytest.raises(CLIError) as err:
        sync_once(state, **_plan_once_args(target, fail_on_skipped=True))
    assert err.value.exit_code == 2

    (tmp_path / "plan.json.part").mkdir()
    with pytest.raises(CLIError) as err:
        sync_once(state, **_plan_once_args(target))
    assert err.value.exit_code == 2
    assert not target.exists()


def test_sync_once_plan_out_uses_the_review_planner_without_writing(
    config_base: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import typer
    import cw_platform.config_base as cfg_module
    from cli.commands.sync import sync_once
    from test_interactive_sync import item, setup_ops
    from test_orchestrator_dry_run_no_side_effects import _cfg

    src, dst = setup_ops(config_base, monkeypatch, [item(1), item(2)])
    monkeypatch.setattr(cfg_module, "load_config", lambda: _cfg(False))
    target = tmp_path / "plan.json"

    with pytest.raises(typer.Exit) as err:
        sync_once(
            SimpleNamespace(obj=_ctx(http=FakeTransport())),
            target="p1",
            pair=[],
            feature=[],
            dry_run=True,
            no_state_json=False,
            fail_on_unresolved=False,
            fail_on_skipped=False,
            json_output=True,
            plan_out=str(target),
        )

    assert err.value.exit_code == 0
    rows = json.loads(target.read_text(encoding="utf-8"))["pairs"][0]["rows"]
    assert sorted(r["item"]["title"] for r in rows) == ["Movie 1", "Movie 2"]
    assert {(r["operation"], r["result"], r["provider"]) for r in rows} == {("add", "add", "DST")}
    assert json.loads(capsys.readouterr().out)["added"] == 2
    assert not src.add_calls and not dst.add_calls


def test_pair_create_add_only_sends_explicit_add_and_remove() -> None:
    from cli.commands.pair import pair_create

    http = FakeTransport({("POST", "/api/pairs"): {"ok": True, "id": "pair_1"}})

    def create(features: list[str]) -> dict[str, Any]:
        pair_create(
            SimpleNamespace(obj=_ctx(http=http)), "plex", "trakt", mode="one-way", feature=features,
            source_instance="", target_instance="", profile="", disabled=True, add_only=True,
        )
        return http.calls[-1][2]["features"]

    safe = {"enable": True, "add": True, "remove": False}
    assert create(["History", "ratings"]) == {"history": safe, "ratings": safe}
    assert create([]) == {"watchlist": safe}


def _pair_routes(pair: dict[str, Any], sync: dict[str, Any] | None = None) -> FakeTransport:
    return FakeTransport({
        ("GET", "/api/pairs"): [pair],
        ("PUT", f"/api/pairs/{pair['id']}"): {"ok": True},
        ("GET", "/api/config"): {"sync": sync or {}},
    })


def test_pair_set_updates_one_key_and_keeps_the_rest(capsys: pytest.CaptureFixture[str]) -> None:
    from cli.commands.pair import pair_set

    pair = {
        "id": "p1", "source": "PLEX", "target": "TRAKT", "mode": "one-way",
        "features": {"history": {"enable": True, "add": True, "remove": False, "remove_mode": "mirror"}, "watchlist": True},
    }
    http = _pair_routes(pair)
    state = SimpleNamespace(obj=_ctx(http=http))

    pair_set(state, "p1", "history.remove", "on")
    sent = http.calls[-2][2]["features"]
    assert sent["history"] == {"enable": True, "add": True, "remove": True, "remove_mode": "mirror"}
    assert sent["watchlist"] is True
    out = json.loads(capsys.readouterr().out)
    assert out["value"] is True
    assert len(out["warnings"]) == 1 and "Remove runs in mirror mode" in out["warnings"][0]

    pair_set(state, "p1", "history.remove_mode", "source")
    assert http.calls[-2][2]["features"]["history"]["remove_mode"] == "source_deletes"
    assert json.loads(capsys.readouterr().out)["warnings"] == []

    pair_set(state, "p1", "watchlist.add", "off")
    assert http.calls[-2][2]["features"]["watchlist"] == {"enable": True, "add": False}


def test_pair_set_libraries_merges_shorthand_and_replaces_with_json() -> None:
    from cli.commands.pair import pair_set

    pair = {
        "id": "p1", "source": "PLEX", "target": "TRAKT", "mode": "one-way",
        "features": {"history": {"enable": True, "libraries": {"PLEX#second": ["9"]}}, "watchlist": {"enable": True}},
    }
    http = _pair_routes(pair)
    state = SimpleNamespace(obj=_ctx(http=http))

    pair_set(state, "p1", "history.libraries", "plex=1, 2,1")
    assert http.calls[-2][2]["features"]["history"]["libraries"] == {"PLEX#second": ["9"], "PLEX": ["1", "2"]}

    pair_set(state, "p1", "history.libraries", '{"PLEX": [3]}')
    assert http.calls[-2][2]["features"]["history"]["libraries"] == {"PLEX": ["3"]}

    pair_set(state, "p1", "history.libraries", "PLEX#second=")
    assert "libraries" not in http.calls[-2][2]["features"]["history"]

    pair["features"]["history"]["libraries"] = {"PLEX": ["1"], "TRAKT": ["7"]}
    pair_set(state, "p1", "history.libraries", "plex=")
    assert http.calls[-2][2]["features"]["history"]["libraries"] == {"TRAKT": ["7"]}

    for setting, value in (("history.libraries", "TRAKT=1"), ("watchlist.libraries", "PLEX=1"), ("history.libraries", "1,2")):
        with pytest.raises(CLIError) as err:
            pair_set(state, "p1", setting, value)
        assert err.value.exit_code == 2


@pytest.mark.parametrize("setting,value", [
    ("history.remove", "maybe"),
    ("history.remove_mode", "wipe"),
    ("history.enable", "on"),
    ("ratings.add", "on"),
    ("watchlist.rewatches", "on"),
    ("history", "on"),
])
def test_pair_set_rejects_invalid_input_without_writing(setting: str, value: str) -> None:
    from cli.commands.pair import pair_set

    pair = {"id": "p1", "source": "PLEX", "target": "TRAKT", "features": {"history": {"enable": True}, "watchlist": {"enable": True}}}
    http = _pair_routes(pair)

    with pytest.raises(CLIError) as err:
        pair_set(SimpleNamespace(obj=_ctx(http=http)), "p1", setting, value)

    assert err.value.exit_code == 2
    assert not [call for call in http.calls if call[0] == "PUT"]


def test_pair_show_warns_about_mirror_mode(capsys: pytest.CaptureFixture[str]) -> None:
    from cli.commands.pair import _mirror_warnings, pair_show

    pair = {
        "id": "p1", "source": "PLEX", "target": "TRAKT", "mode": "one-way",
        "features": {
            "history": {"enable": True, "remove": False, "remove_mode": "mirror"},
            "ratings": {"enable": True, "remove": True},
            "watchlist": {"enable": True, "remove": True, "remove_mode": "source_deletes"},
        },
    }

    pair_show(SimpleNamespace(obj=_ctx(http=_pair_routes(pair))), "p1")
    stored = json.loads(capsys.readouterr().out)["warnings"]
    assert len(stored) == 1 and stored[0].startswith("history: mirror mode is stored on this pair while Remove is off")
    assert "cw pair set p1 history.remove_mode source_deletes" in stored[0]

    pair_show(SimpleNamespace(obj=_ctx(http=_pair_routes(pair, {"one_way_remove_mode": "mirror"}))), "p1")
    inherited = json.loads(capsys.readouterr().out)["warnings"]
    assert [w.split(":")[0] for w in inherited] == ["history", "ratings"]
    assert "inherited from sync.one_way_remove_mode" in inherited[1]

    two_way = _mirror_warnings({**pair, "mode": "two-way"}, {"one_way_remove_mode": "mirror"})
    assert len(two_way) == 1 and "not used while the pair is two-way" in two_way[0]

    plain = {"id": "p2", "source": "PLEX", "target": "TRAKT", "features": {"history": True, "ratings": {"enable": True}, "playlists": True}}
    off = _mirror_warnings(plain, {"one_way_remove_mode": "mirror"})
    assert [w.split(":")[0] for w in off] == ["history", "ratings"]
    assert all("while Remove is off" in w for w in off)
    on = _mirror_warnings(plain, {"one_way_remove_mode": "mirror", "enable_remove": True})
    assert all("Remove runs in mirror mode" in w for w in on) and len(on) == 2
    assert _mirror_warnings(plain, {"enable_remove": True}) == []


def test_playlist_resource_commands_use_provider_resource_api() -> None:
    from cli.commands.playlist import resource_create, resource_delete, resource_rename

    http = FakeTransport(
        {
            ("POST", "/api/playlists/resources"): {"ok": True, "resource": {"id": "new", "provider": "PLEX", "instance": "default", "name": "Weekend"}},
            ("PATCH", "/api/playlists/resources/pl1"): {"ok": True, "resource": {"id": "pl1", "name": "Renamed"}},
            ("DELETE", "/api/playlists/resources/pl1"): {"ok": True},
        }
    )
    state = _ctx(http=http)

    resource_create(SimpleNamespace(obj=state), "PLEX", name="Weekend", media_type="playlist", instance="default")
    resource_rename(SimpleNamespace(obj=state), "PLEX", "pl1", name="Renamed", instance="default")
    resource_delete(SimpleNamespace(obj=state), "PLEX", "pl1", instance="default", yes=True)

    assert http.calls[0] == ("POST", "/api/playlists/resources", {"provider": "PLEX", "instance": "default", "name": "Weekend", "media_type": "playlist"})
    assert http.calls[1] == ("PATCH", "/api/playlists/resources/pl1", {"provider": "PLEX", "instance": "default", "name": "Renamed"})
    assert http.param_calls[2] == ("DELETE", "/api/playlists/resources/pl1", {"provider": "PLEX", "instance": "default"}, None)


def test_playlist_endpoint_add_and_edit_have_friendly_arguments() -> None:
    from cli.commands.playlist import endpoint_add, endpoint_edit

    http = FakeTransport(
        {
            ("POST", "/api/playlists/endpoints"): {"ok": True, "created": True, "endpoint": {"id": "EP-01"}},
            ("GET", "/api/playlists/endpoints"): {
                "ok": True,
                "endpoints": [{"id": "EP-01", "name": "Old", "provider": "PLEX", "instance": "default", "playlist_id": "pl1"}],
            },
        }
    )
    state = _ctx(http=http)

    endpoint_add(SimpleNamespace(obj=state), "PLEX", "pl1", name="PlexFavs", instance="default", create="", media_type="", field=[])
    endpoint_edit(SimpleNamespace(obj=state), "EP", name="New", provider="", playlist_id="pl2", playlist_name="", instance="", media_type="", field=[])

    assert http.calls[0] == ("POST", "/api/playlists/endpoints", {"provider": "PLEX", "playlist_id": "pl1", "name": "PlexFavs", "instance": "default"})
    assert http.calls[-1] == (
        "POST",
        "/api/playlists/endpoints",
        {"id": "EP-01", "name": "New", "provider": "PLEX", "instance": "default", "playlist_id": "pl2"},
    )


def test_playlist_mapping_add_edit_enable_disable_are_friendly() -> None:
    from cli.commands.playlist import mapping_add, mapping_disable, mapping_edit, mapping_enable

    mapping = {
        "id": "MAP-01",
        "name": "Movies",
        "source_endpoint": "EP-01",
        "target_endpoints": ["EP-02"],
        "ruleset_id": "",
        "membership": "managed_only",
        "order": "ignore",
        "enabled": True,
    }
    http = FakeTransport(
        {
            ("POST", "/api/playlists/mappings"): {"ok": True, "created": True, "pair_id": "pair_playlist_1", "mapping": mapping},
            ("GET", "/api/playlists/mappings"): {"ok": True, "mappings": [mapping]},
        }
    )
    state = _ctx(http=http)

    mapping_add(
        SimpleNamespace(obj=state),
        source="EP-01",
        target=["EP-02", "EP-03"],
        name="Movies",
        ruleset="trakt_free_account",
        membership="managed_only",
        order="ignore",
        disabled=False,
        allow_mass_delete=False,
        field=[],
    )
    mapping_edit(
        SimpleNamespace(obj=state),
        "MAP",
        source="",
        target=["EP-04"],
        name="NewName",
        ruleset="",
        membership="mirror",
        order="preserve",
        allow_mass_delete=True,
        field=[],
    )
    mapping_disable(SimpleNamespace(obj=state), "MAP")
    mapping_enable(SimpleNamespace(obj=state), "MAP")

    assert http.calls[0][2]["target_endpoints"] == ["EP-02", "EP-03"]
    assert http.calls[0][2]["ruleset_id"] == "trakt_free_account"
    assert http.calls[2][2]["target_endpoints"] == ["EP-04"]
    assert http.calls[2][2]["membership"] == "mirror"
    assert http.calls[4][2]["enabled"] is False
    assert http.calls[6][2]["enabled"] is True


def test_playlist_mapping_run_accepts_wrapped_result() -> None:
    from cli.commands.playlist import mapping_run

    http = FakeTransport({("POST", "/api/playlists/mappings/MAP-01/run"): {"ok": True, "result": {"added": 2, "removed": 1, "errors": 0}}})
    state = _ctx(http=http)

    mapping_run(SimpleNamespace(obj=state), "MAP-01", dry_run=False)

    assert http.param_calls[-1] == ("POST", "/api/playlists/mappings/MAP-01/run", {"dry_run": False}, None)


def test_playlist_ruleset_add_validate_and_clone() -> None:
    from cli.commands.playlist import ruleset_add, ruleset_clone, ruleset_validate

    http = FakeTransport(
        {
            ("POST", "/api/playlists/rulesets"): {"ok": True, "created": True, "ruleset": {"id": "RS-01"}},
            ("POST", "/api/playlists/rulesets/validate"): {"ok": True, "ruleset": {"id": "RS-01"}},
            ("POST", "/api/playlists/rulesets/trakt_free_account/clone"): {"ok": True, "ruleset": {"id": "RS-02"}},
        }
    )
    state = _ctx(http=http)
    path = Path("ruleset-test.json")
    path.write_text('{"id":"RS-01","name":"Rules"}', encoding="utf-8")
    try:
        ruleset_add(SimpleNamespace(obj=state), str(path))
        ruleset_validate(SimpleNamespace(obj=state), str(path))
        ruleset_clone(SimpleNamespace(obj=state), "trakt_free_account", name="Custom")
    finally:
        path.unlink(missing_ok=True)

    assert http.calls[0] == ("POST", "/api/playlists/rulesets", {"id": "RS-01", "name": "Rules"})
    assert http.calls[1] == ("POST", "/api/playlists/rulesets/validate", {"id": "RS-01", "name": "Rules"})
    assert http.calls[2] == ("POST", "/api/playlists/rulesets/trakt_free_account/clone", {"name": "Custom"})


def test_playlist_setup_creates_endpoints_and_mapping_non_interactively() -> None:
    from cli.commands.playlist import playlist_setup

    endpoint_ids = iter(["EP-01", "EP-02"])

    def post_endpoint(_method: str, _path: str, _params: Any, body: Any) -> dict[str, Any]:
        return {"ok": True, "created": True, "endpoint": {**body, "id": next(endpoint_ids)}}

    def post_mapping(_method: str, _path: str, _params: Any, body: Any) -> dict[str, Any]:
        return {"ok": True, "created": True, "pair_id": "pair_playlist_1", "mapping": {**body, "id": "MAP-01"}}

    http = FakeTransport(
        {
            ("POST", "/api/playlists/endpoints"): post_endpoint,
            ("POST", "/api/playlists/mappings"): post_mapping,
        }
    )
    state = _ctx(http=http)

    playlist_setup(
        SimpleNamespace(obj=state),
        source_provider="TRAKT",
        source_instance="default",
        source_playlist="src1",
        target_provider="PLEX",
        target_instance="default",
        target_playlist=[],
        create_target="Weekend Movies",
        name="Weekend",
        ruleset="",
        membership="managed_only",
        order="ignore",
        run_now=False,
        dry_run=False,
        yes=True,
    )

    assert http.calls[0][2]["playlist_id"] == "src1"
    assert http.calls[1][2]["pending_create"] == {"name": "Weekend Movies", "media_type": "playlist"}
    assert http.calls[2][2]["source_endpoint"] == "EP-01"
    assert http.calls[2][2]["target_endpoints"] == ["EP-02"]


def test_run_sync_wrapper_delegates_to_sync_once() -> None:
    wrapper = (Path(__file__).resolve().parents[1] / "docker" / "run-sync.sh").read_text(encoding="utf-8")

    assert "cw sync once" in wrapper


def test_entrypoint_supports_run_once_alias() -> None:
    entrypoint = (Path(__file__).resolve().parents[1] / "docker" / "entrypoint.sh").read_text(encoding="utf-8")

    assert '"$1" == "run-once"' in entrypoint
    assert "/usr/local/bin/cw sync once" in entrypoint


def test_every_command_group_is_registered() -> None:
    from cli._app import describe_commands

    names = {name for name, _ in describe_commands()}
    expected = {
        "status", "version", "health", "shell", "insights", "stats",
        "pair", "sync", "config", "auth", "watcher", "scheduler", "logs",
        "analyzer", "events", "capture", "backup", "watchlist", "progress",
        "editor", "playlist", "export", "import", "metadata", "manual",
        "anime", "instance", "user-profile", "user", "scrobbler", "activity", "maintenance",
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


TOKEN_ROUTE = ("POST", "/api/app-auth/tokens")
TOKEN_REPLY = {"token": "cwt_secret_value", "entry": {"id": "tok1", "name": "monitor", "prefix": "cwt_secr"}}


@pytest.mark.parametrize(
    ("output", "extra", "expected"),
    [
        ("plain", [], "cwt_secret_value"),
        ("json", [], "cwt_secret_value"),
        ("plain", ["--format", "env"], "CW_TOKEN=cwt_secret_value\n"),
    ],
)
def test_token_create_write_to_keeps_the_token_out_of_the_output(
    clean_cli_env,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    output: str,
    extra: list[str],
    expected: str,
) -> None:
    from cli._app import run_isolated
    from cli._settings import load_settings

    target = tmp_path / "cw_token"
    state = Ctx.build(url="http://cw.test", output=output)
    state._http = FakeTransport({TOKEN_ROUTE: TOKEN_REPLY})

    args = ["auth", "token", "create", "--name", "monitor", "--no-save", "--write-to", str(target), *extra]
    assert run_isolated(state, args) == 0

    captured = capsys.readouterr()
    assert "cwt_secret_value" not in captured.out + captured.err
    assert "tok1" in captured.out
    assert target.read_text(encoding="utf-8") == expected
    assert "token" not in load_settings()
    if os.name != "nt":
        assert stat.S_IMODE(target.stat().st_mode) == 0o600


def test_token_create_for_a_managed_user_sends_the_owner_and_keeps_the_cli_token(clean_cli_env) -> None:
    from cli._app import run_isolated
    from cli._settings import load_settings, update_settings

    update_settings(token="cwt_admin_value", url="http://cw.test")
    state = Ctx.build(url="http://cw.test", output="json")
    transport = FakeTransport({TOKEN_ROUTE: TOKEN_REPLY})
    state._http = transport

    assert run_isolated(state, ["auth", "token", "create", "--name", "mon", "--user", "monitor", "--expires-days", "365"]) == 0

    assert transport.calls == [("POST", "/api/app-auth/tokens", {"name": "mon", "expires_days": 365, "user_id": "monitor"})]
    assert load_settings().get("token") == "cwt_admin_value"


def test_token_create_without_a_user_still_saves_the_token(clean_cli_env) -> None:
    from cli._app import run_isolated
    from cli._settings import load_settings

    state = Ctx.build(url="http://cw.test", output="json")
    transport = FakeTransport({TOKEN_ROUTE: TOKEN_REPLY})
    state._http = transport

    assert run_isolated(state, ["auth", "token", "create", "--name", "cli"]) == 0

    assert transport.calls == [("POST", "/api/app-auth/tokens", {"name": "cli", "expires_days": 0})]
    assert load_settings().get("token") == "cwt_secret_value"


USERS_ROUTE = ("GET", "/api/app-auth/users")
PROFILES_ROUTE = ("GET", "/api/user-profiles")
USERS_REPLY = {
    "ok": True,
    "items": [
        {"id": "administrator", "username": "admin", "protected": True, "is_admin": True},
        {"id": "c" * 32, "username": "Monitor Bot", "profile_id": "p" * 32, "enabled": True, "permissions": {"write": False, "monitor": True}},
    ],
}
PROFILES_REPLY = {"ok": True, "items": [{"id": "p" * 32, "label": "Ops"}]}
MONITOR_PERMISSIONS = {"dashboard": False, "watchlist": False, "playback": False, "write": False, "monitor": True}
READONLY_PERMISSIONS = {"dashboard": True, "watchlist": True, "playback": True, "write": False, "monitor": False}


def _user_state(routes: dict[tuple[str, str], Any]) -> tuple[Ctx, FakeTransport]:
    state = Ctx.build(url="http://cw.test", output="json")
    transport = FakeTransport({USERS_ROUTE: USERS_REPLY, PROFILES_ROUTE: PROFILES_REPLY, **routes})
    state._http = transport
    return state, transport


def test_user_create_resolves_the_profile_and_sets_a_random_password(clean_cli_env, capsys: pytest.CaptureFixture[str]) -> None:
    from cli._app import run_isolated

    reply = {"ok": True, "user": {"id": "d" * 32, "username": "ops", "permissions": MONITOR_PERMISSIONS}}
    state, transport = _user_state({("POST", "/api/app-auth/users"): reply})

    assert run_isolated(state, ["user", "create", "ops", "--profile", "ops", "--access", "monitor"]) == 0

    method, path, body = transport.calls[-1]
    assert (method, path) == ("POST", "/api/app-auth/users")
    assert body["username"] == "ops"
    assert body["profile_id"] == "p" * 32
    assert body["permissions"] == MONITOR_PERMISSIONS
    assert len(body["password"]) >= 24
    out = capsys.readouterr().out
    assert body["password"] not in out
    assert json.loads(out)["password_generated"] is True


def test_user_create_rejects_unknown_access_and_profile_before_posting(clean_cli_env) -> None:
    from cli._app import run_isolated

    state, transport = _user_state({("POST", "/api/app-auth/users"): {"ok": True}})

    assert run_isolated(state, ["user", "create", "ops", "--profile", "Ops", "--access", "root"]) != 0
    assert run_isolated(state, ["user", "create", "ops", "--profile", "Nope"]) != 0
    assert all(call[0] != "POST" for call in transport.calls)


def test_user_set_and_delete_resolve_the_username(clean_cli_env) -> None:
    from cli._app import run_isolated

    target = "/api/app-auth/users/" + "c" * 32
    state, transport = _user_state(
        {
            ("PUT", target): {"ok": True, "user": {"id": "c" * 32, "username": "Monitor Bot", "permissions": READONLY_PERMISSIONS}},
            ("DELETE", target): {"ok": True, "deleted": True},
        }
    )

    assert run_isolated(state, ["user", "set", "monitor bot", "--access", "readonly", "--disable"]) == 0
    assert transport.calls[-1] == ("PUT", target, {"permissions": READONLY_PERMISSIONS, "enabled": False})

    assert run_isolated(state, ["user", "delete", "Monitor Bot", "--yes"]) == 0
    assert transport.calls[-1] == ("DELETE", target, None)

    assert run_isolated(state, ["user", "set", "admin", "--access", "full"]) != 0
    assert run_isolated(state, ["user", "set", "Monitor Bot"]) != 0
    assert run_isolated(state, ["user", "delete", "nobody", "--yes"]) != 0
    assert [call[0] for call in transport.calls].count("PUT") == 1
    assert [call[0] for call in transport.calls].count("DELETE") == 1


def test_token_create_write_to_refuses_existing_file_before_creating_a_token(clean_cli_env, tmp_path: Path) -> None:
    from cli._app import run_isolated

    target = tmp_path / "cw_token"
    target.write_text("keep", encoding="utf-8")
    state = Ctx.build(url="http://cw.test", output="json")
    state._http = FakeTransport({TOKEN_ROUTE: TOKEN_REPLY})

    assert run_isolated(state, ["auth", "token", "create", "--no-save", "--write-to", str(target)]) != 0
    assert state._http.calls == []
    assert target.read_text(encoding="utf-8") == "keep"

    assert run_isolated(state, ["auth", "token", "create", "--no-save", "--write-to", str(target), "--force"]) == 0
    assert target.read_text(encoding="utf-8") == "cwt_secret_value"


@pytest.mark.parametrize(
    "extra",
    [
        ["--format", "env"],
        ["--force"],
        ["--write-to", ""],
        ["--write-to", "  "],
        ["--write-to", "x", "--format", "yaml"],
    ],
)
def test_token_create_rejects_bad_file_options_before_creating_a_token(
    clean_cli_env,
    capsys: pytest.CaptureFixture[str],
    extra: list[str],
) -> None:
    from cli._app import run_isolated

    state = Ctx.build(url="http://cw.test", output="json")
    state._http = FakeTransport({TOKEN_ROUTE: TOKEN_REPLY})

    assert run_isolated(state, ["auth", "token", "create", "--no-save", *extra]) != 0
    assert state._http.calls == []
    captured = capsys.readouterr()
    assert "cwt_secret_value" not in captured.out + captured.err


def test_token_create_write_to_json_only_exposes_id_name_and_expiry(
    clean_cli_env,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from cli._app import run_isolated
    from cli._settings import load_settings

    state = Ctx.build(url="http://cw.test", output="json")
    state._http = FakeTransport({TOKEN_ROUTE: TOKEN_REPLY})

    assert run_isolated(state, ["auth", "token", "create", "--write-to", str(tmp_path / "cw_token")]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["entry"] == {"id": "tok1", "name": "monitor", "expires_at": None}
    assert load_settings()["token"] == "cwt_secret_value"


@pytest.mark.parametrize(("revocable", "hint"), [(True, "was revoked"), (False, "cw auth token revoke")])
def test_token_create_failed_forced_write_keeps_the_old_file_and_revokes(
    clean_cli_env,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    revocable: bool,
    hint: str,
) -> None:
    from cli._app import run_isolated
    from cli.commands import auth

    def boom(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(auth.os, "replace", boom)
    target = tmp_path / "cw_token"
    target.write_text("keep", encoding="utf-8")
    routes: dict[tuple[str, str], Any] = {TOKEN_ROUTE: TOKEN_REPLY}
    if revocable:
        routes[("DELETE", "/api/app-auth/tokens/tok1")] = {"ok": True}
    state = Ctx.build(url="http://cw.test", output="json")
    state._http = FakeTransport(routes)

    assert run_isolated(state, ["auth", "token", "create", "--write-to", str(target), "--force"]) != 0
    assert [call[:2] for call in state._http.calls] == [TOKEN_ROUTE, ("DELETE", "/api/app-auth/tokens/tok1")]
    assert target.read_text(encoding="utf-8") == "keep"
    assert [item.name for item in tmp_path.iterdir() if item.name != "cli-home"] == ["cw_token"]
    captured = capsys.readouterr()
    assert hint in captured.err
    assert "cwt_secret_value" not in captured.out + captured.err


@pytest.mark.parametrize("force", [False, True])
def test_token_create_failed_write_leaves_no_partial_file(
    clean_cli_env,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    force: bool,
) -> None:
    from cli._app import run_isolated
    from cli.commands import auth

    def boom(*_args: Any, **_kwargs: Any) -> int:
        raise OSError("disk full")

    monkeypatch.setattr(auth.os, "write", boom)
    target = tmp_path / "cw_token"
    state = Ctx.build(url="http://cw.test", output="json")
    state._http = FakeTransport({TOKEN_ROUTE: TOKEN_REPLY, ("DELETE", "/api/app-auth/tokens/tok1"): {"ok": True}})

    args = ["auth", "token", "create", "--no-save", "--write-to", str(target), *(["--force"] if force else [])]
    assert run_isolated(state, args) != 0
    assert [call[:2] for call in state._http.calls] == [TOKEN_ROUTE, ("DELETE", "/api/app-auth/tokens/tok1")]
    assert [item.name for item in tmp_path.iterdir() if item.name != "cli-home"] == []


@pytest.mark.parametrize("stalls", [False, True])
def test_token_create_write_to_handles_short_writes(
    clean_cli_env,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stalls: bool,
) -> None:
    from cli._app import run_isolated
    from cli.commands import auth

    real_write = os.write
    chunks: list[int] = []

    def short(fd: int, data: bytes) -> int:
        if stalls and chunks:
            return 0
        chunks.append(real_write(fd, bytes(data)[:4]))
        return chunks[-1]

    monkeypatch.setattr(auth.os, "write", short)
    target = tmp_path / "cw_token"
    target.write_text("keep", encoding="utf-8")
    state = Ctx.build(url="http://cw.test", output="json")
    state._http = FakeTransport({TOKEN_ROUTE: TOKEN_REPLY, ("DELETE", "/api/app-auth/tokens/tok1"): {"ok": True}})

    code = run_isolated(state, ["auth", "token", "create", "--no-save", "--write-to", str(target), "--force"])
    monkeypatch.undo()

    assert [item.name for item in tmp_path.iterdir() if item.name != "cli-home"] == ["cw_token"]
    if stalls:
        assert code != 0
        assert target.read_text(encoding="utf-8") == "keep"
        assert state._http.calls[-1][:2] == ("DELETE", "/api/app-auth/tokens/tok1")
    else:
        assert code == 0
        assert target.read_text(encoding="utf-8") == "cwt_secret_value"
        assert len(chunks) == 4
