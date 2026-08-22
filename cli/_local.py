# /cli/_local.py
# CrossWatch - In-process transport for CLI commands that run without the service
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Iterator

from ._util import as_dict
from ._errors import CLIError, LocalUnsupported
from ._transport import Transport

REPO_ROOT = Path(__file__).resolve().parent.parent


def _ensure_import_path() -> None:
    root = str(REPO_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)


class LocalTransport(Transport):
    name = "local"

    def __init__(self) -> None:
        _ensure_import_path()
        try:
            from cw_platform import config_base
        except Exception as exc:
            raise CLIError(
                "Cannot load the CrossWatch engine from this directory",
                hint=f"Run the CLI from the CrossWatch install root. ({exc})",
            ) from exc
        self._cfg_base = config_base
        self._routes: list[tuple[str, list[str], Callable[..., Any]]] = []
        self._register()

    def _add(self, method: str, pattern: str, handler: Callable[..., Any]) -> None:
        self._routes.append((method.upper(), [p for p in pattern.strip("/").split("/")], handler))

    def _register(self) -> None:
        self._add("GET", "/api/health", self._health)
        self._add("GET", "/api/version", self._version)
        self._add("GET", "/api/status", self._status)
        self._add("GET", "/api/config", self._config_get)
        self._add("POST", "/api/config", self._config_post)
        self._add("POST", "/api/config/unset", self._config_unset)
        self._add("GET", "/api/pairs", self._pairs_get)
        self._add("PUT", "/api/pairs/{pair_id}", self._pairs_put)
        self._add("DELETE", "/api/pairs/{pair_id}", self._pairs_delete)
        self._add("GET", "/api/scheduling", self._scheduling_get)
        self._add("GET", "/api/scheduling/status", self._scheduling_status)
        self._add("GET", "/api/scheduling/next", self._scheduling_next)
        self._add("GET", "/api/app-auth/tokens", self._tokens_get)
        self._add("POST", "/api/app-auth/tokens", self._tokens_post)
        self._add("DELETE", "/api/app-auth/tokens/{token_id}", self._tokens_delete)

    def _match(self, method: str, path: str) -> tuple[Callable[..., Any], dict[str, str]] | None:
        parts = [p for p in path.split("?", 1)[0].strip("/").split("/")]
        for route_method, pattern, handler in self._routes:
            if route_method != method.upper() or len(pattern) != len(parts):
                continue
            captured: dict[str, str] = {}
            ok = True
            for expected, actual in zip(pattern, parts):
                if expected.startswith("{") and expected.endswith("}"):
                    captured[expected[1:-1]] = actual
                elif expected != actual:
                    ok = False
                    break
            if ok:
                return handler, captured
        return None

    def request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: Any = None) -> Any:
        found = self._match(method, path)
        if found is None:
            raise LocalUnsupported(f"'{method.upper()} {path}'")
        handler, captured = found
        return handler(params=params or {}, body=json_body, **captured)

    def stream_sse(self, path: str, *, params: dict[str, Any] | None = None) -> Iterator[tuple[str, str]]:
        raise LocalUnsupported("Log streaming")

    def _load(self) -> dict[str, Any]:
        return dict(self._cfg_base.load_config() or {})

    def _health(self, **_: Any) -> dict[str, Any]:
        out: dict[str, Any] = {"ok": True, "mode": "local", "config_dir": str(self._cfg_base.CONFIG)}
        try:
            from cw_platform.local_db.diagnostics import diagnostics

            out["database"] = diagnostics(self._cfg_base.CONFIG)
        except Exception as exc:
            out["database"] = {"ok": False, "error": str(exc)}
        return out

    def _version(self, **_: Any) -> dict[str, Any]:
        version = ""
        for candidate in (REPO_ROOT / "VERSION", Path("/app/VERSION")):
            try:
                version = candidate.read_text(encoding="utf-8").strip()
            except Exception:
                continue
            if version:
                break
        return {"ok": True, "current": version or os.getenv("APP_VERSION") or "unknown"}

    def _status(self, **_: Any) -> dict[str, Any]:
        cfg = self._load()
        pairs = [p for p in (cfg.get("pairs") or []) if isinstance(p, dict)]
        enabled = [p for p in pairs if p.get("enabled", True) is not False]
        scheduling = as_dict(cfg.get("scheduling"))
        return {
            "ok": True,
            "mode": "local",
            "degraded": True,
            "pairs_total": len(pairs),
            "pairs_enabled": len(enabled),
            "can_sync": bool(enabled),
            "scheduling_enabled": bool(scheduling.get("enabled")),
            "providers": sorted(
                {
                    str(p.get(side) or "").upper()
                    for p in pairs
                    for side in ("source", "target")
                    if str(p.get(side) or "").strip()
                }
            ),
        }

    def _config_get(self, **_: Any) -> dict[str, Any]:
        cfg = self._load()
        try:
            return self._cfg_base.redact_config(cfg)
        except Exception:
            return cfg

    def _config_post(self, *, body: Any = None, **_: Any) -> dict[str, Any]:
        incoming = as_dict(body)
        current = self._load()
        merge: Any = getattr(self._cfg_base, "_deep_merge", None)
        merged: dict[str, Any] = {**current, **incoming} if merge is None else merge(current, incoming)
        self._cfg_base.save_config(merged)
        return {"ok": True}

    def _config_unset(self, *, body: Any = None, **_: Any) -> dict[str, Any]:
        from api.configAPI import PROTECTED_CONFIG_ROOTS, _delete_config_path, _split_config_path

        payload = as_dict(body)
        raw_paths = payload.get("paths")
        if not isinstance(raw_paths, list):
            raw_paths = [payload.get("path")] if payload.get("path") else []
        requested = [str(p or "").strip() for p in raw_paths if str(p or "").strip()]
        if not requested:
            return {"ok": False, "error": "no_paths"}

        cfg = self._load()
        removed: list[str] = []
        missing: list[str] = []
        for raw in requested:
            parts = _split_config_path(raw)
            if not parts:
                missing.append(raw)
                continue
            if parts[0] in PROTECTED_CONFIG_ROOTS:
                return {"ok": False, "error": "protected_path", "path": raw}
            if _delete_config_path(cfg, parts):
                removed.append(raw)
            else:
                missing.append(raw)
        if not removed:
            return {"ok": False, "error": "not_found", "missing": missing}
        self._cfg_base.save_config(cfg)
        return {"ok": True, "removed": removed, "missing": missing}

    def _pairs_get(self, **_: Any) -> list[dict[str, Any]]:
        cfg = self._load()
        return [p for p in (cfg.get("pairs") or []) if isinstance(p, dict)]

    def _pairs_put(self, *, pair_id: str, body: Any = None, **_: Any) -> dict[str, Any]:
        patch = as_dict(body)
        cfg = self._load()
        pairs = [p for p in (cfg.get("pairs") or []) if isinstance(p, dict)]
        for pair in pairs:
            if str(pair.get("id") or "") == str(pair_id):
                pair.update(patch)
                cfg["pairs"] = pairs
                self._cfg_base.save_config(cfg)
                return {"ok": True}
        return {"ok": False, "error": "not_found"}

    def _pairs_delete(self, *, pair_id: str, **_: Any) -> dict[str, Any]:
        cfg = self._load()
        pairs = [p for p in (cfg.get("pairs") or []) if isinstance(p, dict)]
        keep = [p for p in pairs if str(p.get("id") or "") != str(pair_id)]
        if len(keep) == len(pairs):
            return {"ok": False, "error": "not_found"}
        cfg["pairs"] = keep
        self._cfg_base.save_config(cfg)
        return {"ok": True}

    def _scheduling_get(self, **_: Any) -> dict[str, Any]:
        cfg = self._load()
        return as_dict(cfg.get("scheduling"))

    def _next_run_at(self, scfg: dict[str, Any]) -> int:
        try:
            from datetime import datetime, timezone

            from services.scheduling import compute_next_run

            nxt = compute_next_run(datetime.now(timezone.utc), scfg)
            return int(nxt.timestamp())
        except Exception:
            return 0

    def _scheduling_status(self, **_: Any) -> dict[str, Any]:
        scfg = self._scheduling_get()
        return {
            "ok": True,
            "mode": "local",
            "running": False,
            "degraded": True,
            "config": scfg,
            "next_run_at": self._next_run_at(scfg) if scfg.get("enabled") else 0,
        }

    def _scheduling_next(self, **_: Any) -> dict[str, Any]:
        scfg = self._scheduling_get()
        return {"ok": True, "next_run_at": self._next_run_at(scfg), "config": scfg}

    def _tokens_api(self) -> Any:
        try:
            from api import apiTokensAPI

            return apiTokensAPI
        except Exception as exc:
            raise CLIError(f"API token support unavailable: {exc}") from exc

    def _tokens_get(self, **_: Any) -> dict[str, Any]:
        mod = self._tokens_api()
        return {"ok": True, "tokens": mod.list_api_tokens(self._load())}

    def _tokens_post(self, *, body: Any = None, **_: Any) -> dict[str, Any]:
        mod = self._tokens_api()
        payload = as_dict(body)
        try:
            expires_days = int(payload.get("expires_days") or 0)
        except Exception:
            expires_days = 0
        raw, entry = mod.issue_api_token(
            self._load(),
            name=str(payload.get("name") or ""),
            user_id=str(payload.get("user_id") or ""),
            expires_days=expires_days,
        )
        return {"ok": True, "token": raw, "entry": entry}

    def _tokens_delete(self, *, token_id: str, **_: Any) -> dict[str, Any]:
        mod = self._tokens_api()
        if not mod.revoke_api_token(token_id):
            return {"ok": False, "error": "not_found"}
        return {"ok": True, "revoked": token_id}


LOCAL_CAPABLE_HINT = (
    "Locally the CLI can serve status, config, pairs, scheduling and token commands. "
    "Everything else needs the running service."
)


def now_ts() -> int:
    return int(time.time())
