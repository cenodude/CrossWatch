# providers/sync/_mod_TAUTULLI.py
# CrossWatch - Tautulli sync module (history only)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from ._mod_common import build_session, request_with_retries, safe_json

try:  # type: ignore[name-defined]
    ctx  # type: ignore[misc]
except Exception:
    ctx = None  # type: ignore[assignment]

__VERSION__ = "0.1.0"
__all__ = ["get_manifest", "OPS"]


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "TAUTULLI",
        "label": "Tautulli",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": False,
        "features": {"watchlist": False, "ratings": False, "history": True, "playlists": False},
        "requires": [],
        "capabilities": {"bidirectional": False, "provides_ids": True, "index_semantics": "present"},
        "description": "Plex monitoring (history only).",
    }


def _label(method: str, url: str, kw: Mapping[str, Any]) -> str:
    try:
        params = kw.get("params") if isinstance(kw, Mapping) else None
        if isinstance(params, Mapping):
            cmd = str(params.get("cmd") or "").lower()
            if cmd:
                return f"api:{cmd}"
    except Exception:
        pass
    return "api"


def _as_base(url: Any) -> str | None:
    s = str(url or "").strip()
    if not s:
        return None
    if not s.startswith(("http://", "https://")):
        s = "http://" + s
    return s.rstrip("/")


@dataclass(frozen=True)
class TAUTULLIConfig:
    server_url: str
    api_key: str
    verify_ssl: bool = True
    timeout: float = 10.0
    max_retries: int = 3


@dataclass(frozen=True)
class _HistoryAdapter:
    cfg: Mapping[str, Any]
    client: Any


class TAUTULLIClient:
    def __init__(self, cfg: TAUTULLIConfig, raw_cfg: Mapping[str, Any]):
        self.cfg = cfg
        self.raw_cfg = raw_cfg
        self.session = build_session("TAUTULLI", ctx, feature_label=_label)

    def _url(self) -> str:
        return f"{self.cfg.server_url}/api/v2"

    def call(self, cmd: str, **params: Any) -> Any:
        q: dict[str, Any] = {"apikey": self.cfg.api_key, "cmd": cmd}
        for k, v in (params or {}).items():
            if v is not None:
                q[k] = v

        r = request_with_retries(
            self.session,
            "GET",
            self._url(),
            params=q,
            timeout=self.cfg.timeout,
            max_retries=self.cfg.max_retries,
            verify=self.cfg.verify_ssl,
        )
        j = safe_json(r) or {}
        resp = j.get("response") if isinstance(j, dict) else None
        if isinstance(resp, dict):
            if str(resp.get("result") or "").lower() != "success":
                raise RuntimeError(str(resp.get("message") or "unknown error"))
            return resp.get("data")
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code}")
        return j


class TAUTULLIModule:
    def __init__(self, cfg: Mapping[str, Any]):
        t = dict(cfg.get("tautulli") or {})
        base = _as_base(t.get("server_url") or t.get("server"))
        key = str(t.get("api_key") or "").strip()
        if not base or not key:
            raise RuntimeError("Missing tautulli.server_url or tautulli.api_key")

        self.cfg = TAUTULLIConfig(
            server_url=base,
            api_key=key,
            verify_ssl=bool(t.get("verify_ssl", True)),
            timeout=float(t.get("timeout", cfg.get("timeout", 10.0))),
            max_retries=int(t.get("max_retries", cfg.get("max_retries", 3))),
        )
        self.client = TAUTULLIClient(self.cfg, cfg)

    @staticmethod
    def supported_features() -> dict[str, bool]:
        return {"watchlist": False, "ratings": False, "history": True, "playlists": False}

    def health(self) -> Mapping[str, Any]:
        try:
            self.client.call("get_server_info")
            return {"ok": True, "status": "ok"}
        except Exception as e:
            return {"ok": False, "status": "down", "reason": str(e)}

    def activities(self) -> Mapping[str, Any]:
        try:
            data = self.client.call("get_history", start=0, length=1, order_column="date", order_dir="desc") or {}
            rows: list[Any] = []
            if isinstance(data, Mapping) and isinstance(data.get("data"), list):
                rows = list(data.get("data") or [])
            ts: Any = None
            if rows and isinstance(rows[0], Mapping):
                ts = rows[0].get("date") or rows[0].get("started")
            return {"history": str(ts or "0"), "updated_at": str(ts or "0")}
        except Exception:
            return {"updated_at": "0"}

    def build_index(self, feature: str) -> Mapping[str, dict[str, Any]]:
        if feature != "history":
            return {}
        from .tautulli import _history
        adapter = _HistoryAdapter(cfg=self.client.raw_cfg, client=self.client)
        return _history.build_index(adapter)

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        from .tautulli import _history
        return _history.add(self, items, dry_run=dry_run)

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
        from .tautulli import _history
        return _history.remove(self, items, dry_run=dry_run)


class _TAUTULLIOPS:
    def name(self) -> str:
        return "TAUTULLI"

    def label(self) -> str:
        return "Tautulli"

    def features(self) -> Mapping[str, bool]:
        return TAUTULLIModule.supported_features()

    def capabilities(self) -> Mapping[str, Any]:
        return {"bidirectional": False, "provides_ids": True, "index_semantics": "present"}

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        t = cfg.get("tautulli") or {}
        return bool(_as_base(t.get("server_url") or t.get("server")) and str(t.get("api_key") or "").strip())

    def _adapter(self, cfg: Mapping[str, Any]) -> TAUTULLIModule:
        return TAUTULLIModule(cfg)

    def activities(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).activities()

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)


OPS = _TAUTULLIOPS()
