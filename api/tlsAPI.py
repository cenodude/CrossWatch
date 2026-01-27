# /api/tlsAPI.py
# CrossWatch - TLS API Endpoints
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from cw_platform.config_base import CONFIG, load_config, save_config
from cw_platform.tls import cert_info, ensure_self_signed_cert, resolve_tls_paths

router = APIRouter(prefix="/api/ui/tls", tags=["ui"])


class RegenBody(BaseModel):
    hostname: str | None = None
    valid_days: int | None = None
    alt_dns: list[str] | None = None
    alt_ips: list[str] | None = None


def _ensure_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for x in value:
        s = str(x).strip()
        if s:
            out.append(s)
    return out


def _cfg_tls(cfg: dict[str, Any]) -> dict[str, Any]:
    ui = cfg.get("ui")
    if not isinstance(ui, dict):
        ui = {}
        cfg["ui"] = ui
    tls = ui.get("tls")
    if not isinstance(tls, dict):
        tls = {}
        ui["tls"] = tls
    return tls


@router.get("/status")
def tls_status() -> dict[str, Any]:
    cfg = load_config()
    ui = _ensure_dict(cfg.get("ui"))
    tls_cfg = _ensure_dict(ui.get("tls"))

    cert_path, key_path = resolve_tls_paths(cfg)
    info: dict[str, Any] = cert_info(cert_path) if cert_path else {"exists": False}

    out_tls: dict[str, Any] = dict(tls_cfg)
    out_tls["resolved_cert_path"] = str(cert_path) if cert_path else ""
    out_tls["resolved_key_path"] = str(key_path) if key_path else ""
    out_tls["cert"] = info

    protocol = str(ui.get("protocol", "http") or "http")
    return {"protocol": protocol, "tls": out_tls}


@router.post("/regenerate")
def tls_regenerate(body: RegenBody) -> dict[str, Any]:
    cfg = load_config()
    tls_cfg = _cfg_tls(cfg)

    hostname = str(body.hostname or tls_cfg.get("hostname") or "localhost").strip() or "localhost"

    valid_days_raw = body.valid_days if body.valid_days is not None else tls_cfg.get("valid_days", 825)
    try:
        valid_days = int(valid_days_raw)
    except Exception:
        valid_days = 825
    if valid_days < 1:
        valid_days = 1
    if valid_days > 3650:
        valid_days = 3650

    alt_dns = _as_str_list(body.alt_dns if body.alt_dns is not None else tls_cfg.get("alt_dns"))
    alt_ips = _as_str_list(body.alt_ips if body.alt_ips is not None else tls_cfg.get("alt_ips"))

    # Persist settings
    tls_cfg["self_signed"] = True
    tls_cfg["hostname"] = hostname
    tls_cfg["valid_days"] = valid_days
    tls_cfg["alt_dns"] = alt_dns
    tls_cfg["alt_ips"] = alt_ips
    save_config(cfg)

    cert_path = CONFIG / "tls" / "crosswatch.crt"
    key_path = CONFIG / "tls" / "crosswatch.key"
    cert_path.parent.mkdir(parents=True, exist_ok=True)

    ensure_self_signed_cert(
        cert_path,
        key_path,
        hostname=hostname,
        valid_days=valid_days,
        alt_dns=[hostname, "localhost", *alt_dns],
        alt_ips=["127.0.0.1", *alt_ips],
        force=True,
    )

    return tls_status()


@router.get("/cert", response_class=FileResponse, response_model=None)
def tls_download_cert() -> FileResponse:
    cfg = load_config()
    cert_path, _ = resolve_tls_paths(cfg)
    if not cert_path:
        raise HTTPException(status_code=404, detail="No certificate configured")

    p = Path(cert_path)
    if not p.exists():
        raise HTTPException(status_code=404, detail="Certificate not found")

    return FileResponse(
        str(p),
        media_type="application/x-x509-ca-cert",
        filename=p.name or "crosswatch.crt",
    )
