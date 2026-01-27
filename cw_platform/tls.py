from __future__ import annotations

import ipaddress
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from cw_platform import config_base


def _resolve_cfg_path(raw: str, config_dir: Path) -> Path:
    p = Path(raw)
    if p.is_absolute():
        return p
    return (config_dir / p).resolve()


def resolve_tls_paths(cfg: dict[str, Any], config_dir: Path | None = None) -> tuple[Path, Path]:
    config_dir = config_dir or config_base.CONFIG
    ui = cfg.get("ui")
    tls = (ui or {}).get("tls") if isinstance(ui, dict) else None
    tls = tls if isinstance(tls, dict) else {}

    cert_file = str(tls.get("cert_file") or "").strip()
    key_file = str(tls.get("key_file") or "").strip()

    if cert_file:
        cert_path = _resolve_cfg_path(cert_file, config_dir)
    else:
        cert_path = (config_dir / "tls" / "crosswatch.crt").resolve()

    if key_file:
        key_path = _resolve_cfg_path(key_file, config_dir)
    else:
        key_path = (config_dir / "tls" / "crosswatch.key").resolve()

    cert_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.parent.mkdir(parents=True, exist_ok=True)
    return cert_path, key_path


def _dt_utc(dt: Any) -> datetime | None:
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


def cert_info(cert_path: Path) -> dict[str, Any]:
    if not cert_path.exists():
        return {"exists": False, "path": str(cert_path)}

    try:
        pem = cert_path.read_bytes()
        cert = x509.load_pem_x509_certificate(pem)
    except Exception as e:
        return {"exists": True, "path": str(cert_path), "error": f"Failed to parse certificate: {e}"}

    subj = cert.subject.rfc4514_string()
    iss = cert.issuer.rfc4514_string()
    sans: list[str] = []
    try:
        ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        san = ext.value
        for n in san:
            if isinstance(n, x509.DNSName):
                sans.append(str(n.value))
            elif isinstance(n, x509.IPAddress):
                sans.append(str(n.value))
    except Exception:
        pass
    not_before_raw = getattr(cert, "not_valid_before_utc", None) or getattr(cert, "not_valid_before", None)
    not_after_raw = getattr(cert, "not_valid_after_utc", None) or getattr(cert, "not_valid_after", None)
    not_before_dt = _dt_utc(not_before_raw)
    not_after_dt = _dt_utc(not_after_raw)

    return {
        "exists": True,
        "path": str(cert_path),
        "subject": subj,
        "issuer": iss,
        "self_signed": subj == iss,
        "not_before": (not_before_dt.isoformat() if not_before_dt else None),
        "not_after": (not_after_dt.isoformat() if not_after_dt else None),
        "sans": sans,
        "sha256": cert.fingerprint(hashes.SHA256()).hex(),
    }


def ensure_self_signed_cert(
    cert_path: Path,
    key_path: Path,
    *,
    hostname: str,
    valid_days: int = 825,
    alt_dns: list[str] | None = None,
    alt_ips: list[str] | None = None,
    force: bool = False,
) -> None:
    if not force and cert_path.exists() and key_path.exists():
        return

    valid_days = int(valid_days or 825)
    if valid_days < 1:
        valid_days = 1
    if valid_days > 3650:
        valid_days = 3650

    hostname = (hostname or "").strip() or "localhost"

    dns_names = {hostname, "localhost"}
    for d in (alt_dns or []):
        d = str(d or "").strip()
        if d:
            dns_names.add(d)

    ip_addrs: set[ipaddress.IPv4Address | ipaddress.IPv6Address] = set()
    for ip_str in (alt_ips or []):
        try:
            ip_addrs.add(ipaddress.ip_address(str(ip_str).strip()))
        except Exception:
            continue
    try:
        ip_addrs.add(ipaddress.ip_address("127.0.0.1"))
    except Exception:
        pass

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, hostname)])
    now = datetime.now(timezone.utc)

    san_items: list[x509.GeneralName] = [x509.DNSName(d) for d in sorted(dns_names)]
    san_items.extend([x509.IPAddress(ip) for ip in sorted(ip_addrs, key=lambda x: (x.version, x.packed))])

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=valid_days))
        .add_extension(x509.SubjectAlternativeName(san_items), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .sign(private_key=key, algorithm=hashes.SHA256())
    )

    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )

    try:
        os.chmod(key_path, 0o600)
    except Exception:
        pass


def regenerate_self_signed(cfg: dict[str, Any], *, force: bool = True) -> tuple[Path, Path]:
    config_dir = config_base.CONFIG
    cert_path, key_path = resolve_tls_paths(cfg, config_dir)

    ui = cfg.get("ui") if isinstance(cfg.get("ui"), dict) else {}
    tls = (ui or {}).get("tls") if isinstance(ui, dict) else None
    tls = tls if isinstance(tls, dict) else {}

    hostname = str(tls.get("hostname") or "localhost").strip() or "localhost"
    try:
        valid_days = int(tls.get("valid_days") or 825)
    except Exception:
        valid_days = 825

    alt_dns = tls.get("alt_dns")
    if not isinstance(alt_dns, list):
        alt_dns = []
    alt_ips = tls.get("alt_ips")
    if not isinstance(alt_ips, list):
        alt_ips = []

    ensure_self_signed_cert(
        cert_path,
        key_path,
        hostname=hostname,
        valid_days=valid_days,
        alt_dns=[str(x) for x in alt_dns],
        alt_ips=[str(x) for x in alt_ips],
        force=force,
    )
    return cert_path, key_path
