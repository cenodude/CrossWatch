# CrossWatch test scripts
from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_nuvio_modules_are_registered() -> None:
    from cw_platform.modules_registry import MODULES

    assert MODULES["AUTH"]["_auth_NUVIO"] == "providers.auth._auth_NUVIO"
    assert MODULES["SYNC"]["_mod_NUVIO"] == "providers.sync._mod_NUVIO"


def test_nuvio_auth_category_order_and_membership() -> None:
    from providers.auth.registry import auth_providers_html

    html = auth_providers_html()
    media = html.index('id="sec-auth-media"')
    trackers = html.index('id="sec-auth-trackers"')
    clients = html.index('id="sec-auth-clients"')
    others = html.index('id="sec-auth-others"')

    assert media < trackers < clients < others
    assert html.index('id="sec-nuvio"') > clients
    assert 'id="sec-nuvio"' not in html[trackers:clients]


def test_nuvio_provider_metadata_uses_png_and_generic_fallback() -> None:
    meta = (ROOT / "assets" / "helpers" / "provider-meta.js").read_text(encoding="utf-8")
    ui = (ROOT / "assets" / "helpers" / "providers-ui.js").read_text(encoding="utf-8")
    loader = (ROOT / "assets" / "auth" / "auth_loader.js").read_text(encoding="utf-8")

    assert 'logoFile: "NUVIO.png"' in meta
    assert 'authGroupId: "sec-auth-clients"' in meta
    assert 'const file = info.logoFile || `${info.key}.svg`;' in meta
    assert 'NUVIO.svg' not in meta
    assert 'NUVIO.svg' not in ui
    assert 'if (provider === "NUVIO")' not in meta
    assert 'if (info.key === "NUVIO")' not in meta
    assert 'nuvio: "/assets/auth/auth.nuvio.js"' in loader
    assert 'PLEX: { key: "PLEX"' in meta
    assert 'logoFile: "PLEX.svg"' not in meta


def test_official_nuvio_png_asset_exists() -> None:
    logo = ROOT / "assets" / "img" / "NUVIO.png"

    assert logo.exists()
    assert logo.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
