from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_webhook_parse_failure_log_omits_raw_body_snippet() -> None:
    from api import scrobbleAPI

    raw = b'{"Account":{"title":"secret-user"},"Metadata":{"title":"Private Movie"}}'
    msg = scrobbleAPI._webhook_parse_failure_log(
        "plex-webhook",
        "plex",
        "application/json",
        raw,
        ValueError("parser exploded near Private Movie"),
    )

    assert "body[:200]" not in msg
    assert "secret-user" not in msg
    assert "Private Movie" not in msg
    assert "parser exploded" not in msg
    assert "provider=plex" in msg
    assert "content-type='application/json'" in msg
    assert f"bytes={len(raw)}" in msg
    assert "error_class=ValueError" in msg


def test_webhook_parse_failure_handlers_do_not_log_body_prefix() -> None:
    source = (ROOT / "api" / "scrobbleAPI.py").read_text(encoding="utf-8")

    assert "body[:200]" not in source
    assert "raw[:200]" not in source
