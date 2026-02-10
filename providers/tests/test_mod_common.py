from __future__ import annotations


def test_parse_rate_limit_common_headers():
    import sync._mod_common as m

    headers = {
        "X-RateLimit-Limit": "100",
        "X-RateLimit-Remaining": "42",
        "X-RateLimit-Reset": "1700000000",
    }
    out = m.parse_rate_limit(headers)
    assert out == {"limit": 100, "remaining": 42, "reset": 1700000000}


def test_parse_rate_limit_missing_headers():
    import sync._mod_common as m

    out = m.parse_rate_limit({})
    assert out == {"limit": None, "remaining": None, "reset": None}
