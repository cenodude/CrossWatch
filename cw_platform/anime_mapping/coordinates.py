# /cw_platform/anime_mapping/coordinates.py
# CrossWatch - Anime Mapping Coordinate Ranges
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
from typing import Any

_SPAN_RE = re.compile(r"^(\d+)(-(\d+)?)?$")
_OPEN = 1 << 30

Span = tuple[int, int | None]


def parse_source(spec: Any) -> Span | None:
    text = str(spec or "").strip()
    m = _SPAN_RE.fullmatch(text)
    if not m:
        return None
    lo = int(m.group(1))
    if lo <= 0:
        return None
    if not m.group(2):
        return (lo, lo)
    hi = m.group(3)
    if hi is None:
        return (lo, None)
    hi_i = int(hi)
    return (lo, hi_i) if hi_i >= lo else None


def parse_target(spec: Any) -> list[Span] | None:
    text = str(spec or "").strip()
    if not text or "|" in text:
        return None
    out: list[Span] = []
    for part in text.split(","):
        span = parse_source(part)
        if span is None:
            return None
        out.append(span)
    return out or None


def covers(spec: Any, episode: Any) -> bool:
    span = parse_source(spec)
    ep = _as_int(episode)
    if span is None or ep is None:
        return False
    lo, hi = span
    return ep >= lo and (hi is None or ep <= hi)


def width(span: Span) -> int:
    lo, hi = span
    return _OPEN if hi is None else (hi - lo + 1)


def translate(source_spec: Any, target_spec: Any, episode: Any) -> int | None:
    ep = _as_int(episode)
    src = parse_source(source_spec)
    if ep is None or src is None:
        return None
    lo, hi = src
    if ep < lo or (hi is not None and ep > hi):
        return None
    spans = parse_target(target_spec)
    if not spans:
        return None
    offset = ep - lo
    for span in spans:
        size = width(span)
        if offset < size:
            return span[0] + offset
        offset -= size
    return None


def _as_int(value: Any) -> int | None:
    try:
        out = int(str(value).strip())
    except Exception:
        return None
    return out if out > 0 else None
