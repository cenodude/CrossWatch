# /cli/_util.py
# CrossWatch - CLI helpers for config paths, values and formatting
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from html import unescape
from typing import Any

from ._errors import CLIError, EXIT_USAGE

_INDEX_RE = re.compile(r"^\[(\d+)\]$")
TRUE_WORDS = {"1", "true", "yes", "on", "enable", "enabled"}
FALSE_WORDS = {"0", "false", "no", "off", "disable", "disabled"}


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def rows_from_payload(payload: Any, *keys: str) -> list[dict[str, Any]]:
    def _coerce_rows(value: Any) -> list[dict[str, Any]]:
        if isinstance(value, dict):
            rows: list[dict[str, Any]] = []
            for key, item in value.items():
                row = dict(item) if isinstance(item, dict) else {"value": item}
                row.setdefault("key", str(key))
                rows.append(row)
            return rows
        if isinstance(value, list):
            rows = []
            for item in value:
                if isinstance(item, dict):
                    rows.append(dict(item))
                    continue
                text = str(item or "").strip()
                if text:
                    rows.append({"value": text, "name": text, "provider": text})
            return rows
        return []

    block = as_dict(payload)
    for key in keys:
        if key in block:
            return _coerce_rows(block.get(key))
    if keys:
        return _coerce_rows(payload) if isinstance(payload, list) else []
    return _coerce_rows(payload)


def error_text(payload: Any, default: str = "Rejected") -> str:
    block = payload if isinstance(payload, dict) else {}
    for key in ("message", "detail", "error", "status"):
        value = block.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    if block.get("found") is False:
        return "not found"
    return default


def split_path(path: str) -> list[str]:
    raw = str(path or "").strip()
    if not raw:
        raise CLIError("A config path is required, for example sync.anime.enabled", exit_code=EXIT_USAGE)
    parts: list[str] = []
    for chunk in raw.replace("/", ".").split("."):
        chunk = chunk.strip()
        if not chunk:
            continue
        match = re.fullmatch(r"([^\[\]]*)((?:\[\d+\])*)", chunk)
        if match is None:
            parts.append(chunk)
            continue
        head, indexes = match.group(1), match.group(2)
        if head:
            parts.append(head)
        for pos in re.findall(r"\[(\d+)\]", indexes):
            parts.append(f"[{pos}]")
    if not parts:
        raise CLIError(f"Cannot parse config path '{raw}'", exit_code=EXIT_USAGE)
    return parts


def dotted_get(data: Any, path: str, default: Any = None) -> Any:
    node = data
    for part in split_path(path):
        index = _INDEX_RE.match(part)
        if index is not None:
            if not isinstance(node, list):
                return default
            pos = int(index.group(1))
            if pos >= len(node):
                return default
            node = node[pos]
            continue
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
    return node


def dotted_payload(path: str, value: Any) -> dict[str, Any]:
    parts = split_path(path)
    if any(_INDEX_RE.match(p) for p in parts):
        raise CLIError("List indexes cannot be set directly; set the whole list with --json", exit_code=EXIT_USAGE)
    out: dict[str, Any] = {}
    node = out
    for part in parts[:-1]:
        nxt: dict[str, Any] = {}
        node[part] = nxt
        node = nxt
    node[parts[-1]] = value
    return out


def dotted_delete(data: dict[str, Any], path: str) -> bool:
    parts = split_path(path)
    node: Any = data
    for part in parts[:-1]:
        if not isinstance(node, dict) or part not in node:
            return False
        node = node[part]
    if not isinstance(node, dict) or parts[-1] not in node:
        return False
    node.pop(parts[-1], None)
    return True


def parse_value(raw: str, *, as_json: bool = False) -> Any:
    text = "" if raw is None else str(raw)
    if as_json:
        try:
            return json.loads(text)
        except Exception as exc:
            raise CLIError(f"Not valid JSON: {text}", exit_code=EXIT_USAGE) from exc
    low = text.strip().lower()
    if low in TRUE_WORDS:
        return True
    if low in FALSE_WORDS:
        return False
    if low in ("null", "none", "~"):
        return None
    stripped = text.strip()
    if re.fullmatch(r"[+-]?\d+", stripped):
        try:
            return int(stripped)
        except Exception:
            return stripped
    if re.fullmatch(r"[+-]?\d*\.\d+", stripped):
        try:
            return float(stripped)
        except Exception:
            return stripped
    return text


def coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in TRUE_WORDS


def fmt_ts(value: Any, *, empty: str = "-") -> str:
    try:
        epoch = int(float(value or 0))
    except Exception:
        return empty
    if epoch <= 0:
        return empty
    if epoch > 1_000_000_000_000:
        epoch = epoch // 1000
    try:
        return datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return empty


def fmt_rel(value: Any, *, empty: str = "-") -> str:
    try:
        epoch = int(float(value or 0))
    except Exception:
        return empty
    if epoch <= 0:
        return empty
    if epoch > 1_000_000_000_000:
        epoch = epoch // 1000
    delta = int(epoch - datetime.now(tz=timezone.utc).timestamp())
    ahead = delta >= 0
    delta = abs(delta)
    text = fmt_duration(delta)
    return f"in {text}" if ahead else f"{text} ago"


def fmt_duration(seconds: Any) -> str:
    try:
        total = int(float(seconds or 0))
    except Exception:
        return "-"
    if total < 0:
        total = 0
    if total < 60:
        return f"{total}s"
    minutes, sec = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m {sec}s" if sec else f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m" if minutes else f"{hours}h"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h" if hours else f"{days}d"


def parse_iso(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return int(datetime.fromisoformat(text).timestamp())
    except Exception:
        return 0


def pair_label(pair: dict[str, Any]) -> str:
    source = str(pair.get("source") or "?").upper()
    target = str(pair.get("target") or "?").upper()
    mode = str(pair.get("mode") or "one-way").strip().lower()
    arrow = "<->" if mode in ("two-way", "both", "bidirectional") else "->"
    return f"{source} {arrow} {target}"


def pair_features(pair: dict[str, Any]) -> list[str]:
    features = pair.get("features")
    if not isinstance(features, dict):
        return []
    out: list[str] = []
    for name, block in features.items():
        if isinstance(block, dict):
            if coerce_bool(block.get("enable", block.get("enabled")), False):
                out.append(str(name))
        elif coerce_bool(block, False):
            out.append(str(name))
    return sorted(out)


def find_pair(pairs: list[dict[str, Any]], needle: str) -> dict[str, Any]:
    want = str(needle or "").strip()
    if not want:
        raise CLIError("A pair id is required", exit_code=EXIT_USAGE)
    for pair in pairs:
        if str(pair.get("id") or "") == want:
            return pair
    if re.fullmatch(r"\d+", want):
        index = int(want)
        if 1 <= index <= len(pairs):
            return pairs[index - 1]
    lowered = want.lower()
    partial = [p for p in pairs if str(p.get("id") or "").lower().startswith(lowered)]
    if len(partial) == 1:
        return partial[0]
    if len(partial) > 1:
        ids = ", ".join(str(p.get("id")) for p in partial[:6])
        raise CLIError(f"Pair id '{want}' is ambiguous", hint=f"Matches: {ids}", exit_code=EXIT_USAGE)
    labelled = [p for p in pairs if pair_label(p).lower().replace(" ", "") == lowered.replace(" ", "")]
    if len(labelled) == 1:
        return labelled[0]
    raise CLIError(f"No pair matches '{want}'", hint="Run 'cw sync list' to see the configured pairs.", exit_code=5)


LOG_CONTROL_LINES = {"::CLEAR::"}
_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
_SPAN_RE = re.compile(r"</?span\b[^>]*>", re.IGNORECASE)


def is_log_control(line: str) -> bool:
    return str(line or "").strip() in LOG_CONTROL_LINES


def strip_ansi(text: str) -> str:
    cleaned = _ANSI_RE.sub("", str(text or ""))
    cleaned = _SPAN_RE.sub("", cleaned)
    return unescape(cleaned)
