# tests/i18n_source.py
# CrossWatch - Resolves translation calls in frontend source to English for copy assertions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import re
from pathlib import Path

from cw_platform import i18n

_CALL = re.compile(r'\b(?:window\.)?TH?\(\s*"([a-z][\w.-]*)"\s*\)')
_INLINE = re.compile(r'\$\{\s*(?:window\.)?TH?\(\s*"([a-z][\w.-]*)"\s*\)\s*\}')
_CONCAT = re.compile(r"""(["'])\s*\+\s*(?:window\.)?TH?\(\s*"([a-z][\w.-]*)"\s*\)\s*\+\s*\1""")
_VARS = re.compile(r'\b((?:window\.)?TH?)\(\s*"([a-z][\w.-]*)"\s*,')
_TOKEN = re.compile(r"\[\[t:([A-Za-z0-9_.-]+)\]\]")
_RICH_CALL = re.compile(r'\$\{\s*(?:window\.)?TR\(\s*"([a-z][\w.-]*)"\s*\)\s*\}|\b(?:window\.)?TR\(\s*"([a-z][\w.-]*)"\s*\)')
_RICH = re.compile(r"\[\[(/?)(b|strong|i|em|code|br)\]\]")


def _rich(messages: dict, match: re.Match) -> str:
    key = match.group(1) or match.group(2)
    if key not in messages:
        return match.group(0)
    return _RICH.sub(lambda m: f"<{m.group(1)}{m.group(2)}>", messages[key])


def _with_vars(messages: dict, match: re.Match) -> str:
    key = match.group(2)
    value = messages.get(key, messages.get(f"{key}_other"))
    if value is None:
        return match.group(0)
    return f"{match.group(1)}({json.dumps(value, ensure_ascii=False)},"


def english_source(text: str) -> str:
    messages = i18n.messages("en")
    text = _RICH_CALL.sub(lambda m: _rich(messages, m), text)
    text = _CONCAT.sub(lambda m: messages.get(m.group(2), m.group(0)), text)
    text = _INLINE.sub(lambda m: messages.get(m.group(1), m.group(0)), text)
    text = _CALL.sub(lambda m: json.dumps(messages[m.group(1)], ensure_ascii=False) if m.group(1) in messages else m.group(0), text)
    text = _VARS.sub(lambda m: _with_vars(messages, m), text)
    return _TOKEN.sub(lambda m: messages.get(m.group(1), m.group(0)), text)


def read_english(path: str | Path) -> str:
    return english_source(Path(path).read_text(encoding="utf-8"))
