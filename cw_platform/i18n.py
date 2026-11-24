# cw_platform/i18n.py
# CrossWatch - Shared UI translation catalogs, language selection and server-side lookup
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import html as html_lib
import json
import logging
import re
import threading
from pathlib import Path
from typing import Any, Callable, Mapping

DEFAULT_LANGUAGE = "en"
LANGUAGE_COOKIE = "cw_lang"
LOCALES_DIR = Path(__file__).resolve().parent.parent / "assets" / "locales"
PLURAL_CATEGORIES = ("zero", "one", "two", "few", "many", "other")

_LOG = logging.getLogger("crosswatch.i18n")
_CODE_RE = re.compile(r"^[a-z]{2,3}(?:-[a-z0-9]{2,8})?$")
_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9_-]*$")
_TOKEN_RE = re.compile(r"\[\[t:([A-Za-z0-9_.-]+)((?:\|[A-Za-z0-9_]+[@=][^|\[\]]*)*)\]\]")
_TOKEN_ARG_RE = re.compile(r"\|([A-Za-z0-9_]+)([@=])([^|\[\]]*)")
_VAR_RE = re.compile(r"\{\{\s*([A-Za-z0-9_]+)\s*\}\}")
_LOCK = threading.Lock()
_CACHE: dict[str, tuple[Any, Any]] = {}
_REPORTED: set[str] = set()


class CatalogError(ValueError):
    pass


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise CatalogError(f"duplicate key {key!r}")
        out[key] = value
    return out


def read_catalog_file(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_reject_duplicates)
    if not isinstance(data, dict):
        raise CatalogError(f"{path.name} must contain a JSON object")
    return data


def _signature(paths: list[Path]) -> tuple[tuple[str, int, int], ...]:
    out = []
    for path in paths:
        try:
            stat = path.stat()
            out.append((str(path), int(stat.st_mtime_ns), int(stat.st_size)))
        except OSError:
            out.append((str(path), 0, 0))
    return tuple(out)


def _cached(name: str, paths: list[Path], build: Callable[[], Any]) -> Any:
    sig = _signature(paths)
    with _LOCK:
        hit = _CACHE.get(name)
        if hit and hit[0] == sig:
            return hit[1]
    value = build()
    with _LOCK:
        _CACHE[name] = (sig, value)
    return value


def _report(message: str) -> None:
    if message in _REPORTED:
        return
    _REPORTED.add(message)
    _LOG.warning(message)


def _registry_path() -> Path:
    return LOCALES_DIR / "languages.json"


def _load_registry() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    try:
        data = read_catalog_file(_registry_path())
        for row in data.get("languages") or []:
            if not isinstance(row, dict):
                continue
            code = str(row.get("code") or "").strip().lower()
            if not _CODE_RE.match(code) or any(item["code"] == code for item in rows):
                continue
            rows.append({"code": code, "name": str(row.get("name") or code)})
    except Exception as exc:
        _report(f"language registry unavailable: {exc}")
    if not any(row["code"] == DEFAULT_LANGUAGE for row in rows):
        rows.insert(0, {"code": DEFAULT_LANGUAGE, "name": "English"})
    return rows


def registry() -> list[dict[str, str]]:
    return _cached("registry", [_registry_path()], _load_registry)


def languages() -> dict[str, str]:
    return {row["code"]: row["name"] for row in registry()}


def language_or_none(value: Any) -> str | None:
    code = str(value or "").strip().lower()
    return next((known for known in languages() if known == code), None)


def clean_language(value: Any) -> str:
    return language_or_none(value) or DEFAULT_LANGUAGE


def catalog_paths(code: str) -> list[Path]:
    if not _CODE_RE.match(str(code or "")):
        return []
    folder = LOCALES_DIR / code
    return sorted(path for path in folder.glob("*.json") if _NAMESPACE_RE.match(path.stem))


def flatten(tree: Mapping[str, Any], prefix: str = "") -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in tree.items():
        name = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, Mapping):
            out.update(flatten(value, name))
        elif isinstance(value, str):
            out[name] = value
        else:
            raise CatalogError(f"{name} must be a string or an object")
    return out


def _load_messages(code: str) -> dict[str, str]:
    out: dict[str, str] = {}
    owners: dict[str, str] = {}
    for path in catalog_paths(code):
        try:
            data = read_catalog_file(path)
            clash = sorted(name for name in data if name in owners)
            if clash:
                raise CatalogError(f"{', '.join(clash)} already defined in {owners[clash[0]]}")
            messages_in_file = flatten(data)
        except Exception as exc:
            _report(f"translation catalog {code}/{path.name} ignored: {exc}")
            continue
        owners.update({name: path.name for name in data})
        out.update(messages_in_file)
    return out


def messages(code: str) -> dict[str, str]:
    code = clean_language(code)
    return _cached(f"messages:{code}", [_registry_path(), *catalog_paths(code)], lambda: _load_messages(code))


def plural_category(code: str, count: Any) -> str:
    try:
        number = float(count)
    except (TypeError, ValueError):
        return "other"
    return "one" if number == 1 else "other"


def _lookup(code: str, key: str, count: Any = None) -> str | None:
    names = [key] if count is None else [f"{key}_{plural_category(code, count)}", f"{key}_other", key]
    for lang in dict.fromkeys((clean_language(code), DEFAULT_LANGUAGE)):
        table = messages(lang)
        for name in names:
            if name in table:
                return table[name]
    return None


def interpolate(text: str, values: Mapping[str, Any] | None) -> str:
    if not values:
        return text
    return _VAR_RE.sub(lambda match: str(values[match.group(1)]) if match.group(1) in values else match.group(0), text)


def t(code: str, key: str, values: Mapping[str, Any] | None = None, **kwargs: Any) -> str:
    merged = {**(values or {}), **kwargs}
    text = _lookup(code, key, merged.get("count"))
    if text is None:
        _report(f"missing translation key {key}")
        return key
    return interpolate(text, merged)


def h(code: str, key: str, values: Mapping[str, Any] | None = None, **kwargs: Any) -> str:
    return html_lib.escape(t(code, key, values, **kwargs), quote=True)


def _token_text(code: str, match: re.Match[str]) -> str:
    values: dict[str, Any] = {}
    for name, kind, raw in _TOKEN_ARG_RE.findall(match.group(2) or ""):
        values[name] = t(code, raw) if kind == "@" else raw
    if "count" in values:
        try:
            values["count"] = int(values["count"])
        except ValueError:
            pass
    return h(code, match.group(1), values)


def token(key: str, **values: Any) -> str:
    args = "".join(f"|{name}{'@' + str(value)[1:] if str(value).startswith('@') else '=' + str(value)}" for name, value in values.items())
    return f"[[t:{key}{args}]]"


def localize_html(html: str, code: str) -> str:
    return _TOKEN_RE.sub(lambda match: _token_text(code, match), html)


def _script_json(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return text.replace("<", "\\u003c").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")


def bundle_js(code: str) -> str:
    code = clean_language(code)

    def build() -> str:
        payload = {"code": code, "name": languages().get(code, code), "messages": messages(code)}
        meta = {"default": DEFAULT_LANGUAGE, "cookie": LANGUAGE_COOKIE, "languages": registry()}
        return (
            f"(window.__cwLocales=window.__cwLocales||{{}})[{_script_json(code)}]={_script_json(payload)};\n"
            f"window.__cwLocaleMeta={_script_json(meta)};\n"
        )

    return _cached(f"bundle:{code}", [_registry_path(), *catalog_paths(code)], build)


def user_language(user: Any) -> str:
    prefs = user.get("preferences") if isinstance(user, dict) else None
    return clean_language(prefs.get("language") if isinstance(prefs, dict) else None)


def cookie_language(request: Any) -> str:
    try:
        value = request.cookies.get(LANGUAGE_COOKIE) if request is not None else None
    except Exception:
        value = None
    return clean_language(value)


def request_language(request: Any = None, user: Any = None) -> str:
    if isinstance(user, dict):
        return user_language(user)
    return cookie_language(request)


def language_options_html(current: str) -> str:
    code = clean_language(current)
    return "".join(
        f'<option value="{html_lib.escape(row["code"], quote=True)}" lang="{html_lib.escape(row["code"], quote=True)}"{" selected" if row["code"] == code else ""}>{html_lib.escape(row["name"])}</option>'
        for row in registry()
    )


def done_page(code: str, title_key: str) -> str:
    lang = clean_language(code)
    return (
        f'<!doctype html><html lang="{lang}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{h(lang, title_key)}</title></head>'
        '<body style="font-family:Segoe UI,Arial,sans-serif;background:#08111d;color:#eef5ff;display:grid;place-items:center;min-height:100vh;margin:0">'
        f'<div style="text-align:center"><div style="font-size:28px;margin-bottom:8px">{h(lang, "auth.popup.done")}</div><div style="opacity:.8">{h(lang, "auth.popup.close")}</div></div>'
        "<script>try{window.close()}catch(e){}</script></body></html>"
    )


def head_tags(code: str, version: str, *, signed_in: bool = False) -> str:
    code = clean_language(code)
    ver = html_lib.escape(str(version or ""), quote=True)
    failed = "(window.__cwLocaleFailed=window.__cwLocaleFailed||[]).push(this.dataset.cwLocale)"
    tags = [
        f'<script>window.__cwLanguage={{"code":"{code}","account":{"true" if signed_in else "false"}}};</script>',
        f'<script src="/assets/locales/{DEFAULT_LANGUAGE}.js?v={ver}" data-cw-locale="{DEFAULT_LANGUAGE}" onerror="{failed}"></script>',
    ]
    if code != DEFAULT_LANGUAGE:
        tags.append(f'<script src="/assets/locales/{code}.js?v={ver}" data-cw-locale="{code}" onerror="{failed}"></script>')
    tags.append(f'<script src="/assets/helpers/i18n.js?v={ver}"></script>')
    tags.append('<script>window.T=window.T||function(k){return String(k)};window.TH=window.TH||function(k){return String(k)};</script>')
    return "\n".join(tags)
