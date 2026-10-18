# cw_platform/collection_media.py
# Provider-neutral collection media info (format, resolution, HDR, audio)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

FORMATS = ("4k_bluray", "bluray", "hd_dvd", "dvd", "digital", "vcd", "vhs", "betamax", "laserdisc")
RESOLUTIONS = ("uhd_4k", "hd_1080p", "hd_1080i", "hd_720p", "sd_576p", "sd_576i", "sd_480p", "sd_480i")
HDRS = ("dolby_vision", "hdr10_plus", "hdr10", "hlg")
AUDIOS = (
    "lpcm", "mp3", "mp2", "aac", "ogg", "ogg_opus", "wma", "flac", "dts", "dts_ma", "dts_hr", "dts_x", "auro_3d",
    "dolby_digital", "dolby_digital_plus", "dolby_digital_plus_atmos", "dolby_atmos", "dolby_truehd", "dolby_prologic",
)
CHANNELS = ("1.0", "2.0", "2.1", "3.0", "3.1", "4.0", "4.1", "5.0", "5.1", "5.1.2", "5.1.4", "6.1", "7.1", "7.1.2", "7.1.4", "9.1", "10.1")
MEDIA_KEYS = ("resolution", "hdr", "audio", "audio_channels", "is_3d")

_TRAKT_MEDIA_TYPES = ("digital", "bluray", "hddvd", "dvd", "vcd", "vhs", "betamax", "laserdisc")
_CHANNEL_COUNT_FALLBACK = {1: "1.0", 2: "2.0", 3: "2.1", 4: "4.0", 5: "5.0", 6: "5.1", 7: "6.1", 8: "7.1"}
_RESOLUTION_LABELS = {
    "uhd_4k": "4K", "hd_1080p": "1080p", "hd_1080i": "1080i", "hd_720p": "720p",
    "sd_576p": "576p", "sd_576i": "576i", "sd_480p": "480p", "sd_480i": "480i",
}
_HDR_LABELS = {"dolby_vision": "Dolby Vision", "hdr10_plus": "HDR10+", "hdr10": "HDR10", "hlg": "HLG"}
_AUDIO_LABELS = {
    "lpcm": "LPCM", "mp3": "MP3", "mp2": "MP2", "aac": "AAC", "ogg": "Ogg", "ogg_opus": "Opus", "wma": "WMA", "flac": "FLAC",
    "dts": "DTS", "dts_ma": "DTS-HD MA", "dts_hr": "DTS-HD HRA", "dts_x": "DTS:X", "auro_3d": "Auro-3D",
    "dolby_digital": "Dolby Digital", "dolby_digital_plus": "Dolby Digital Plus", "dolby_digital_plus_atmos": "DD+ Atmos",
    "dolby_atmos": "Dolby Atmos", "dolby_truehd": "Dolby TrueHD", "dolby_prologic": "Dolby Pro Logic",
}


def _text(value: Any) -> str:
    return "" if value is None or isinstance(value, bool) else str(value).strip()


def _compact(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", _text(value).lower())


def normalize_format(value: Any) -> str:
    compact = _compact(value)
    if "bluray" in compact or compact in {"bd", "uhd"}:
        return "4k_bluray" if any(tag in compact for tag in ("4k", "uhd", "ultrahd")) else "bluray"
    if compact == "hddvd":
        return "hd_dvd"
    return compact if compact in FORMATS else ""


def normalize_resolution(value: Any) -> str:
    low = _text(value).lower()
    if low in RESOLUTIONS:
        return low
    if re.search(r"(?<![a-z0-9])(4k|uhd)(?![a-z0-9])|(?<!\d)2160(?!\d)", low):
        return "uhd_4k"
    for lines in ("1080", "720", "576", "480"):
        match = re.search(rf"(?<!\d){lines}([ip])?(?![a-z0-9])", low)
        if match:
            if lines == "720":
                return "hd_720p"
            return f"{'hd' if lines == '1080' else 'sd'}_{lines}{match.group(1) or 'p'}"
    return ""


def normalize_hdr(value: Any) -> str:
    low = _text(value).lower()
    if low in HDRS:
        return low
    compact = _compact(low)
    if "dolbyvision" in compact or "dovi" in compact or compact == "dv" or compact.startswith("dvhdr"):
        return "dolby_vision"
    if "hdr10+" in re.sub(r"\s+", "", low) or "hdr10plus" in compact:
        return "hdr10_plus"
    if "hlg" in compact:
        return "hlg"
    if "hdr10" in compact or compact == "hdr":
        return "hdr10"
    return ""


def normalize_audio(value: Any) -> str:
    low = _text(value).lower()
    if low in AUDIOS:
        return low
    compact = _compact(low)
    if not compact:
        return ""
    atmos = "atmos" in compact
    if "dtsx" in compact:
        return "dts_x"
    if "dts" in compact:
        tokens = set(re.findall(r"[a-z0-9]+", low))
        if tokens & {"ma", "master"} or "dtshdma" in compact:
            return "dts_ma"
        return "dts_hr" if tokens & {"hra", "hr"} or "dtshdhra" in compact else "dts"
    if "truehd" in compact:
        return "dolby_atmos" if atmos else "dolby_truehd"
    if "dd+" in low or any(tag in compact for tag in ("ddp", "eac3", "dolbydigitalplus")):
        return "dolby_digital_plus_atmos" if atmos else "dolby_digital_plus"
    if atmos:
        return "dolby_atmos"
    if compact in {"dd", "ac3"} or "dolbydigital" in compact:
        return "dolby_digital"
    if "prologic" in compact:
        return "dolby_prologic"
    if "pcm" in compact:
        return "lpcm"
    if "auro" in compact:
        return "auro_3d"
    if "opus" in compact:
        return "ogg_opus"
    if compact == "vorbis":
        return "ogg"
    return compact if compact in AUDIOS else ""


def normalize_channels(value: Any) -> str:
    low = _text(value).lower().replace("ch", "").strip()
    if low in CHANNELS:
        return low
    if low == "stereo":
        return "2.0"
    if low == "mono":
        return "1.0"
    if not re.fullmatch(r"\d+(\.\d+){0,2}", low):
        return ""
    return _CHANNEL_COUNT_FALLBACK.get(sum(int(part) for part in low.split(".")), "")


def normalize_3d(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    low = _text(value).lower()
    if low in {"1", "true", "yes", "3d"}:
        return True
    if low in {"0", "false", "no"}:
        return False
    return None


_NORMALIZERS = {"resolution": normalize_resolution, "hdr": normalize_hdr, "audio": normalize_audio, "audio_channels": normalize_channels}


def media_block(values: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, normalize in _NORMALIZERS.items():
        raw = _text(values.get(key))
        if raw:
            out[key] = normalize(raw) or raw
    is_3d = normalize_3d(values.get("is_3d"))
    if is_3d is not None:
        out["is_3d"] = is_3d
    return out


def item_media(item: Mapping[str, Any]) -> dict[str, Any]:
    raw = item.get("media_info")
    return {k: raw[k] for k in MEDIA_KEYS if isinstance(raw, Mapping) and raw.get(k) not in (None, "")}


def from_trakt_metadata(metadata: Any) -> tuple[str, dict[str, Any]]:
    if not isinstance(metadata, Mapping):
        return "", {}
    fmt = normalize_format(metadata.get("media_type"))
    block = media_block({**metadata, "is_3d": True if metadata.get("3d") else None})
    return fmt, block


def to_trakt_metadata(item: Mapping[str, Any]) -> dict[str, Any]:
    media = item_media(item)
    out: dict[str, Any] = {}
    fmt = normalize_format(item.get("format"))
    if fmt == "4k_bluray":
        out["media_type"] = "bluray"
        media.setdefault("resolution", "uhd_4k")
    elif fmt == "hd_dvd":
        out["media_type"] = "hddvd"
    elif fmt in _TRAKT_MEDIA_TYPES:
        out["media_type"] = fmt
    for key, normalize in _NORMALIZERS.items():
        value = normalize(media.get(key))
        if value:
            out[key] = value
    if media.get("is_3d") is True:
        out["3d"] = True
    return out


def to_floppy_fields(item: Mapping[str, Any]) -> dict[str, Any]:
    media = item_media(item)
    out: dict[str, Any] = {}
    for key, field, labels, limit in (
        ("resolution", "resolution", _RESOLUTION_LABELS, 100),
        ("hdr", "hdr", _HDR_LABELS, 30),
        ("audio", "audio_codec", _AUDIO_LABELS, 30),
        ("audio_channels", "audio_channels", {}, 20),
    ):
        value = _text(media.get(key))
        if value:
            out[field] = str(labels.get(value, value))[:limit]
    if isinstance(media.get("is_3d"), bool):
        out["is_3d"] = media["is_3d"]
    return out


def punchplay_format(item: Mapping[str, Any]) -> str:
    fmt = normalize_format(item.get("format") or item.get("edition_format"))
    if fmt == "bluray" and item_media(item).get("resolution") == "uhd_4k":
        return "4k_bluray"
    return fmt
