# cw_platform/collection_media.py
# Provider-neutral collection media info (format, resolution, HDR, audio)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
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
    tokens = set(re.findall(r"[a-z0-9]+", low))
    atmos = "atmos" in compact or "joc" in tokens or ("mlp" in tokens and "16ch" in compact)
    if "dts" in compact or "dca" in tokens:
        if "xll" in tokens:
            return "dts_x" if "x" in tokens else "dts_ma"
        if "xbr" in tokens:
            return "dts_hr"
        if re.search(r"dts[\s:-]*x(?![a-z0-9])", low):
            return "dts_x"
        if tokens & {"ma", "master"} or "dtshdma" in compact:
            return "dts_ma"
        return "dts_hr" if tokens & {"hra", "hr"} or "dtshdhra" in compact else "dts"
    if "truehd" in compact or "mlp" in tokens:
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
    if "aac" in tokens:
        return "aac"
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


def _int(value: Any) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def resolution_from_dimensions(width: Any, height: Any, *, interlaced: bool = False) -> str:
    w, h = _int(width), _int(height)
    if not w and not h:
        return ""
    scan = "i" if interlaced else "p"
    if w >= 3200 or h >= 2000:
        return "uhd_4k"
    if w >= 1800 or h >= 1000:
        return f"hd_1080{scan}"
    if w >= 1200 or h >= 700:
        return "hd_720p"
    return f"sd_576{scan}" if h > 480 else f"sd_480{scan}"


def plex_media_info(row: Mapping[str, Any], hdr: str = "") -> dict[str, Any]:
    medias = [m for m in row.get("Media") or [] if isinstance(m, Mapping)]
    if not medias:
        return {}
    media = max(medias, key=lambda m: (_int(m.get("width")), _int(m.get("bitrate"))))
    interlaced = _text(media.get("videoResolution")).lower().endswith("i")
    resolution = resolution_from_dimensions(media.get("width"), media.get("height"), interlaced=interlaced) or normalize_resolution(media.get("videoResolution"))
    audio = " ".join(str(media[k]) for k in ("audioCodec", "audioProfile") if media.get(k))
    return media_block({"resolution": resolution, "hdr": hdr, "audio": audio, "audio_channels": media.get("audioChannels")})


_LOSSLESS_AUDIO = {"lpcm", "flac", "dolby_truehd", "dolby_atmos", "dts_ma", "dts_x"}
_OBJECT_AUDIO = {"dolby_atmos", "dolby_digital_plus_atmos", "dts_x", "auro_3d"}
_SKIPPED_AUDIO_WORDS = ("commentary", "commentaire", "kommentar", "description", "descriptive", "isolated", "music & effects", "music and effects", "m&e")


def _item_streams(item: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    sources = [s for s in item.get("MediaSources") or [] if isinstance(s, Mapping)]
    streams = sources[0].get("MediaStreams") if sources else None
    return [s for s in (streams or item.get("MediaStreams") or []) if isinstance(s, Mapping)]


def stream_hdr(video: Mapping[str, Any]) -> str:
    range_type, extended, video_range = _compact(video.get("VideoRangeType")), _compact(video.get("ExtendedVideoType")), _compact(video.get("VideoRange"))
    transfer = _text(video.get("ColorTransfer")).lower()
    if (range_type.startswith("dovi") and range_type != "doviinvalid") or "dolbyvision" in extended or "dolbyvision" in video_range:
        return "dolby_vision"
    if "hdr10plus" in range_type or "hdr10plus" in extended:
        return "hdr10_plus"
    if range_type == "hlg" or "hyperloggamma" in extended or "hlg" in video_range or transfer == "arib-std-b67":
        return "hlg"
    if "hdr10" in range_type or "hdr10" in extended or "hdr" in video_range or transfer == "smpte2084":
        return "hdr10"
    return ""


def stream_audio(stream: Mapping[str, Any]) -> str:
    base = normalize_audio(" ".join(_text(stream.get(k)) for k in ("Codec", "Profile") if _text(stream.get(k))))
    title = "" if "AudioSpatialFormat" in stream else _audio_title(stream)
    spatial = _compact(stream.get("AudioSpatialFormat"))
    if (spatial == "dtsx" or re.search(r"dts[\s:-]*x(?![a-z0-9])", title)) and base.startswith("dts"):
        return "dts_x"
    if spatial == "dolbyatmos" or "atmos" in title:
        return {"dolby_truehd": "dolby_atmos", "dolby_digital_plus": "dolby_digital_plus_atmos"}.get(base, base)
    return base


def _audio_title(stream: Mapping[str, Any]) -> str:
    return f"{_text(stream.get('Title'))} {_text(stream.get('DisplayTitle'))}".lower()


def _audio_language(stream: Mapping[str, Any]) -> str:
    lang = _text(stream.get("Language")).lower()
    return "" if lang in {"und", "unk"} else lang


def _audio_rank(stream: Mapping[str, Any], value: str) -> tuple[int, int, int]:
    return ((2 if value in _LOSSLESS_AUDIO else 0) + (1 if value in _OBJECT_AUDIO else 0), _int(stream.get("Channels")), _int(stream.get("BitRate")))


def _best_audio(streams: list[Mapping[str, Any]]) -> tuple[str, str]:
    audio = [s for s in streams if _text(s.get("Type")).lower() == "audio"]
    main = [s for s in audio if _audio_language(s) != "zxx" and not any(w in _audio_title(s) for w in _SKIPPED_AUDIO_WORDS)] or audio
    if not main:
        return "", ""
    default = next((s for s in main if s.get("IsDefault") is True), main[0])
    best, best_value = default, stream_audio(default)
    lang, channels = _audio_language(default), _int(default.get("Channels"))
    for stream in main:
        value = stream_audio(stream)
        if stream is default or not (value in _LOSSLESS_AUDIO or value in _OBJECT_AUDIO):
            continue
        if lang and _audio_language(stream) and _audio_language(stream) != lang:
            continue
        if _int(stream.get("Channels")) < channels or _audio_rank(stream, value) <= _audio_rank(best, best_value):
            continue
        best, best_value = stream, value
    return best_value, normalize_channels(best.get("ChannelLayout")) or normalize_channels(best.get("Channels"))


def stream_media_info(item: Mapping[str, Any]) -> dict[str, Any]:
    streams = _item_streams(item)
    video = next((s for s in streams if _text(s.get("Type")).lower() == "video"), {})
    resolution = resolution_from_dimensions(item.get("Width") or video.get("Width"), item.get("Height") or video.get("Height"), interlaced=bool(video.get("IsInterlaced")))
    audio, channels = _best_audio(streams)
    return media_block({"resolution": resolution, "hdr": stream_hdr(video), "audio": audio, "audio_channels": channels, "is_3d": True if _text(item.get("Video3DFormat")) else None})


_PHYSICAL_FORMATS = frozenset(FORMATS) - {"digital"}


def _known_media(media: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {key: value for key, normalize in _NORMALIZERS.items() if (value := normalize(media.get(key)))}
    if media.get("is_3d") is True:
        out["is_3d"] = True
    return out


def _folded_format(fmt: str, media: Mapping[str, Any]) -> str:
    return "4k_bluray" if fmt == "bluray" and media.get("resolution") == "uhd_4k" else fmt


_RESOLUTION_RANK = {value: rank for rank, value in enumerate(reversed(RESOLUTIONS))}
_HDR_RANK = {"hlg": 1, "hdr10": 2, "hdr10_plus": 3, "dolby_vision": 4}
_FORMAT_RANK = {"digital": 1, "vcd": 2, "vhs": 2, "betamax": 2, "laserdisc": 2, "dvd": 3, "hd_dvd": 4, "bluray": 5, "4k_bluray": 6}


def _channel_count(value: Any) -> int:
    try:
        return sum(int(part) for part in str(value or "").split("."))
    except ValueError:
        return 0


def combine_media(entries: Iterable[tuple[Any, Any]]) -> tuple[str, dict[str, Any]]:
    fmt = ""
    best: dict[str, Any] = {}
    audio_rank: tuple[int, int] | None = None
    for raw_fmt, raw_media in entries:
        known = _known_media(raw_media if isinstance(raw_media, Mapping) else {})
        candidate = normalize_format(raw_fmt)
        if _FORMAT_RANK.get(candidate, 0) > _FORMAT_RANK.get(fmt, 0):
            fmt = candidate
        resolution = known.get("resolution")
        if resolution and _RESOLUTION_RANK[resolution] > _RESOLUTION_RANK.get(best.get("resolution") or "", -1):
            best["resolution"] = resolution
        hdr = known.get("hdr")
        if hdr and _HDR_RANK[hdr] > _HDR_RANK.get(best.get("hdr") or "", 0):
            best["hdr"] = hdr
        audio, channels = known.get("audio"), known.get("audio_channels")
        if audio:
            rank = ((2 if audio in _LOSSLESS_AUDIO else 0) + (1 if audio in _OBJECT_AUDIO else 0), _channel_count(channels))
            if audio_rank is None or rank > audio_rank:
                audio_rank = rank
                best["audio"] = audio
                best.pop("audio_channels", None)
                if channels:
                    best["audio_channels"] = channels
        elif channels and audio_rank is None and "audio_channels" not in best:
            best["audio_channels"] = channels
        if known.get("is_3d"):
            best["is_3d"] = True
    return fmt, best


def media_update(src: Mapping[str, Any], dst: Mapping[str, Any]) -> dict[str, Any] | None:
    src_media, dst_media = item_media(src), item_media(dst)
    src_known, dst_known = _known_media(src_media), _known_media(dst_media)
    src_fmt, dst_fmt = normalize_format(src.get("format")), normalize_format(dst.get("format"))
    if src_fmt == "4k_bluray":
        src_known.setdefault("resolution", "uhd_4k")
    dst_folded = _folded_format(dst_fmt, dst_known)
    if dst_folded in _PHYSICAL_FORMATS and _folded_format(src_fmt, src_known) not in _PHYSICAL_FORMATS:
        return None
    changed = {key: value for key, value in src_known.items() if dst_known.get(key) != value}
    merged_fmt = src_fmt or dst_fmt
    format_changed = _folded_format(merged_fmt, {**dst_known, **changed}) != dst_folded
    if not changed and not format_changed:
        return None
    out = dict(dst)
    out["_cw_media_update"] = True
    out["media_info"] = {**dst_media, **changed}
    if format_changed:
        out["format"] = merged_fmt
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
