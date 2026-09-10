# providers/scrobble/jellyfin/sink.py
# CrossWatch - Jellyfin Scrobble Sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from providers.scrobble._jellyfin_emby import JellyfinEmbySink


class JellyfinSink(JellyfinEmbySink):
    name = "jellyfin"
