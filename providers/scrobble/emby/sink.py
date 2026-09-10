# providers/scrobble/emby/sink.py
# CrossWatch - Emby Scrobble Sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from providers.scrobble._jellyfin_emby import JellyfinEmbySink


class EmbySink(JellyfinEmbySink):
    name = "emby"
