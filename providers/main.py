# providers/main.py
# CrossWatch - Plex scrobbler to multiple services
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import time

from providers.scrobble.plex.watch import make_default_watch
from providers.scrobble.trakt.sink import TraktSink

def main() -> None:
    watcher = make_default_watch([TraktSink()])
    try:
        watcher.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        watcher.stop()

if __name__ == "__main__":
    main()
