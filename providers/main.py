# providers/main.py
# CrossWatch - Plex scrobbler to multiple services
# Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from providers.scrobble.trakt.sink import TraktSink
from providers.scrobble.plex.watch import make_default_watch
from time import sleep

def main():
    w = make_default_watch([TraktSink()])
    try:
        w.start()
        while True:
            sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        w.stop()

if __name__ == "__main__":
    main()
