# providers/main.py - refactored: 21-09-2025
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
