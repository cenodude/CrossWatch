# /providers/main.py
from providers.scrobble.trakt.sink import TraktSink
from providers.scrobble.plex.watch import make_default_watch
import time

if __name__ == "__main__":
    watch = make_default_watch(sinks=[TraktSink()])
    watch.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        watch.stop()
