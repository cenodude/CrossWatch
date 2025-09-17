"""Entry point for running the default scrobble watcher.

Starts a Plex watch service and forwards events to the Trakt sink. Intended
for quick local runs and demos; production setups typically invoke modules via
the orchestrator.
"""

# /providers/main.py
from providers.scrobble.trakt.sink import TraktSink
from providers.scrobble.plex.watch import make_default_watch
import time

if __name__ == "__main__":
    watch = make_default_watch(sinks=[TraktSink()])
    watch.start()
    try:
        # Keep the process alive until interrupted
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # Graceful shutdown on Ctrl+C
        watch.stop()
