# providers/main.py
# # 24-09-2025 Back-to-Basics Editions...and pray i guess..
import time
from providers.scrobble.plex.watch import WatchService, autostart_from_config
from providers.scrobble.scrobble import cfg, log

def main():
    svc = autostart_from_config() or WatchService()
    if not svc.is_running():
        svc.start()
    poll = float(cfg("scrobble.poll_secs", 5))
    log(f"CrossWatch watch started (poll={poll:.0f}s)")
    try:
        while True:
            time.sleep(poll)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
