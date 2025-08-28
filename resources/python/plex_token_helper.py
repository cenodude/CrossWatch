#!/usr/bin/env python3
"""
Plex Token Helper (raw PIN flow; no plexapi)

- Single command: --fetch
- Auto-detect container vs. host
  * Container → saves to /config/config.json
  * Host      → saves to ./config.json
- Prints PIN + link; user completes linking on ANY device at https://plex.tv/link
"""

import argparse
import builtins
import json
import os
import sys
import time
import uuid
import calendar
from pathlib import Path
from typing import Optional

import requests

# --- ANSI colors -------------------------------------------------------------
ANSI_DIM    = "\033[90m"
ANSI_BLUE   = "\033[94m"
ANSI_YELLOW = "\033[33m"
ANSI_G      = "\033[92m"
ANSI_R      = "\033[91m"
ANSI_X      = "\033[0m"

# --- Version / UA ------------------------------------------------------------
__VERSION__ = "0.3.7"
UA = f"Plex-Token-Helper/{__VERSION__}"

def cprint(*args, **kwargs):
    builtins.print(*args, flush=True, **kwargs)

# --- Banner ------------------------------------------------------------------
def print_banner() -> None:
    cprint(f"{ANSI_G}Plex Token Helper{ANSI_X}  {ANSI_DIM}v{__VERSION__}{ANSI_X}")
    cprint(f"{ANSI_DIM}Fetch a Plex auth token via official PIN login and save it to config.json.{ANSI_X}")
    cprint("")

# --- Env detection -----------------------------------------------------------
def running_in_container() -> bool:
    if os.path.exists("/.dockerenv"):
        return True
    try:
        with open("/proc/1/cgroup", "rt") as f:
            data = f.read()
            if any(s in data for s in ("docker", "kubepods", "containerd", "lxc")):
                return True
    except Exception:
        pass
    if "KUBERNETES_SERVICE_HOST" in os.environ:
        return True
    return False

def target_config_path() -> Path:
    if running_in_container():
        base = Path("/config")
        path = base / "config.json"
        try:
            base.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        return path
    return Path("./config.json").resolve()

# --- Config I/O --------------------------------------------------------------
def load_config(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"{ANSI_R}[!]{ANSI_X} Could not parse {path} as JSON: {e}")

def save_config(path: Path, cfg: dict) -> None:
    try:
        path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
        cprint(f"{ANSI_G}[✓]{ANSI_X} Token saved to {path}")
    except Exception as e:
        raise SystemExit(f"{ANSI_R}[!]{ANSI_X} Failed to write {path}: {e}")

# --- Plex PIN API ------------------------------------------------------------
PLEX_PIN_CREATE = "https://plex.tv/pins.json"
PLEX_PIN_STATUS = "https://plex.tv/pins/{id}.json"

def _plex_headers(client_id: str) -> dict:
    return {
        "Accept": "application/json",
        "User-Agent": UA,
        "X-Plex-Product": "Plex-Token-Helper",
        "X-Plex-Version": __VERSION__,
        "X-Plex-Client-Identifier": client_id,
        "X-Plex-Device": "Python",
        "X-Plex-Device-Name": "plex-token-helper",
        "X-Plex-Platform": "Python",
    }

def _unwrap_pin(js: dict) -> dict:
    # Support both {'pin': {...}} and legacy flat shapes.
    if isinstance(js, dict) and "pin" in js and isinstance(js["pin"], dict):
        return js["pin"]
    return js or {}

def _iso_to_epoch_utc(iso_str: str) -> int:
    # e.g. '2025-08-27T23:00:30Z' -> epoch seconds, using time/calendar only
    try:
        ts = time.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ")
        return calendar.timegm(ts)
    except Exception:
        return 0

def _create_pin(session: requests.Session, client_id: str):
    headers = _plex_headers(client_id)
    r = session.post(PLEX_PIN_CREATE, headers=headers, data={"strong": "true"}, timeout=15)
    if not r.ok:
        raise SystemExit(f"{ANSI_R}[!]{ANSI_X} PIN create failed: HTTP {r.status_code} {r.text}")
    pin_js = _unwrap_pin(r.json())
    pin_id = pin_js.get("id")
    code = pin_js.get("code")
    exp_iso = pin_js.get("expires_at")
    exp_epoch = _iso_to_epoch_utc(exp_iso) if isinstance(exp_iso, str) else 0
    if not pin_id or not code or not exp_epoch:
        raise SystemExit(f"{ANSI_R}[!]{ANSI_X} Incomplete PIN response: {r.json()}")
    return pin_id, code, exp_epoch, headers

def _poll_for_token(session: requests.Session, pin_id: int, headers: dict,
                    exp_epoch: int, poll_interval: float = 1.0) -> Optional[str]:
    while True:
        remaining = exp_epoch - int(time.time())
        if remaining <= 0:
            # Clear the countdown line
            sys.stdout.write("\r" + " " * 60 + "\r")
            sys.stdout.flush()
            return None

        # live countdown overwrite (single line)
        sys.stdout.write(f"\r{ANSI_BLUE}[i]{ANSI_X} PIN expires in ~{remaining:4d}s")
        sys.stdout.flush()

        rr = session.get(PLEX_PIN_STATUS.format(id=pin_id), headers=headers, timeout=15)
        if rr.ok:
            st_pin = _unwrap_pin(rr.json())
            token = st_pin.get("auth_token")
            if token:
                sys.stdout.write("\r" + " " * 60 + "\r")
                sys.stdout.flush()
                cprint(f"{ANSI_G}[✓]{ANSI_X} Plex token retrieved.")
                return token

        time.sleep(poll_interval)

def fetch_token_with_auto_refresh(max_attempts: int = 3) -> str:
    """
    Creates a PIN, shows link + countdown, polls until token or expiry.
    On expiry, automatically creates a new PIN (up to max_attempts).
    Returns the token string, or raises SystemExit if all attempts expire.
    """
    attempts = max(1, int(max_attempts))  # clamp to at least 1 to satisfy type checkers
    cid = "plex-simkl-bridge-" + uuid.uuid4().hex[:8]
    with requests.Session() as session:
        for attempt in range(1, attempts + 1):
            pin_id, code, exp_epoch, headers = _create_pin(session, cid)
            link_url = f"https://plex.tv/link?code={code}"
            cprint(f"{ANSI_BLUE}[i]{ANSI_X} Your Plex link code: {ANSI_G}{code}{ANSI_X}")
            cprint(f"    Open on ANY device/browser: {link_url}")
            ttl = max(0, exp_epoch - int(time.time()))
            cprint(f"    This code expires in ~{ttl}s (attempt {attempt}/{attempts}).")

            token = _poll_for_token(session, pin_id, headers, exp_epoch)
            if token is not None:
                return token

            if attempt < attempts:
                cprint(f"{ANSI_YELLOW}[i]{ANSI_X} PIN expired; requesting a new code...")

    # If we reach here, all attempts expired without a token
    raise SystemExit(f"{ANSI_R}[!]{ANSI_X} PIN expired too many times. Aborting.")

# --- CLI ---------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="plex_token_helper.py",
        add_help=True,
        description="Fetch a Plex auth token via PIN login and save it to config.json",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    ap.add_argument("--fetch", action="store_true",
                    help="Run PIN login and save token to /config/config.json (container) or ./config.json (host)")
    ap.add_argument("--attempts", type=int, default=3, help=argparse.SUPPRESS)
    return ap

def print_usage_short() -> None:
    print_banner()
    cprint(f"{ANSI_BLUE}Usage:{ANSI_X}")
    cprint("  python helper.py --fetch")
    cprint("")
    cprint(f"{ANSI_BLUE}What it does:{ANSI_X}")
    cprint("  • Detects if running in a container.")
    cprint("  • Saves token to /config/config.json (container) or ./config.json (host).")
    cprint("  • Creates or updates the 'plex.account_token' field.")
    cprint("  • Auto-refreshes the PIN up to 3 attempts with visible countdown.")
    cprint("")

def main() -> None:
    ap = build_parser()

    if len(sys.argv) == 1:
        print_usage_short()
        return

    args = ap.parse_args()
    if not args.fetch:
        print_usage_short()
        return

    print_banner()

    token = fetch_token_with_auto_refresh(max_attempts=args.attempts)

    dest = target_config_path()
    cfg = load_config(dest) if dest.exists() else {}
    plex_cfg = cfg.get("plex") or {}
    plex_cfg["account_token"] = token
    cfg["plex"] = plex_cfg
    save_config(dest, cfg)

    cprint(f"{ANSI_G}Done.{ANSI_X}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.stdout.write("\r" + " " * 60 + "\r")
        sys.stdout.flush()
        cprint("\n[!] Aborted")
