# cw_platform/config_base.py
from pathlib import Path
import os

def CONFIG_BASE() -> Path:
    """
    Resolve the base configuration directory.

    Precedence:
      1. Explicit CONFIG_BASE environment variable
      2. /config if running in container (/app exists)
      3. Project root (two levels up from this file)
    """
    env = os.getenv("CONFIG_BASE")
    if env:
        return Path(env)

    if Path("/app").exists():
        return Path("/config")

    # default: repo root
    return Path(__file__).resolve().parents[1]

# Ready-to-use Path (most modules can import this directly)
CONFIG = CONFIG_BASE()
