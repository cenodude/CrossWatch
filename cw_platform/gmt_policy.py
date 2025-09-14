# --------------- Global tombstone policy: scopes, opposing ops, and suppression rules ---------------
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal

ScopeList = Literal["watchlist", "ratings", "history", "playlists"]
ScopeDim  = Literal["add", "remove", "rate", "unrate", "scrobble", "unscrobble"]

@dataclass(frozen=True)
class Scope:
    list: ScopeList
    dim:  ScopeDim

OPPOSITE = {
    "add": "remove",
    "remove": "add",
    "rate": "unrate",
    "unrate": "rate",
    "scrobble": "unscrobble",
    "unscrobble": "scrobble",
}

NEGATIVE_OPS = {"remove", "unrate", "unscrobble"}

def opposing(op: str) -> str:
    return OPPOSITE.get(op, op)

def is_negative(op: str) -> bool:
    return op in NEGATIVE_OPS

def should_suppress_write(op: str, tombstone_dim: str) -> bool:
    """
    True when a pending tombstone with tombstone_dim should block a write with op.
    E.g. unrate blocks rate, remove blocks add, unscrobble blocks scrobble.
    """
    return tombstone_dim == opposing(op)
