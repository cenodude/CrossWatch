from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Mapping, MutableMapping, Optional, Protocol

# UI schema for a provider
@dataclass
class AuthManifest:
    name: str                 # "PLEX"
    label: str                # "Plex"
    flow: str                 # "device_pin" | "oauth" | "api_keys" | "token"
    fields: list[dict] = field(default_factory=list)   # editable fields for Settings (e.g., client_id)
    actions: dict = field(default_factory=dict)        # which buttons exist: {"start":True,"finish":True,"refresh":True,"disconnect":True}
    verify_url: Optional[str] = None                   # for device_pin flows
    notes: Optional[str] = None                        # short hint for UI

# Status for the UI
@dataclass
class AuthStatus:
    connected: bool
    label: str
    user: Optional[str] = None
    expires_at: Optional[int] = None
    scopes: Optional[list[str]] = None
    extra: dict[str, Any] = field(default_factory=dict)

# Provider interface
class AuthProvider(Protocol):
    name: str

    def manifest(self) -> AuthManifest: ...

    def get_status(self, cfg: Mapping[str, Any]) -> AuthStatus: ...
    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str) -> dict[str, Any]: ...
    def finish(self, cfg: MutableMapping[str, Any], **payload) -> AuthStatus: ...
    def refresh(self, cfg: MutableMapping[str, Any]) -> AuthStatus: ...
    def disconnect(self, cfg: MutableMapping[str, Any]) -> AuthStatus: ...
