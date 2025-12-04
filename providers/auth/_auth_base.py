from __future__ import annotations
from _logging import log
from dataclasses import dataclass, field
from typing import Any, Mapping, MutableMapping, Optional, Protocol

# Schema for a provider
@dataclass
class AuthManifest:
    name: str
    label: str
    flow: str 
    fields: list[dict] = field(default_factory=list)
    actions: dict = field(default_factory=dict)
    verify_url: Optional[str] = None                   
    notes: Optional[str] = None                        

# Status
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
    def capabilities(self) -> dict: ...
    def get_status(self, cfg: Mapping[str, Any]) -> AuthStatus: ...
    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str) -> dict[str, Any]: ...
    def finish(self, cfg: MutableMapping[str, Any], **payload) -> AuthStatus: ...
    def refresh(self, cfg: MutableMapping[str, Any]) -> AuthStatus: ...
    def disconnect(self, cfg: MutableMapping[str, Any]) -> AuthStatus: ...