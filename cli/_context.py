# /cli/_context.py
# CrossWatch - CLI invocation state, service first with local fallback
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator

from ._errors import CLIError, LocalUnsupported, TransportUnavailable
from ._local import LocalTransport
from ._render import Output
from ._settings import resolve_token, resolve_url
from ._transport import HttpTransport, Transport


@dataclass
class Ctx:
    url: str = ""
    token: str = ""
    timeout: float = 30.0
    insecure: bool = False
    force_local: bool = False
    out: Output = field(default_factory=Output)
    options: dict[str, Any] = field(default_factory=dict, repr=False)
    _http: HttpTransport | None = field(default=None, repr=False)
    _local: LocalTransport | None = field(default=None, repr=False)
    _fell_back: bool = field(default=False, repr=False)

    @classmethod
    def build(
        cls,
        *,
        url: str = "",
        token: str = "",
        timeout: float = 30.0,
        insecure: bool = False,
        local: bool = False,
        output: str = "auto",
        color: bool = True,
        quiet: bool = False,
    ) -> "Ctx":
        return cls(
            url=resolve_url(url),
            token=resolve_token(token),
            timeout=timeout,
            insecure=insecure,
            force_local=bool(local),
            out=Output(output, color=color, quiet=quiet),
            options={
                "url": url,
                "token": token,
                "timeout": timeout,
                "insecure": insecure,
                "local": local,
                "output": output,
                "color": color,
                "quiet": quiet,
            },
        )

    @property
    def http(self) -> HttpTransport:
        if self._http is None:
            self._http = HttpTransport(self.url, token=self.token, timeout=self.timeout, insecure=self.insecure)
        return self._http

    @property
    def local(self) -> LocalTransport:
        if self._local is None:
            self._local = LocalTransport()
        return self._local

    @property
    def transport(self) -> Transport:
        return self.local if self.force_local else self.http

    @property
    def mode(self) -> str:
        if self.force_local:
            return "local"
        return "local (fallback)" if self._fell_back else "service"

    def _note_fallback(self, reason: str) -> None:
        if self._fell_back:
            return
        self._fell_back = True
        self.out.warn(f"{reason} - answering from the local install instead (read-only view may be stale).")

    def request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: Any = None) -> Any:
        if self.force_local:
            return self.local.request(method, path, params=params, json_body=json_body)
        try:
            return self.http.request(method, path, params=params, json_body=json_body)
        except TransportUnavailable as unreachable:
            try:
                result = self.local.request(method, path, params=params, json_body=json_body)
            except LocalUnsupported:
                raise unreachable from None
            except CLIError:
                raise unreachable from None
            self._note_fallback(str(unreachable.message))
            return result

    def get(self, path: str, **kwargs: Any) -> Any:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> Any:
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> Any:
        return self.request("PUT", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> Any:
        return self.request("DELETE", path, **kwargs)

    def stream_sse(self, path: str, *, params: dict[str, Any] | None = None) -> Iterator[tuple[str, str]]:
        if self.force_local:
            raise LocalUnsupported("Log streaming")
        return self.http.stream_sse(path, params=params)

    def stream_client(self) -> HttpTransport:
        return HttpTransport(self.url, token=self.token, timeout=self.timeout, insecure=self.insecure)

    def require_service(self, what: str) -> None:
        if self.force_local:
            raise LocalUnsupported(what)

    def close(self) -> None:
        if self._http is not None:
            self._http.close()
