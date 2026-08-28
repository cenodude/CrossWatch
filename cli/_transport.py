# /cli/_transport.py
# CrossWatch - CLI HTTP transport, including log streaming
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any, Iterator

from ._errors import ApiError, CLIError, TransportUnavailable

JSON_CT = "application/json"


class Transport:
    name = "transport"

    def request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: Any = None) -> Any:
        raise NotImplementedError

    def get(self, path: str, **kwargs: Any) -> Any:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> Any:
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> Any:
        return self.request("PUT", path, **kwargs)

    def patch(self, path: str, **kwargs: Any) -> Any:
        return self.request("PATCH", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> Any:
        return self.request("DELETE", path, **kwargs)

    def stream_sse(self, path: str, *, params: dict[str, Any] | None = None) -> Iterator[tuple[str, str]]:
        raise NotImplementedError

    def close(self) -> None:
        return None


class HttpTransport(Transport):
    name = "http"

    def __init__(self, base_url: str, *, token: str = "", timeout: float = 30.0, insecure: bool = False) -> None:
        try:
            import requests
        except ImportError as exc:
            raise CLIError("The 'requests' package is required for the CrossWatch CLI") from exc
        self._requests = requests
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = float(timeout)
        self.insecure = bool(insecure)
        self.session = requests.Session()
        self.session.headers.update({"Accept": JSON_CT, "User-Agent": "crosswatch-cli"})
        if token:
            self.session.headers["X-CW-Token"] = token
        if insecure:
            try:
                import urllib3

                urllib3.disable_warnings()
            except Exception:
                pass

    def _url(self, path: str) -> str:
        return self.base_url + ("/" + path.lstrip("/"))

    def _unreachable(self, exc: Exception) -> TransportUnavailable:
        return TransportUnavailable(
            f"Cannot reach CrossWatch at {self.base_url}",
            hint=f"Is the service running? ({type(exc).__name__})",
        )

    def request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: Any = None) -> Any:
        try:
            resp = self.session.request(
                method.upper(),
                self._url(path),
                params=params or None,
                json=json_body,
                timeout=self.timeout,
                verify=not self.insecure,
                allow_redirects=False,
            )
        except self._requests.exceptions.SSLError as exc:
            raise TransportUnavailable(
                f"TLS handshake failed for {self.base_url}",
                hint="CrossWatch is probably using a self-signed certificate. Retry with --insecure.",
            ) from exc
        except self._requests.exceptions.Timeout as exc:
            raise TransportUnavailable(f"Timed out after {self.timeout:g}s talking to {self.base_url}") from exc
        except self._requests.exceptions.RequestException as exc:
            raise self._unreachable(exc) from exc

        if resp.status_code in (301, 302, 303, 307, 308):
            location = str(resp.headers.get("location") or "")
            if "/login" in location:
                raise ApiError(401, {"error": "Authentication required"}, method=method.upper(), path=path)

        payload: Any
        text = resp.text or ""
        ctype = str(resp.headers.get("content-type") or "")
        if JSON_CT in ctype:
            try:
                payload = resp.json()
            except Exception:
                payload = text
        else:
            payload = text

        if resp.status_code >= 400:
            raise ApiError(resp.status_code, payload, method=method.upper(), path=path)
        return payload

    def stream_sse(self, path: str, *, params: dict[str, Any] | None = None) -> Iterator[tuple[str, str]]:
        try:
            resp = self.session.get(
                self._url(path),
                params=params or None,
                stream=True,
                timeout=(self.timeout, None),
                verify=not self.insecure,
                headers={"Accept": "text/event-stream"},
            )
        except self._requests.exceptions.RequestException as exc:
            raise self._unreachable(exc) from exc

        if resp.status_code >= 400:
            body: Any
            try:
                body = resp.json()
            except Exception:
                body = resp.text
            raise ApiError(resp.status_code, body, method="GET", path=path)

        event = "message"
        data: list[str] = []
        try:
            for raw in resp.iter_lines(decode_unicode=True):
                if raw is None:
                    continue
                line = str(raw)
                if line == "":
                    if data:
                        yield event, "\n".join(data)
                    event = "message"
                    data = []
                    continue
                if line.startswith(":"):
                    continue
                if line.startswith("event:"):
                    event = line[6:].strip() or "message"
                elif line.startswith("data:"):
                    data.append(line[5:].lstrip())
        finally:
            try:
                resp.close()
            except Exception:
                pass

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass


def probe(transport: HttpTransport) -> bool:
    try:
        transport.request("GET", "/api/health")
        return True
    except TransportUnavailable:
        return False
    except ApiError:
        return True


def dumps(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=False, default=str)
