"""Simple TTL cache backed by shelve."""
from __future__ import annotations

import os
import shelve
import time
from pathlib import Path
from typing import Any

import requests


class TTLCache:
    def __init__(self, path: str | os.PathLike[str], ttl: int) -> None:
        self.path = Path(path)
        self.ttl = float(ttl)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _open(self):
        return shelve.open(str(self.path))

    def _now(self) -> float:
        return time.time()

    def make_key(self, method: str, url: str, params: Any, headers: dict[str, str]) -> str:
        params_repr = repr(sorted(params.items())) if params else ""
        headers_repr = repr(sorted(headers.items())) if headers else ""
        return f"{method.upper()}:{url}:{params_repr}:{headers_repr}"

    def get(self, key: str) -> requests.Response | None:
        with self._open() as db:
            if key not in db:
                return None
            expiry, payload = db[key]
            if expiry < self._now():
                del db[key]
                return None
        response = requests.Response()
        response.status_code = payload["status"]
        response._content = payload["content"]
        response.headers = payload["headers"]
        response.url = payload["url"]
        response.reason = payload.get("reason")
        return response

    def set(self, key: str, response: requests.Response) -> None:
        expiry = self._now() + self.ttl
        payload = {
            "status": response.status_code,
            "content": response.content,
            "headers": dict(response.headers),
            "url": response.url,
            "reason": response.reason,
        }
        with self._open() as db:
            db[key] = (expiry, payload)
            db.sync()
