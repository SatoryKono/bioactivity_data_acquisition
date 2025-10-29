from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from typing import Any

class PreparedRequest:
    method: str | None
    url: str | None
    headers: MutableMapping[str, str]
    body: bytes | str | None

    def prepare(
        self,
        method: str,
        url: str,
        files: Any | None = ...,
        data: Any | None = ...,
        headers: Mapping[str, str] | None = ...,
        params: Mapping[str, str] | Sequence[tuple[str, str]] | None = ...,
    ) -> None: ...


class Request:
    method: str
    url: str
    headers: MutableMapping[str, str]
    params: Mapping[str, str] | Sequence[tuple[str, str]] | None
    data: Any

    def prepare(self) -> PreparedRequest: ...


class Response:
    status_code: int
    headers: MutableMapping[str, str]
    text: str
    content: bytes
    url: str
    reason: str
    request: PreparedRequest

    def json(self, **kwargs: Any) -> Any: ...
    def iter_content(self, chunk_size: int = ..., decode_unicode: bool = ...) -> Iterable[bytes]: ...
    def raise_for_status(self) -> None: ...
