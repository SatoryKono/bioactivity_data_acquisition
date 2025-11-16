"""Minimal Typer testing stubs used by tests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

class Result(Protocol):
    exit_code: int
    stdout: str
    stderr: str


class CliRunner:
    def __init__(self, *, mix_stderr: bool = ...) -> None: ...

    def invoke(
        self,
        app: Any,
        args: Sequence[str] | None = ...,
        *,
        input: str | bytes | None = ...,
        env: Mapping[str, str] | None = ...,
        color: bool | None = ...,
        catch_exceptions: bool = ...,
    ) -> Result: ...


__all__ = ["CliRunner"]

