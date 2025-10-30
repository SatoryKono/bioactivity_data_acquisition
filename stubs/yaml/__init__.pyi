"""Minimal PyYAML stub definitions used for type checking."""

from collections.abc import Iterable
from typing import IO, Any, Callable

# YAML Nodes
class Node: ...
class ScalarNode(Node): ...

# Stub the nodes submodule
class nodes:
    Node = Node
    ScalarNode = ScalarNode

# Loaders
class Loader:
    def __init__(self, stream: IO[str] | IO[bytes]) -> None: ...
    def construct_scalar(self, node: ScalarNode) -> Any: ...
    @classmethod
    def add_constructor(cls, tag: str, constructor: Callable[[Loader, Node], Any]) -> None: ...
class SafeLoader(Loader): ...


def safe_load(stream: str | bytes | IO[str] | IO[bytes]) -> Any: ...


def safe_load_all(stream: str | bytes | IO[str] | IO[bytes]) -> Iterable[Any]: ...


def load(stream: str | bytes | IO[str] | IO[bytes], *, Loader: type[Loader]) -> Any: ...


def dump(
    data: Any,
    stream: IO[str] | None = ...,
    *,
    default_flow_style: bool | None = ...,
    sort_keys: bool | None = ...,
) -> str | None: ...


def safe_dump(
    data: Any,
    stream: IO[str] | None = ...,
    *,
    default_flow_style: bool | None = ...,
    sort_keys: bool | None = ...,
) -> str | None: ...
