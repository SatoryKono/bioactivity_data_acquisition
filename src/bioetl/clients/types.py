"""Protocol definitions for entity-level clients."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any, Protocol, TypeVar

__all__ = ["EntityClient"]

T_co = TypeVar("T_co", covariant=True)


class EntityClient(Protocol[T_co]):
    """Structural contract for lightweight data-access clients.

    Implementations are expected to expose a minimal trio of methods that
    pipelines rely on: :meth:`fetch`, :meth:`iter` and :meth:`close`.

    The protocol intentionally uses ``*args``/``**kwargs`` to stay agnostic to
    concrete filtering parameters.  Only the return types are relevant for
    static type-checking.  ``Protocol`` performs **static** duck typing –
    attributes are not validated at runtime even when combined with
    :func:`typing.runtime_checkable`.
    """

    def fetch(self, *args: Any, **kwargs: Any) -> T_co | Iterable[T_co]:
        """Load one or several records eagerly."""

    def iter(self, *args: Any, **kwargs: Any) -> Iterator[T_co]:
        """Stream records lazily."""

    def close(self) -> None:
        """Release any underlying resources (if applicable)."""
