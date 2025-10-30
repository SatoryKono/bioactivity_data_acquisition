"""Shared helpers for adapter normalizers."""

from functools import lru_cache
from typing import cast

from bioetl.normalizers.identifier import IdentifierNormalizer
from bioetl.normalizers.registry import registry
from bioetl.normalizers.string import StringNormalizer


@lru_cache(maxsize=1)
def get_bibliography_normalizers() -> tuple[IdentifierNormalizer, StringNormalizer]:
    """Return identifier and string normalizers used by bibliography adapters.

    The normalizers are resolved through :mod:`bioetl.normalizers.registry` once
    and cached for subsequent calls. Missing registry entries raise the same
    :class:`ValueError` as :func:`registry.get`.
    """

    identifier = cast(IdentifierNormalizer, registry.get("identifier"))
    string = cast(StringNormalizer, registry.get("string"))
    return identifier, string
