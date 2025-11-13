"""Base class for ChEMBL entity iterators."""

from __future__ import annotations

import warnings
from collections import deque
from collections.abc import Iterable, Iterator, Mapping, Sequence
from functools import lru_cache
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlencode

from structlog.stdlib import (
    BoundLogger,  # pyright: ignore[reportMissingImports, reportUnknownVariableType]
)

from bioetl.clients.chembl_config import EntityConfig
from bioetl.config.loader import _load_yaml
from bioetl.core.logging import UnifiedLogger

# ChemblClient is dynamically loaded in __init__.py, so we use Any for type checking.
# Import happens at runtime to avoid circular dependencies.

__all__ = ["ChemblEntityIteratorBase", "ChemblEntityIterator"]

_DEFAULT_STATUS_ENDPOINT = "/status.json"
_CHEMBL_DEFAULTS_PATH = Path(__file__).resolve().parents[3] / "configs" / "defaults" / "chembl.yaml"


@lru_cache(maxsize=1)
def _load_chembl_client_defaults() -> Mapping[str, Any]:
    """Load cached Chembl client defaults from the shared YAML profile."""
    try:
        payload = _load_yaml(_CHEMBL_DEFAULTS_PATH)
    except FileNotFoundError:
        return {}

    if not isinstance(payload, Mapping):
        return {}

    chembl_section = payload.get("chembl")
    if isinstance(chembl_section, Mapping):
        return dict(chembl_section)

    clients_section = payload.get("clients")
    if isinstance(clients_section, Mapping):
        legacy_chembl = clients_section.get("chembl")
        if isinstance(legacy_chembl, Mapping):
            return dict(legacy_chembl)

    return {}


def _resolve_status_endpoint() -> str:
    """Resolve the status endpoint based on defaults with a static fallback."""
    chembl_defaults = _load_chembl_client_defaults()
    candidate: Any = chembl_defaults.get("status_endpoint")
    if isinstance(candidate, str):
        normalized = candidate.strip()
        if normalized:
            return normalized
    return _DEFAULT_STATUS_ENDPOINT


class ChemblEntityIteratorBase:
    """Provide a unified iterator over ChEMBL entities with pagination and chunking."""

    def __init__(
        self,
        chembl_client: Any,
        config: EntityConfig,
        *,
        batch_size: int,
        max_url_length: int | None = None,
    ) -> None:
        """Initialise the iterator for a particular entity type.

        Parameters
        ----------
        chembl_client:
            ChemblClient instance responsible for HTTP calls.
        config:
            Entity configuration.
        batch_size:
            Batch size for pagination (capped at 25 by the ChEMBL API).
        max_url_length:
            Optional URL length constraint; disables the check when ``None``.
        """
        if batch_size <= 0:
            msg = "batch_size must be a positive integer"
            raise ValueError(msg)
        if max_url_length is not None and max_url_length <= 0:
            msg = "max_url_length must be a positive integer if provided"
            raise ValueError(msg)

        self._chembl_client = chembl_client
        # Backwards compatibility for client attribute expected by legacy code/tests.
        self._client = chembl_client
        self._config = config
        self._batch_size = min(batch_size, 25)
        self._max_url_length = max_url_length
        self._chembl_release: str | None = None
        self._log: BoundLogger = UnifiedLogger.get(__name__).bind(  # pyright: ignore[reportUnknownMemberType]
            component="chembl_iterator",
            entity=config.log_prefix,
        )

    @property
    def chembl_release(self) -> str | None:
        """Return the ChEMBL release identifier captured during handshake.

        Returns
        -------
        str | None:
            ChEMBL release version or ``None`` if handshake has not run.
        """
        return self._chembl_release

    @property
    def chembl_client(self) -> Any:
        """Return the wrapped ChemblClient instance."""

        return self._chembl_client

    @property
    def batch_size(self) -> int:
        """Return the effective batch size for pagination."""

        return self._batch_size

    @property
    def max_url_length(self) -> int | None:
        """Return the enforced URL length limit, if any."""

        return self._max_url_length

    def handshake(
        self,
        *,
        endpoint: str | None = None,
        enabled: bool = True,
    ) -> Mapping[str, object]:
        """Perform the status handshake and cache the release identifier.

        When ``endpoint`` is omitted, the value is resolved from ``chembl.status_endpoint``
        (preserving compatibility with legacy ``clients.chembl.status_endpoint``;
        fallback ``"/status.json"``). The resolved endpoint is used verbatim without
        additional normalisation.

        Parameters
        ----------
        endpoint:
            Endpoint used for the handshake request.
        enabled:
            Set to ``False`` to skip the handshake. Defaults to ``True``.

        Returns
        -------
        Mapping[str, object]:
            Response payload from the handshake or an empty mapping when skipped.
        """
        resolved_endpoint = endpoint if endpoint is not None else _resolve_status_endpoint()
        if not enabled:
            self._log.info(  # pyright: ignore[reportUnknownMemberType]
                f"{self._config.log_prefix}.handshake.skipped",
                handshake_endpoint=resolved_endpoint,
                handshake_enabled=enabled,
                phase="skip",
            )
            return {}

        payload = self._chembl_client.handshake(resolved_endpoint)
        release = payload.get("chembl_db_version")
        if isinstance(release, str):
            self._chembl_release = release

        self._log.info(  # pyright: ignore[reportUnknownMemberType]
            f"{self._config.log_prefix}.handshake",
            handshake_endpoint=resolved_endpoint,
            handshake_enabled=enabled,
            chembl_release=self._chembl_release,
        )

        return cast(Mapping[str, object], payload)

    def iterate_all(
        self,
        *,
        limit: int | None = None,
        page_size: int | None = None,
        select_fields: Sequence[str] | None = None,
    ) -> Iterator[Mapping[str, object]]:
        """Iterate over all entity records with optional limits.

        Parameters
        ----------
        limit:
            Maximum number of records to return.
        page_size:
            Page size override; defaults to the configured batch size.
        select_fields:
            Optional list of fields requested via the ``only`` parameter.

        Yields
        ------
        Mapping[str, object]:
            Entity records yielded from the API.
        """
        effective_page_size = self._coerce_page_size(page_size)
        yielded = 0

        params: dict[str, object] = {}
        if limit is not None and limit > 0:
            effective_page_size = min(effective_page_size, limit)
            params["limit"] = effective_page_size
        else:
            params["limit"] = effective_page_size

        if select_fields:
            params["only"] = ",".join(sorted(select_fields))

        for item in self._chembl_client.paginate(
            self._config.endpoint,
            params=params,
            page_size=effective_page_size,
            items_key=self._config.items_key,
        ):
            yield item
            yielded += 1
            if limit is not None and yielded >= limit:
                break

    def iterate_by_ids(
        self,
        ids: Sequence[str],
        *,
        select_fields: Sequence[str] | None = None,
    ) -> Iterator[Mapping[str, object]]:
        """Iterate over entity records for explicit identifiers with smart chunking.

        Parameters
        ----------
        ids:
            Sequence of identifiers to fetch.
        select_fields:
            Optional list of fields requested via the ``only`` parameter.

        Yields
        ------
        Mapping[str, object]:
            Entity records emitted for the requested identifiers.
        """
        for chunk in self._chunk_identifiers(ids, select_fields=select_fields):
            params: dict[str, object] = {self._config.filter_param: ",".join(chunk)}
            if select_fields:
                params["only"] = ",".join(sorted(select_fields))

            yield from self._chembl_client.paginate(
                self._config.endpoint,
                params=params,
                page_size=len(chunk),
                items_key=self._config.items_key,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _coerce_page_size(self, requested: int | None) -> int:
        """Normalize the requested page size to a valid value.

        Parameters
        ----------
        requested:
            Requested page size; falls back to ``batch_size`` when ``None``.

        Returns
        -------
        int:
            Effective page size used for requests.
        """
        if requested is None:
            return self._batch_size
        if requested <= 0:
            return self._batch_size
        return min(requested, self._batch_size)

    def _chunk_identifiers(
        self,
        ids: Sequence[object],
        *,
        select_fields: Sequence[str] | None = None,
    ) -> Iterable[Sequence[str]]:
        """Split identifiers into chunks while honouring the URL length limits.

        Parameters
        ----------
        ids:
            Sequence of identifiers (any objects) to be chunked.
        select_fields:
            Optional list of fields used to compute URL length.

        Yields
        ------
        Sequence[str]:
            Chunks of identifiers suitable for a single request.
        """
        chunk: deque[str] = deque()

        for identifier in ids:
            if identifier is None:
                continue
            if isinstance(identifier, str):
                candidate_identifier = identifier.strip()
            else:
                candidate_identifier = str(identifier).strip()
            if not candidate_identifier:
                continue

            candidate_size = len(chunk) + 1

            # Enforce URL length check when enabled.
            if self._config.enable_url_length_check and self._max_url_length is not None:
                candidate_param_length = self._encode_in_query(
                    tuple(list(chunk) + [candidate_identifier]),
                    select_fields=select_fields,
                )
                if (
                    candidate_size > self._batch_size
                    or candidate_param_length > self._max_url_length
                ):
                    if chunk:
                        yield tuple(chunk)
                        chunk.clear()
                    chunk.append(candidate_identifier)
                    continue
            elif candidate_size > self._batch_size:
                # Fallback to chunk size validation when URL checks are disabled.
                if chunk:
                    yield tuple(chunk)
                    chunk.clear()
                chunk.append(candidate_identifier)
                continue

            chunk.append(candidate_identifier)

        if chunk:
            yield tuple(chunk)

    def _encode_in_query(
        self,
        identifiers: Sequence[str],
        *,
        select_fields: Sequence[str] | None = None,
    ) -> int:
        """Compute the URL query length for the provided identifiers.

        Parameters
        ----------
        identifiers:
            Sequence of identifiers for encoding.
        select_fields:
            Optional list of fields to include in the request.

        Returns
        -------
        int:
            Approximate length of the query component in the URL.
        """
        params_dict: dict[str, str] = {self._config.filter_param: ",".join(identifiers)}
        if select_fields:
            params_dict["only"] = ",".join(sorted(select_fields))

        params = urlencode(params_dict)
        # Include the base endpoint length to approximate the final URL length.
        base_length = self._config.base_endpoint_length or len(self._config.endpoint)
        return base_length + len("?") + len(params)


class ChemblEntityIterator(ChemblEntityIteratorBase):
    """Deprecated alias for :class:`ChemblEntityIteratorBase`."""

    def __init__(
        self,
        chembl_client: Any,
        config: EntityConfig,
        *,
        batch_size: int,
        max_url_length: int | None = None,
    ) -> None:
        """Initialise the deprecated iterator alias while emitting a warning."""
        warnings.warn(
            "ChemblEntityIterator is deprecated and will be removed in a future version. "
            "Use ChemblEntityIteratorBase instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(
            chembl_client=chembl_client,
            config=config,
            batch_size=batch_size,
            max_url_length=max_url_length,
        )
