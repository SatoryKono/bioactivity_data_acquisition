"""Общий каркас Chembl пайплайнов."""

from __future__ import annotations

from contextlib import suppress
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import pandas as pd
from bioetl.chembl.common.descriptor import (
    ChemblContextSpec,
    ChemblDescriptorSpec,
)
from bioetl.clients.chembl_entity_factory import (
    ChemblClientBundle,
    ChemblEntityClientFactory,
)
from bioetl.config.models.models import PipelineConfig
from bioetl.config.models.source import SourceConfig
from bioetl.pipelines.chembl.mixins import (
    EnrichmentMixin,
    NormalizationMixin,
    ValidationMixin,
)
from bioetl.pipelines.unified_base import UnifiedPipelineBase

from .helpers import build_dataframe
from .io import ChemblIO
from bioetl.clients.client_chembl import _resolve_status_endpoint


class BaseChemblPipeline(
    UnifiedPipelineBase, NormalizationMixin, EnrichmentMixin, ValidationMixin
):
    """Базовый класс, инкапсулирующий повторяемый цикл fetch→normalize→enrich→validate→write."""

    entity_name: str = ""
    id_column: str | None = None

    def __init__(
        self,
        config: PipelineConfig,
        run_id: str,
        source: Iterable[dict[str, Any]] | None = None,
        *,
        io: ChemblIO | None = None,
        writer=None,
    ) -> None:
        super().__init__(config, run_id)
        self.io = io or ChemblIO()
        self._source = source
        self.writer = writer
        self.results: list[pd.DataFrame] = []

    # --- Hooks ---
    def _fetch_source(self) -> Iterable[dict[str, Any]]:
        if self._source is None:
            raise NotImplementedError(
                "_fetch_source must be implemented when source is not provided"
            )
        return self._source

    def _normalize(
        self, chunk: Sequence[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        rules = self.get_normalization_rules()
        return self.normalize_records(chunk, **rules)

    def _enrich(
        self, records: Sequence[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        return self.enrich_records(records, self.get_enrichment_rules())

    def _validate(self, df: pd.DataFrame) -> None:
        self.validate_dataframe(df, self.get_schema())

    def _write(self, df: pd.DataFrame) -> Any:
        writer = getattr(self, "writer", None)
        if writer is None:
            # По умолчанию просто накапливаем результаты в памяти.
            self.results.append(df)
            return None
        return self.io.write_dataframe(df, writer)

    # --- Abstract rule providers ---
    def get_normalization_rules(self) -> Mapping[str, Any]:
        raise NotImplementedError

    def get_enrichment_rules(self) -> Iterable[Any]:
        return []

    def get_schema(self) -> Mapping[str, Any] | None:
        return None

    # --- Configuration helpers ---
    def _resolve_source_config(self, name: str) -> Any:
        if self.config is None:
            return None
        return getattr(self.config.domain, "sources", {}).get(name)

    def _resolve_batch_size(
        self, name: str | Any, fallback: int = 1000
    ) -> int:
        """Resolve batch size from source name or SourceConfig object.

        Parameters
        ----------
        name
            Either a source name (str) or a SourceConfig object.
        fallback
            Default batch size if not found.

        Returns
        -------
        int
            The resolved batch size.
        """

        # Имя источника: разрешаем через config.domain.sources
        config_obj: Any | None
        if isinstance(name, str):
            config_obj = getattr(self.config.domain, "sources", {}).get(name)
        else:
            # Уже готовый SourceConfig или совместимый объект
            config_obj = name

        if config_obj is None:
            return fallback

        batch_size = getattr(config_obj, "batch_size", None)
        if batch_size is None and hasattr(config_obj, "parameters"):
            batch_size = getattr(config_obj.parameters, "batch_size", None)
        return int(batch_size) if batch_size else fallback

    # --- Descriptor helpers ---
    def descriptor_spec(self) -> ChemblDescriptorSpec[Any]:
        """Provide a default descriptor spec for descriptor-driven extraction.

        This keeps individual Chembl pipelines lightweight: they only need to set
        ``entity_name`` and ``id_column`` while sharing a common descriptor
        contract used by tests and the unified extraction runner.
        """

        if not self.id_column:
            msg = f"{type(self).__name__} must define id_column to build descriptor"
            raise RuntimeError(msg)

        entity = (
            getattr(self, "entity_name", None) or ""
        ).strip() or self.pipeline_code
        descriptor_name = f"chembl_{entity}"

        def _source_config_factory(source_cfg: SourceConfig[Any]) -> Any:
            # In most call sites a typed SourceConfig is already provided, so we
            # can simply pass it through unmodified.
            return source_cfg

        context_spec = ChemblContextSpec[Any](
            entity_name=entity,
        )

        return ChemblDescriptorSpec[Any](
            name=descriptor_name,
            source_name="chembl",
            source_config_factory=_source_config_factory,
            context=context_spec,
            id_column=self.id_column,
            summary_event=f"{descriptor_name}.summary",
        )

    # --- Client helpers ---
    def build_chembl_entity_bundle(
        self,
        entity_name: str,
        source_name: str,
        source_config: Any,
        options: Any,
        chembl_client_kwargs: Any,
        fresh_http_client: bool,
    ) -> ChemblClientBundle:
        if self.config is None:
            msg = "Pipeline config is required to build Chembl clients"
            raise RuntimeError(msg)
        factory = ChemblEntityClientFactory(self.config)
        source_config = self._resolve_source_config("chembl")
        return factory.build(
            self.entity_name or "",
            source_name="chembl",
            source_config=source_config,
        )

    def fetch_chembl_release(
        self, client: Any, log: Any | None = None
    ) -> str | None:
        """Fetch ChEMBL release from a bundle or API client.

        Accept either a ChemblClientBundle (with .chembl_client) or a
        client exposing `handshake()` or `get()` to obtain the status payload.
        This keeps backward compatibility with older code that passed bundles
        while supporting newer code that passes API clients.
        """
        try:
            # Check if client has required methods before unwrapping
            # Use getattr to avoid auto-creating attributes in Mock objects
            original_handshake = getattr(client, "handshake", None)
            original_get = getattr(client, "get", None)
            original_has_handshake = (
                original_handshake is not None and callable(original_handshake)
            )
            original_has_get = original_get is not None and callable(
                original_get
            )

            # Unwrap bundle if necessary (only if original client doesn't have methods)
            if not (original_has_handshake or original_has_get):
                unwrapped = None
                if (
                    hasattr(client, "__dict__")
                    and "chembl_client" in client.__dict__
                ):
                    unwrapped = client.__dict__["chembl_client"]
                if unwrapped is not None:
                    unwrapped_handshake = getattr(unwrapped, "handshake", None)
                    unwrapped_get = getattr(unwrapped, "get", None)
                    unwrapped_has_handshake = (
                        unwrapped_handshake is not None
                        and callable(unwrapped_handshake)
                    )
                    unwrapped_has_get = unwrapped_get is not None and callable(
                        unwrapped_get
                    )
                    if unwrapped_has_handshake or unwrapped_has_get:
                        client = unwrapped

            # Final check: ensure client has at least one of the required methods
            final_handshake = getattr(client, "handshake", None)
            final_get = getattr(client, "get", None)
            has_handshake = final_handshake is not None and callable(
                final_handshake
            )
            has_get = final_get is not None and callable(final_get)
            if not (has_handshake or has_get):
                return None

            # Prefer handshake if available
            handshake = getattr(client, "handshake", None)
            if callable(handshake):
                status = handshake()
                if not status:
                    return None
                version = status.get("chembl_db_version")
                if version is None:
                    version = status.get("chembl_release")
                if version is None:
                    return None
                release_raw = str(version)
                release_value = release_raw.strip()
                if not release_value:
                    return None
                if hasattr(self, "_set_chembl_release"):
                    self._set_chembl_release(release_value)
                api_value: str | None = None
                api_version = status.get("api_version")
                if api_version is not None:
                    api_value = str(api_version).strip()
                    if api_value:
                        api_setter = getattr(self, "_set_api_version", None)
                        if callable(api_setter):
                            api_setter(api_value)
                        else:
                            self.__dict__["api_version"] = api_value
                    else:
                        api_value = None
                metadata_update: dict[str, Any] = {
                    "chembl_db_version": release_value
                }
                if api_value is not None:
                    metadata_update["api_version"] = api_value
                if hasattr(self, "update_chembl_release_metadata"):
                    self.update_chembl_release_metadata(**metadata_update)
                return release_raw

            # Fall back to HTTP status endpoint
            get = getattr(client, "get", None)
            if callable(get):
                endpoint = _resolve_status_endpoint()
                response = get(endpoint)
                json_candidate = getattr(response, "json", None)
                if callable(json_candidate):
                    payload = json_candidate()
                    if isinstance(payload, dict):
                        candidate = payload.get(
                            "chembl_db_version"
                        ) or payload.get("chembl_release")
                        if candidate is not None:
                            release_raw = str(candidate)
                            release_value = release_raw.strip()
                            if not release_value:
                                return None
                            if hasattr(self, "_set_chembl_release"):
                                self._set_chembl_release(release_value)
                            api_value = None
                            api_version = payload.get("api_version")
                            if api_version is not None:
                                candidate_api = str(api_version).strip()
                                if candidate_api:
                                    api_value = candidate_api
                                    api_setter = getattr(
                                        self, "_set_api_version", None
                                    )
                                    if callable(api_setter):
                                        api_setter(api_value)
                                    else:
                                        self.__dict__["api_version"] = (
                                            api_value
                                        )
                            metadata_update = {
                                "chembl_db_version": release_value
                            }
                            if api_value is not None:
                                metadata_update["api_version"] = api_value
                            if hasattr(self, "update_chembl_release_metadata"):
                                self.update_chembl_release_metadata(
                                    **metadata_update
                                )
                            return release_raw
            return None
        except Exception as exc:
            if log is not None:
                with suppress(Exception):
                    log.warning("fetch_chembl_release_failed", error=str(exc))
            return None

    def _get_entity_client(self, bundle: ChemblClientBundle) -> Any:
        """Get entity client from bundle, with special handling for document entity."""
        if self.entity_name == "document" and hasattr(
            self, "_build_document_client"
        ):
            try:
                return self._build_document_client(bundle)
            except Exception:
                return bundle.entity_client
        return bundle.entity_client

    # --- Extraction ---
    def extract_by_ids(
        self, ids: Sequence[str], *, select_fields: Sequence[str] | None = None
    ) -> pd.DataFrame:
        """Extract records by IDs."""
        normalized_ids = [str(i).strip() for i in ids if str(i).strip()]
        unique_ids = list(dict.fromkeys(normalized_ids))
        if not unique_ids:
            return pd.DataFrame()
        if self.config and getattr(self.config.cli, "dry_run", False):
            return pd.DataFrame()
        limit = (
            getattr(self.config.cli, "limit", None) if self.config else None
        )
        if limit is not None:
            unique_ids = unique_ids[: int(limit)]
        bundle = self.build_chembl_entity_bundle(
            entity_name=self.entity_name or "default_entity",
            source_name="chembl",
            source_config=self._resolve_source_config("chembl"),
            options={},
            chembl_client_kwargs={},
            fresh_http_client=False,
        )
        self.fetch_chembl_release(bundle)
        entity_client = self._get_entity_client(bundle)
        if entity_client is None or not hasattr(
            entity_client, "iterate_by_ids"
        ):
            msg = "Entity client does not support iterate_by_ids"
            raise RuntimeError(msg)
        batch_size = self._resolve_batch_size("chembl", len(unique_ids))
        results: list[Mapping[str, Any]] = []
        for start in range(0, len(unique_ids), batch_size):
            batch = unique_ids[start : start + batch_size]
            fetched = entity_client.iterate_by_ids(
                batch, select_fields=select_fields
            )
            results.extend(list(fetched))
        return build_dataframe(results)

    # --- PipelineBase abstract methods implementation ---
    def extract_all(self) -> pd.DataFrame:
        """Extract all records from the source."""
        # If _source is provided, use legacy extraction path
        if self._source is not None:
            combined: list[pd.DataFrame] = []
            for chunk in self.io.chunked_fetch(self._fetch_source()):
                normalized = self._normalize(chunk)
                enriched = self._enrich(normalized)
                df = build_dataframe(enriched)
                combined.append(df)
            if combined:
                return pd.concat(combined, ignore_index=True)
            return pd.DataFrame()
        # Otherwise, delegate to ChemblPipelineBase.extract_all which uses descriptors
        return super().extract_all()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw DataFrame by applying normalization and enrichment."""
        if df.empty:
            return df
        # Convert DataFrame to records for processing
        records = df.to_dict("records")
        normalized = self._normalize(records)
        enriched = self._enrich(normalized)
        return build_dataframe(enriched)

    def _schema_column_specs(self) -> Mapping[str, Mapping[str, Any]]:
        """Override to add ChEMBL-specific column specs."""
        base_specs = super()._schema_column_specs()
        base_specs["source"] = {
            "default": "ChEMBL",
            "dtype": pd.StringDtype(),
        }
        return base_specs
