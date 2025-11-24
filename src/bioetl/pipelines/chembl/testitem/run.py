"""Упрощённый TestItem-пайплайн."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Mapping as TypingMapping

import pandas as pd

from bioetl.pipelines.chembl.common import BaseChemblPipeline
from . import transform as testitem_transform


class ChemblTestItemPipeline(BaseChemblPipeline):
    entity_name = "testitem"
    id_column = "molecule_chembl_id"
    actor = "testitem_chembl"

    def __init__(
        self,
        config: Any,
        run_id: str,
        source: Iterable[dict[str, Any]] | None = None,
        *,
        writer=None,
    ) -> None:
        super().__init__(config, run_id, source, writer=writer)

    def get_normalization_rules(self) -> Mapping[str, Any]:
        return {
            "field_mappings": {"test_item_id": "test_item_id", "name": "name"}
        }

    def get_schema(self):
        return {"test_item_id": lambda series: series.notna()}

    def save_results(self, df: pd.DataFrame, output_dir, **_: Any):
        return super().save_results(df, output_dir)

    def _fetch_chembl_release(self, client, log=None):
        release = super()._fetch_chembl_release(client, log)
        # Normalize release value
        if release:
            normalized_release = (
                release.strip()
                if isinstance(release, str)
                else str(release).strip()
            )
            if hasattr(self, "_set_chembl_release"):
                self._set_chembl_release(normalized_release)
        # Extract and set api_version if available
        # Use getattr to avoid auto-creating attributes in Mock objects
        handshake_method = getattr(client, "handshake", None)
        if handshake_method is not None and callable(handshake_method):
            try:
                status = handshake_method()
                if isinstance(status, dict):
                    api_version = status.get("api_version")
                    if api_version is not None and hasattr(
                        self, "_set_api_version"
                    ):
                        self._set_api_version(str(api_version))
            except Exception:
                pass  # Ignore errors when extracting api_version
        return release

    def ensure_chembl_release(self, context, log):  # type: ignore[override]
        """Ensure release is resolved and propagate api_version into extra_filters.

        This adapter keeps the behaviour of the shared Chembl pipeline base while
        also mirroring the expectations of the test suite, which relies on
        ``context.extra_filters['api_version']`` being populated whenever an API
        version is available.
        """

        # Delegate core logic to the unified Chembl descriptor implementation.
        release, metadata = super().ensure_chembl_release(context, log)

        api_version = metadata.get("api_version") or getattr(
            self, "api_version", None
        )
        if api_version:
            # Ensure the filter is visible to downstream pagination logic.
            context.extra_filters["api_version"] = api_version

        return release, metadata

    def _normalize_identifiers(
        self, df: pd.DataFrame, log: Any
    ) -> pd.DataFrame:  # noqa: D401
        """Normalize ChEMBL identifiers and InChI keys for the testitem entity."""

        result = df.copy()

        if "molecule_chembl_id" in result.columns:
            series = (
                result["molecule_chembl_id"]
                .astype("string")
                .str.strip()
                .str.upper()
            )
            series = series.mask(series == "", pd.NA)
            result["molecule_chembl_id"] = series

        if "molecule_structures__standard_inchi_key" in result.columns:
            series = (
                result["molecule_structures__standard_inchi_key"]
                .astype("string")
                .str.strip()
                .str.upper()
            )
            series = series.mask(series == "", pd.NA)
            result["molecule_structures__standard_inchi_key"] = series

        return result

    def _normalize_string_fields(
        self, df: pd.DataFrame, log: Any
    ) -> pd.DataFrame:  # noqa: D401
        """Normalize human-readable string fields for the testitem entity."""

        result = df.copy()

        for column in ("pref_name", "molecule_structures__canonical_smiles"):
            if column in result.columns:
                series = result[column].astype("string").str.strip()
                series = series.mask(series == "", pd.NA)
                result[column] = series

        return result

    def _deduplicate_molecules(
        self, df: pd.DataFrame, log: Any
    ) -> pd.DataFrame:  # noqa: D401
        """Deduplicate molecules by structural identifiers.

        Rows are grouped by a stable subset of structural keys. The first
        occurrence is retained to preserve input order, which keeps IO
        deterministic for identical inputs.
        """

        if df.empty:
            return df

        subset: list[str] = []
        for col in (
            "molecule_structures__standard_inchi_key",
            "molecule_structures__canonical_smiles",
        ):
            if col in df.columns:
                subset.append(col)

        if not subset:
            return df

        result = df.copy()
        result = result.drop_duplicates(
            subset=subset, keep="first"
        ).reset_index(drop=True)
        return result

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[override]
        """Apply testitem-specific transform and annotate with release metadata."""

        if df.empty:
            return df

        # Delegate structural flattening and array serialization to the shared
        # testitem transform utility.
        flattened = testitem_transform.transform(df, self.config)

        result = flattened.copy()
        metadata = self.chembl_release_metadata()

        chembl_db_version = metadata.get("chembl_db_version")
        api_version = metadata.get("api_version")

        # The schema requires non-null strings for version fields. When the
        # release handshake has not been executed (e.g. in unit tests that call
        # ``transform`` directly), we fall back to empty strings to satisfy the
        # contract and match the committed golden artefacts.
        result["_chembl_db_version"] = (
            str(chembl_db_version) if chembl_db_version is not None else ""
        )
        result["_api_version"] = (
            str(api_version) if api_version is not None else ""
        )

        return result

    def augment_metadata(
        self,
        metadata: TypingMapping[str, object],
        df: pd.DataFrame,
    ) -> TypingMapping[str, object]:  # type: ignore[override]
        """Include ChEMBL release metadata in the persisted meta.yaml payload."""

        enriched: dict[str, object] = dict(
            super().augment_metadata(metadata, df)
        )

        release_meta = self.chembl_release_metadata()
        chembl_db_version = release_meta.get("chembl_db_version")
        api_version = release_meta.get("api_version")

        if chembl_db_version is not None:
            enriched["chembl_db_version"] = chembl_db_version
        if api_version is not None:
            enriched["api_version"] = api_version

        return enriched


# Backward-compatible alias expected by tests and stage wrappers
TestItemChemblPipeline = ChemblTestItemPipeline


__all__ = ["ChemblTestItemPipeline", "TestItemChemblPipeline"]
