from __future__ import annotations

"""Извлечение сущностей ChEMBL через дескриптор."""

from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from bioetl.core.pipeline.unified import ChemblExtractionDescriptor


class ChemblExtractor:
    """Общий экстрактор для сущностей ChEMBL."""

    def extract(
        self,
        pipeline: "ChemblEntityPipeline",
        *,
        summary_event: str,
    ) -> pd.DataFrame:
        ids = pipeline.config.get("ids") if isinstance(pipeline.config, Mapping) else None
        descriptor = pipeline.build_descriptor()
        frame, _stats = pipeline.run_descriptor_extraction(
            descriptor,
            ids if isinstance(ids, Sequence) else None,
            summary_event=summary_event,
            batch_size=int(pipeline.get_config_value("sources.chembl.batch_size")),
        )
        return frame

    def build_generic_descriptor(
        self, pipeline: "ChemblEntityPipeline"
    ) -> ChemblExtractionDescriptor:
        def build_context(_pipeline: "ChemblEntityPipeline") -> Mapping[str, Any]:
            chembl_ctx = (
                pipeline.config.get("sources", {}).get("chembl", {})
                if isinstance(pipeline.config, Mapping)
                else {}
            )
            return {
                "chembl_client": chembl_ctx.get("client"),
                "entity_fetcher": chembl_ctx.get(f"{pipeline.entity_name}_fetcher"),
            }

        def fetcher_factory(context: Mapping[str, Any]):
            fetcher = context.get("entity_fetcher")

            def fetch(batch: Sequence[str] | None):
                meta = {"api_calls": 0, "cache_hit": False, "fallback": 0}
                if batch is None:
                    return [], meta
                try:
                    if callable(fetcher):
                        result = fetcher(batch)
                    else:
                        result = [{"chembl_id": chembl_id} for chembl_id in batch]
                except Exception as exc:  # pragma: no cover - защитный сценарий
                    fallback_rows = self._fallback_rows(batch, exc)
                    meta["fallback"] = len(fallback_rows)
                    return fallback_rows, meta

                if isinstance(result, tuple) and len(result) == 2:
                    rows, extra = result
                    meta.update({k: v for k, v in extra.items() if k not in meta})
                    return rows, meta
                return result, meta

            return fetch

        def finalizer_factory(context: Mapping[str, Any]):
            release = context.get("chembl_release")

            def finalize(df: pd.DataFrame) -> pd.DataFrame:
                if release and "chembl_release" not in df.columns:
                    df = df.assign(chembl_release=release)
                sort_columns = (
                    list(pipeline.required_sort_fields)
                    if pipeline.required_sort_fields
                    else list(df.columns)
                )
                return df.sort_values(by=sort_columns, ignore_index=True) if not df.empty else df

            return finalize

        return ChemblExtractionDescriptor(
            build_context=build_context,
            fetcher_factory=fetcher_factory,
            finalizer_factory=finalizer_factory,
        )

    def _fallback_rows(self, ids: Iterable[str], exc: Exception) -> list[dict[str, Any]]:
        timestamp = datetime.utcnow().isoformat()
        return [
            {
                "chembl_id": chembl_id,
                "error_code": "extract_failed",
                "http_status": None,
                "error_message": str(exc),
                "retry_after_sec": None,
                "attempt": 1,
                "extracted_at": timestamp,
            }
            for chembl_id in ids
        ]


__all__ = ["ChemblExtractor"]
