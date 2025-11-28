from __future__ import annotations

"""Common framework for ChEMBL pipelines on new runtime."""

from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd
import yaml

from bioetl.clients.enrichers.facade import (
    EnricherFacade,
    NullEnricherFacade,
    build_enricher_facade,
)
from bioetl.clients.enrichers.factory import EnricherClientFactory
from bioetl.clients.enrichers.strategy_registry import StrategyRegistry
from bioetl.core.pipeline.services import (
    default_write_service_factory,
    ArtifactPlanner,
)
from bioetl.core.pipeline.types import (
    StageExecutionOptions,
    WriteArtifacts,
    WriteResult,
)
from bioetl.core.pipeline.unified import (
    ChemblExtractionServiceDescriptor,
    ChemblPipelineBase,
)
from bioetl.pipelines.chembl.common.chembl_extraction_service import (
    ChemblExtractionService,
)
from bioetl.pipelines.chembl.common.descriptor import (
    ConfigValidationError,
    ChemblExtractionDescriptor,
)
from bioetl.pipelines.chembl.common.descriptor_factory import (
    ChemblContextFacade,
    ChemblDescriptorFactory,
    FetcherStrategy,
)
from bioetl.core.io.artifacts import SchemaRegistry
from bioetl.pipelines.chembl.common.strategies import (
    ExtractionStrategyFactory,
)
from bioetl.clients.chembl import (
    ChemblEntityClientFactory,
    default_chembl_factory,
)


class ChemblWriteService:
    """Детерминированная запись для ChEMBL-пайплайнов."""

    def __init__(self, pipeline: "ChemblCommonPipeline") -> None:
        self.pipeline = pipeline

    def save(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
        *,
        context,
        runtime,
    ) -> WriteResult:
        _ = runtime
        date_suffix = datetime.utcnow().date().isoformat()
        stem = f"{self.pipeline.entity_name}_chembl"
        dataset_path = artifacts.data_path or artifacts.extra.get("dataset")
        # Determine output directory first
        if dataset_path:
            output_dir = dataset_path.parent.resolve()
        else:
            output_dir = context.output_dir.resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        dataset_path = (
            dataset_path or output_dir / f"{stem}_all_{date_suffix}.csv"
        )
        quality_report_path = (
            artifacts.quality_report_path
            or output_dir / f"{stem}_quality_report.csv"
        )
        meta_path = artifacts.meta_path or output_dir / f"{stem}_meta.yaml"
        manifest_path = (
            artifacts.manifest_path
            or output_dir / f"{stem}_run_manifest.json"
        )

        artifacts.data_path = dataset_path
        artifacts.quality_report_path = quality_report_path
        artifacts.meta_path = meta_path
        artifacts.manifest_path = manifest_path

        if not options.dry_run:
            df.to_csv(dataset_path, index=False)
            self.pipeline._write_quality_report(df, quality_report_path)

        payload = {
            "run_id": self.pipeline.run_id,
            "pipeline": self.pipeline.pipeline_name,
            "entity": self.pipeline.entity_name,
            "rows": int(df.shape[0]),
            "columns": list(df.columns),
            "generated_at": datetime.utcnow().isoformat(),
        }
        meta_path.write_text(yaml.safe_dump(payload, allow_unicode=True))
        manifest_path.write_text(
            yaml.safe_dump(
                {
                    "run_id": self.pipeline.run_id,
                    "artifacts": {
                        "dataset": dataset_path.name,
                        "quality_report": quality_report_path.name,
                        "meta": meta_path.name,
                    },
                }
            )
        )

        log_dir = Path("/data/logs") / stem
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / f"{stem}.log").touch()

        if options.extended:
            metadata_writer = getattr(self.pipeline, "_write_metadata", None)
            if callable(metadata_writer):
                metadata_writer(output_dir, df)
        return WriteResult(rows=int(df.shape[0]), artifacts=artifacts)

    def write_metadata(
        self,
        output_dir: Path,
        artifacts: WriteArtifacts,
        df: pd.DataFrame | None,
        *,
        dry_run: bool,
    ) -> None:  # noqa: D401
        """Compatibility with WriteService interface.

        Metadata is written in save method.
        """
        _ = (output_dir, artifacts, df, dry_run)
        return None


class ChemblCommonPipeline(ChemblPipelineBase):
    """Базовый класс для ChEMBL-пайплайнов без legacy-зависимостей."""

    entity_name: str = "chembl"
    required_sort_fields: Sequence[str] = ()

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        run_id: str | None = None,
        extraction_service: ChemblExtractionService | None = None,
        extraction_service_factory: (
            Callable[[], ChemblExtractionService] | None
        ) = None,
        descriptor_factory: ChemblDescriptorFactory | None = None,
        artifact_runtime_service_factory: (
            Callable[[Any], Any] | None
        ) = None,
        custom_artifact_planner_factory: (
            Callable[[], ArtifactPlanner] | None
        ) = None,
        schema_registry_factory: Callable[[], SchemaRegistry] | None = None,
        descriptor_type: str = "service",
        extraction_strategy_factory: ExtractionStrategyFactory | None = None,
    ) -> None:
        super().__init__(
            config,
            run_id=run_id,
            extraction_service=extraction_service,
            extraction_service_factory=extraction_service_factory,
            artifact_runtime_service_factory=artifact_runtime_service_factory,
            write_service_factory=default_write_service_factory,
        )
        self._validate_common_config()
        self.write_service = ChemblWriteService(self)
        # Store optional custom factories
        self._custom_artifact_planner_factory = custom_artifact_planner_factory
        self._schema_registry_factory = schema_registry_factory
        self._descriptor_type = descriptor_type
        self._extraction_strategy_factory = (
            extraction_strategy_factory or ExtractionStrategyFactory()
        )
        self._descriptor_factory = (
            descriptor_factory or self._create_descriptor_factory()
        )

    def _create_descriptor_factory(self) -> ChemblDescriptorFactory:
        chembl_ctx = (
            self.config.get("sources", {}).get("chembl", {})
            if isinstance(self.config, Mapping)
            else {}
        )
        chembl_client = chembl_ctx.get("client")
        pagination_strategy = chembl_ctx.get("pagination_strategy")
        pagination_strategy_name = chembl_ctx.get("pagination_strategy_name")
        pagination_factories = chembl_ctx.get("pagination_factories")
        transport_factory = chembl_ctx.get("transport_factory")
        chembl_release = chembl_ctx.get("chembl_release")

        client_factory: ChemblEntityClientFactory | None = chembl_ctx.get(
            "client_factory"
        )
        if client_factory is None and chembl_client is None:
            client_factory = default_chembl_factory(
                self.config,
                pagination_strategy=pagination_strategy,
                pagination_strategy_name=pagination_strategy_name,
                pagination_factories=pagination_factories,
                transport_factory=transport_factory,
            )
            transport_factory = client_factory.config.transport_factory
            pagination_strategy_name = (
                pagination_strategy_name
                or client_factory.config.pagination_strategy_name
            )
            pagination_strategy = (
                pagination_strategy or client_factory.config.pagination_strategy
            )
            pagination_factories = (
                pagination_factories
                or client_factory.config.pagination_factories
            )

        context_facade = ChemblContextFacade(
            transport_factory=transport_factory,
            pagination_strategy=pagination_strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_factories=pagination_factories,
            chembl_release=chembl_release,
            chembl_client=chembl_client,
            client_factory=client_factory,
        )

        fetcher_key = f"{self.entity_name}_fetcher"
        fetcher_strategies: dict[str, FetcherStrategy] = {}
        if fetcher_key in chembl_ctx:
            fetcher = chembl_ctx[fetcher_key]

            def strategy(_context: Mapping[str, Any], _plan: Any, _fetcher=fetcher):
                if callable(_fetcher):
                    return _fetcher
                if _fetcher is None:
                    return None

                def noop(batch: Sequence[str] | None):
                    if batch is None:
                        return []
                    return [{"chembl_id": chembl_id} for chembl_id in batch]

                return noop

            fetcher_strategies[self.entity_name] = strategy

        return ChemblDescriptorFactory(
            context_facade,
            fetcher_strategies=fetcher_strategies,
            fallback_rows=self._fallback_rows,
            sort_fields={self.entity_name: self.required_sort_fields},
        )

    def _validate_common_config(self) -> None:
        batch_size = self._get_config_value("sources.chembl.batch_size")
        if (
            not isinstance(batch_size, int)
            or batch_size <= 0
            or batch_size > 25
        ):
            raise ConfigValidationError(
                "sources.chembl.batch_size must be integer within (0,25]"
            )

        max_url_length = self._get_config_value(
            "sources.chembl.max_url_length"
        )
        if (
            not isinstance(max_url_length, int)
            or max_url_length <= 0
            or max_url_length > 2000
        ):
            raise ConfigValidationError(
                "sources.chembl.max_url_length must be integer within (0,2000]"
            )

        namespace = self._get_config_value("cache.namespace")
        if not isinstance(namespace, str) or not namespace.strip():
            raise ConfigValidationError(
                "cache.namespace must be non-empty string"
            )

        sort_by = self._get_config_value("determinism.sort.by")
        if (
            not isinstance(sort_by, list)
            or not all(isinstance(x, str) for x in sort_by)
        ):
            raise ConfigValidationError(
                "determinism.sort.by must be a list of strings"
            )
        missing = [
            field
            for field in self.required_sort_fields
            if field not in sort_by
        ]
        if missing:
            raise ConfigValidationError(
                f"determinism.sort.by is missing required fields for "
                f"{self.entity_name}: {missing}"
            )

    def _init_enrichers(
        self, config: Mapping[str, Any]
    ) -> EnricherFacade | NullEnricherFacade:
        enricher_cfg = config.get("enrichers") if isinstance(config, Mapping) else None
        factory = EnricherClientFactory.from_config(enricher_cfg)
        strategies = StrategyRegistry.from_config(enricher_cfg)
        return build_enricher_facade(factory, strategies)

    def _get_config_value(self, dotted_path: str) -> Any:
        current: Any = self.config
        for part in dotted_path.split("."):
            if not isinstance(current, Mapping) or part not in current:
                raise ConfigValidationError(
                    f"Missing configuration key: {dotted_path}"
                )
            current = current[part]
        return current

    def extract(
        self,
        descriptor: (
            ChemblExtractionServiceDescriptor
            | ChemblExtractionDescriptor
            | None
        ),
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        if options.dry_run and self.validation_service:
            return self.validation_service.empty_frame()

        strategy = self._extraction_strategy_factory.get(self._descriptor_type)
        return strategy.run(self, descriptor, options)

    def transform(
        self, df: pd.DataFrame, options: StageExecutionOptions
    ) -> pd.DataFrame:
        df = self.pre_transform(df)
        df = self.domain_enrich(df)
        return df

    def validate(
        self, df: pd.DataFrame, options: StageExecutionOptions
    ) -> pd.DataFrame:
        if self.validation_service:
            return self.validation_service.validate(
                df, pipeline=self, options=options
            )
        return df

    def save_results(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
    ) -> WriteResult:
        """Save results using default implementation."""
        return super().save_results(df, artifacts, options)

    def _write_quality_report(
        self, df: pd.DataFrame, output_path: Path
    ) -> None:
        summary = {
            "rows": int(df.shape[0]),
            "columns": len(df.columns),
            "missing_values": (
                int(df.isna().sum().sum()) if not df.empty else 0
            ),
        }
        pd.DataFrame([summary]).to_csv(output_path, index=False)

    def _fallback_rows(
        self, ids: Iterable[str], exc: Exception
    ) -> list[dict[str, Any]]:
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

    def _build_generic_descriptor(
        self,
    ) -> ChemblExtractionServiceDescriptor:
        def build_context(
            _pipeline: ChemblCommonPipeline
        ) -> Mapping[str, Any]:
            chembl_ctx = (
                self.config.get("sources", {}).get("chembl", {})
                if isinstance(self.config, Mapping)
                else {}
            )
            return {
                "chembl_client": chembl_ctx.get("client"),
                "entity_fetcher": chembl_ctx.get(
                    f"{self.entity_name}_fetcher"
                ),
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
                        result = [
                            {"chembl_id": chembl_id} for chembl_id in batch
                        ]
                except Exception as exc:
                    fallback_rows = self._fallback_rows(batch, exc)
                    meta["fallback"] = len(fallback_rows)
                    return fallback_rows, meta

                if isinstance(result, tuple) and len(result) == 2:
                    rows, extra = result
                    meta.update(
                        {k: v for k, v in extra.items() if k not in meta}
                    )
                    return rows, meta
                return result, meta

            return fetch

        def finalizer_factory(context: Mapping[str, Any]):
            release = context.get("chembl_release")

            def finalize(df: pd.DataFrame) -> pd.DataFrame:
                if release and "chembl_release" not in df.columns:
                    df = df.assign(chembl_release=release)
                sort_columns = (
                    list(self.required_sort_fields)
                    if self.required_sort_fields
                    else list(df.columns)
                )
                return (
                    df.sort_values(by=sort_columns, ignore_index=True)
                    if not df.empty
                    else df
                )

            return finalize

        return ChemblExtractionServiceDescriptor(
            build_context=build_context,
            fetcher_factory=fetcher_factory,
            finalizer_factory=finalizer_factory,
        )

    def build_descriptor(
        self,
    ) -> ChemblExtractionServiceDescriptor | ChemblExtractionDescriptor:
        """Build extraction descriptor using the configured factory."""

        if self._descriptor_type == "service":
            return self._descriptor_factory.build(self.entity_name)
        # pragma: no cover - dataclass pipelines override this path
        raise NotImplementedError

    def _extract_with_dataclass_descriptor(
        self,
        descriptor: ChemblExtractionDescriptor,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        """Extract data using dataclass descriptor pattern.

        For Activity pipeline.
        """
        # This method should be overridden by pipelines using dataclass
        # descriptors. Default implementation returns empty DataFrame
        if options.dry_run:
            return pd.DataFrame()

        # Default fallback - can be overridden by specific pipelines
        return pd.DataFrame()


__all__ = [
    "ChemblCommonPipeline",
    "ChemblWriteService",
    "ConfigValidationError",
]
