from __future__ import annotations

import json
import os
import sys
import time
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ContextManager,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    cast,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd
else:  # pragma: no cover - runtime import
    try:
        import pandas as pd
    except ImportError:
        pd = cast(Any, None)


TRecord = TypeVar("TRecord")
JSONDict = Dict[str, Any]
Row = Dict[str, Any]


def _utc_iso_now() -> str:
    """Return current UTC time in ISO-8601 format with Z suffix."""

    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _duration_ms(start: float, end: Optional[float] = None) -> float:
    """Compute duration in milliseconds for performance tracking."""

    finish = end if end is not None else time.perf_counter()
    return (finish - start) * 1000.0


def _as_records(obj: Any) -> List[Row]:
    """Normalize arbitrary tabular object into list of mapping rows."""

    if pd is not None and isinstance(obj, pd.DataFrame):
        return [cast(Row, dict(row)) for row in obj.to_dict("records")]
    if isinstance(obj, list):
        if all(isinstance(item, Mapping) for item in obj):
            return [cast(Row, dict(item)) for item in obj]
    if isinstance(obj, Iterable):
        records: List[Row] = []
        for item in obj:
            if isinstance(item, Mapping):
                records.append(cast(Row, dict(item)))
            elif isinstance(item, Sequence) and not isinstance(item, (str, bytes)):
                records.append({f"col_{index}": value for index, value in enumerate(item)})
            else:
                records.append({"value": item})
        return records
    raise TypeError("Unsupported tabular object for conversion into records.")


def _to_dataframe(records: Sequence[Row]) -> Any:
    """Convert records into pandas DataFrame when available."""

    if pd is not None:
        return pd.DataFrame(records)
    return records


class SimpleSeries(MutableMapping[str, Any]):
    """Fallback Series implementation when pandas is unavailable."""

    def __init__(self, initial: Optional[Mapping[str, Any]] = None) -> None:
        self._data: Dict[str, Any] = dict(initial or {})

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def __delitem__(self, key: str) -> None:
        del self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __getattr__(self, key: str) -> Any:
        try:
            return self._data[key]
        except KeyError as exc:  # pragma: no cover - attribute access path
            raise AttributeError(key) from exc

    def __repr__(self) -> str:  # pragma: no cover - human readable
        return f"SimpleSeries({self._data!r})"


def _to_series(record: Mapping[str, Any]) -> Any:
    """Convert mapping into pandas Series or fallback implementation."""

    if pd is not None:
        return pd.Series(dict(record))
    return SimpleSeries(record)


class _StageLogContext(AbstractContextManager[None]):
    """Context manager handling stage lifecycle logging."""

    def __init__(self, logger: "UnifiedLoggerMixin", phase: str, fields: Mapping[str, Any]) -> None:
        self._logger = logger
        self._phase = phase
        self._fields = dict(fields)
        self._start: Optional[float] = None

    def __enter__(self) -> None:
        self._start = time.perf_counter()
        self._logger.info(self._phase, event="started", **self._fields)
        return None

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if self._start is None:
            return None
        duration = _duration_ms(self._start)
        if exc_type is None:
            self._logger.info(self._phase, event="completed", duration_ms=duration, **self._fields)
        else:
            self._logger.error(self._phase, event="failed", duration_ms=duration, **self._fields)
        return None


@dataclass(frozen=True)
class RetryConfig:
    """Configuration capturing retry/backoff parameters for pagination."""

    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    backoff_factor: float = 2.0
    max_delay_seconds: float = 30.0
    jitter_seconds: float = 0.1


@dataclass(frozen=True)
class DeterminismConfig:
    """Determinism contract used before persisting a dataset."""

    sort_by: Tuple[str, ...] = ("row_id",)
    column_order: Optional[Tuple[str, ...]] = None
    float_format: Optional[str] = None


@dataclass(frozen=True)
class PipelineConfig:
    """Runtime configuration used during pipeline execution."""

    pipeline_name: str
    dataset_name: str
    run_id: str
    determinism: DeterminismConfig = field(default_factory=DeterminismConfig)
    retries: RetryConfig = field(default_factory=RetryConfig)
    handshake_enabled: bool = True
    handshake_ttl_seconds: int = 3600


@dataclass(frozen=True)
class RunResult:
    """Summary of pipeline execution outcome."""

    dataset_path: Path
    metadata_path: Path
    record_count: int
    stage_durations_ms: Mapping[str, float]
    handshake: Mapping[str, Any]


@dataclass(frozen=True)
class ExtractedPage(Generic[TRecord]):
    """Represents a single extracted page of records."""

    index: int
    items: Sequence[TRecord]
    meta: Mapping[str, Any] = field(default_factory=dict)


class UnifiedLoggerMixin:
    """Structured JSON logger compatible with the pipeline contract."""

    _base_log_fields: Dict[str, Any]

    def __init__(self, *, pipeline: str, dataset: str, run_id: str) -> None:
        self._base_log_fields = {
            "pipeline": pipeline,
            "dataset": dataset,
            "run_id": run_id,
        }

    def _emit_log(self, level: str, phase: str, *, extra: Optional[Mapping[str, Any]] = None) -> None:
        payload = {
            **self._base_log_fields,
            "level": level.upper(),
            "phase": phase,
            "timestamp": _utc_iso_now(),
        }
        if extra:
            payload.update(extra)
        message = json.dumps(payload, sort_keys=True, ensure_ascii=True)
        sys.stdout.write(f"{message}\n")
        sys.stdout.flush()

    def info(self, phase: str, **fields: Any) -> None:
        """Log informational message for the given phase."""

        self._emit_log("INFO", phase, extra=fields)

    def warn(self, phase: str, **fields: Any) -> None:
        """Log warning message for the given phase."""

        self._emit_log("WARN", phase, extra=fields)

    def error(self, phase: str, **fields: Any) -> None:
        """Log error message for the given phase."""

        self._emit_log("ERROR", phase, extra=fields)

    def log_stage(self, phase: str, **fields: Any) -> ContextManager[None]:
        """Context manager logging start and completion of a stage."""

        return _StageLogContext(self, phase, fields)


class _LoggerProtocol(Protocol):
    """Internal protocol describing logging surface used by mixins."""

    def info(self, phase: str, **fields: Any) -> None:
        ...

    def warn(self, phase: str, **fields: Any) -> None:
        ...

    def error(self, phase: str, **fields: Any) -> None:
        ...

    def log_stage(self, phase: str, **fields: Any) -> ContextManager[None]:
        ...


class ReleaseHandshakeMixin(_LoggerProtocol):
    """Mixin providing release handshake with TTL-based caching."""

    _handshake_cache: Dict[str, Tuple[float, Mapping[str, Any]]]

    def __init__(self) -> None:
        self._handshake_cache = {}

    def perform_handshake(self, endpoint: str, *, enabled: bool = True, ttl_seconds: int = 3600) -> Mapping[str, Any]:
        """Execute or reuse handshake metadata depending on TTL."""

        cache_key = endpoint
        now = time.time()
        cached = self._handshake_cache.get(cache_key)
        if not enabled:
            self.info("handshake", event="skipped", endpoint=endpoint)
            return {}
        if cached and (now - cached[0]) < ttl_seconds:
            self.info("handshake", event="cache_hit", endpoint=endpoint)
            return cached[1]
        start = time.perf_counter()
        self.info("handshake", event="started", endpoint=endpoint)
        payload = self._handshake_impl(endpoint)
        duration = _duration_ms(start)
        self._handshake_cache[cache_key] = (now, payload)
        self.info("handshake", event="completed", duration_ms=duration, endpoint=endpoint)
        return payload

    def _handshake_impl(self, endpoint: str) -> Mapping[str, Any]:
        """Perform handshake; meant to be overridden by subclasses."""

        return {"endpoint": endpoint, "checked_at": _utc_iso_now()}


class PaginatedExtractorMixin(_LoggerProtocol, Generic[TRecord]):
    """Mixin orchestrating pagination with retry/backoff strategy."""

    def iterate_pages(
        self,
        fetch_page: Callable[[int], Optional[ExtractedPage[TRecord]]],
        *,
        retries: RetryConfig,
    ) -> Iterator[ExtractedPage[TRecord]]:
        """Yield pages by invoking a fetcher with retry semantics."""

        index = 0
        should_continue = True
        while should_continue:
            index += 1
            attempt = 0
            page: Optional[ExtractedPage[TRecord]] = None
            start = time.perf_counter()
            while attempt < retries.max_attempts:
                attempt += 1
                start = time.perf_counter()
                try:
                    page = fetch_page(index)
                    break
                except Exception as exc:
                    delay = min(
                        retries.max_delay_seconds,
                        retries.base_delay_seconds * (retries.backoff_factor ** (attempt - 1)),
                    )
                    delay += retries.jitter_seconds
                    self.warn(
                        "extract",
                        event="retry",
                        page=index,
                        attempt=attempt,
                        delay_seconds=delay,
                        error=str(exc),
                    )
                    time.sleep(delay)
            if page is None:
                self.info("extract", event="completed", page=index, records=0)
                should_continue = False
                continue
            with self.log_stage("extract", page=index):
                duration = _duration_ms(start)
                item_count = len(page.items)
                self.info("extract", event="page", page=index, records=item_count, duration_ms=duration)
                yield page
                should_continue = item_count > 0


class SchemaValidationMixin(_LoggerProtocol):
    """Mixin encapsulating validation logic; default is pass-through."""

    def __init__(self) -> None:
        self._validation_summary: Dict[str, Any] = {}

    def validate_dataframe(
        self,
        df: Any,
        *,
        dataset_name: str,
        fail_open: bool = False,
    ) -> Any:
        """Validate dataframe according to downstream contract."""

        start = time.perf_counter()
        self.info("validate", event="started", dataset=dataset_name)
        try:
            validated = self._validate_dataframe_impl(df, dataset_name=dataset_name, fail_open=fail_open)
        except Exception as exc:
            self.error("validate", event="failed", dataset=dataset_name, error=str(exc), duration_ms=_duration_ms(start))
            if fail_open:
                self.warn("validate", event="fail_open_returning_original", dataset=dataset_name)
                return df
            raise
        duration = _duration_ms(start)
        row_count = len(validated) if hasattr(validated, "__len__") else None
        self.info(
            "validate",
            event="completed",
            dataset=dataset_name,
            records=row_count,
            duration_ms=duration,
        )
        self._validation_summary = {
            "dataset": dataset_name,
            "records": row_count,
            "duration_ms": duration,
            "validated_at": _utc_iso_now(),
        }
        return validated

    def _validate_dataframe_impl(self, df: Any, *, dataset_name: str, fail_open: bool) -> Any:
        """Actual validation hook to be overridden; defaults to no-op."""

        return df


class DeterministicWriterMixin:
    """Mixin providing deterministic, atomic persistence helpers."""

    def write_deterministic(
        self,
        records: Sequence[Row],
        *,
        output_dir: Path,
        dataset_name: str,
        determinism: DeterminismConfig,
        stage_durations_ms: Mapping[str, float],
    ) -> RunResult:
        """Persist records using deterministic ordering and atomic write."""

        output_dir.mkdir(parents=True, exist_ok=True)
        dataset_path = output_dir / f"{dataset_name}.csv"
        metadata_path = output_dir / f"{dataset_name}.meta.yaml"

        canonical_records = self._prepare_records(records, determinism)
        record_count = len(canonical_records)
        self._write_csv_atomic(dataset_path, canonical_records, determinism)
        self._write_meta_atomic(
            metadata_path,
            dataset_path=dataset_path,
            dataset_name=dataset_name,
            record_count=record_count,
            stage_durations_ms=stage_durations_ms,
        )
        return RunResult(
            dataset_path=dataset_path,
            metadata_path=metadata_path,
            record_count=record_count,
            stage_durations_ms=stage_durations_ms,
            handshake={},
        )

    def _prepare_records(
        self,
        records: Sequence[Row],
        determinism: DeterminismConfig,
    ) -> List[Row]:
        """Sort records and align columns according to determinism config."""

        if determinism.column_order:
            ordered: List[Row] = []
            for record in records:
                ordered.append({column: record.get(column) for column in determinism.column_order})
        else:
            ordered = [dict(record) for record in records]

        sort_columns = determinism.sort_by or ()
        if sort_columns:
            ordered.sort(key=lambda item: tuple(str(item.get(column, "")) for column in sort_columns))
        return ordered

    def _write_csv_atomic(
        self,
        target_path: Path,
        records: Sequence[Row],
        determinism: DeterminismConfig,
    ) -> None:
        """Write CSV file atomically by leveraging temporary file."""

        import csv
        from tempfile import NamedTemporaryFile

        columns: List[str]
        if determinism.column_order:
            columns = list(determinism.column_order)
        elif records:
            columns = sorted({column for record in records for column in record.keys()})
        else:
            columns = ["row_id"]

        temp_suffix = target_path.suffix or ".csv"
        with NamedTemporaryFile("w", delete=False, suffix=temp_suffix, encoding="utf-8", newline="") as tmp_file:
            writer = csv.DictWriter(tmp_file, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for row in records:
                serialized_row = {column: row.get(column) for column in columns}
                writer.writerow(serialized_row)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
            temp_path = Path(tmp_file.name)

        os.replace(temp_path, target_path)

    def _write_meta_atomic(
        self,
        target_path: Path,
        *,
        dataset_path: Path,
        dataset_name: str,
        record_count: int,
        stage_durations_ms: Mapping[str, float],
    ) -> None:
        """Persist deterministic meta.yaml sidecar file."""

        from tempfile import NamedTemporaryFile

        payload = {
            "dataset_name": dataset_name,
            "dataset_path": str(dataset_path),
            "record_count": record_count,
            "generated_at_utc": _utc_iso_now(),
            "stage_durations_ms": dict(sorted(stage_durations_ms.items())),
        }

        yaml_lines = [f"{key}: {json.dumps(value, sort_keys=True)}" for key, value in sorted(payload.items())]
        yaml_content = "\n".join(yaml_lines) + "\n"

        temp_suffix = target_path.suffix or ".yaml"
        with NamedTemporaryFile("w", delete=False, suffix=temp_suffix, encoding="utf-8") as tmp_file:
            tmp_file.write(yaml_content)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
            temp_path = Path(tmp_file.name)
        os.replace(temp_path, target_path)


class BasePipeline(
    UnifiedLoggerMixin,
    ReleaseHandshakeMixin,
    PaginatedExtractorMixin[TRecord],
    SchemaValidationMixin,
    DeterministicWriterMixin,
    ABC,
    Generic[TRecord],
):
    """Base pipeline implementing orchestrated ETL lifecycle."""

    def __init__(self, config: PipelineConfig) -> None:
        UnifiedLoggerMixin.__init__(self, pipeline=config.pipeline_name, dataset=config.dataset_name, run_id=config.run_id)
        ReleaseHandshakeMixin.__init__(self)
        SchemaValidationMixin.__init__(self)
        self._config = config
        self._stage_durations_ms: Dict[str, float] = {}
        self._handshake_meta: Mapping[str, Any] = {}

    @abstractmethod
    def build_query(self, config: PipelineConfig) -> Mapping[str, Any]:
        """Construct query parameters for data extraction."""

    @abstractmethod
    def extract_pages(self, client: Any, query: Mapping[str, Any]) -> Iterable[Any]:
        """Yield raw pages according to the pipeline's extract contract."""

    @abstractmethod
    def normalize(self, raw: Iterable[Any]) -> "pd.DataFrame":
        """Normalize raw records into a tabular representation."""

    @abstractmethod
    def map_schema(self, record: "pd.Series") -> "pd.Series":
        """Align a single record to the target schema."""

    @abstractmethod
    def row_id(self, record: "pd.Series") -> str:
        """Produce deterministic row identifier for the given record."""

    def validate(self, df: "pd.DataFrame") -> "pd.DataFrame":
        """Validate normalized dataframe using schema mixin."""

        validated_df = self.validate_dataframe(df, dataset_name=self._config.dataset_name)
        return cast("pd.DataFrame", validated_df)

    def write(
        self,
        df: "pd.DataFrame",
        output_path: Path,
        *,
        extended: bool = False,
        include_correlation: bool | None = None,
        include_qc_metrics: bool | None = None,
    ) -> RunResult:
        """Serialize dataframe deterministically into target artifacts."""

        del extended, include_correlation, include_qc_metrics
        records = _as_records(df)
        result = self.write_deterministic(
            records,
            output_dir=output_path,
            dataset_name=self._config.dataset_name,
            determinism=self._config.determinism,
            stage_durations_ms=self._stage_durations_ms,
        )
        return result

    def run(
        self,
        client: Any,
        config: PipelineConfig,
        output_path: Path,
        *,
        extended: bool = False,
        include_correlation: bool | None = None,
        include_qc_metrics: bool | None = None,
    ) -> RunResult:
        """Execute full pipeline lifecycle with deterministic output."""

        self._config = config
        self._base_log_fields.update(
            {"pipeline": config.pipeline_name, "dataset": config.dataset_name, "run_id": config.run_id}
        )
        self._stage_durations_ms = {}
        self.info("run", event="started")
        run_start = time.perf_counter()
        try:
            self.on_start(config)
            handshake_endpoint = self.handshake_endpoint(config)
            if handshake_endpoint:
                stage_start = time.perf_counter()
                self._handshake_meta = self.perform_handshake(
                    handshake_endpoint,
                    enabled=config.handshake_enabled,
                    ttl_seconds=config.handshake_ttl_seconds,
                )
                self._stage_durations_ms["handshake"] = _duration_ms(stage_start)
            stage_start = time.perf_counter()
            query = self.build_query(config)
            self._stage_durations_ms["build_query"] = _duration_ms(stage_start)
            raw_records = self._collect_raw_records(client, query, config)
            normalized = self._run_normalize_stage(raw_records)
            mapped = self._run_map_schema_stage(normalized)
            validated = self._run_validate_stage(mapped)
            stage_start = time.perf_counter()
            result = self.write(
                validated,
                output_path,
                extended=extended,
                include_correlation=include_correlation,
                include_qc_metrics=include_qc_metrics,
            )
            self._stage_durations_ms["write"] = _duration_ms(stage_start)
            self.on_finish(
                {
                    "record_count": result.record_count,
                    "handshake": self._handshake_meta,
                    "stage_durations_ms": dict(self._stage_durations_ms),
                }
            )
            total_duration = _duration_ms(run_start)
            self.info("run", event="completed", duration_ms=total_duration, records=result.record_count)
            return RunResult(
                dataset_path=result.dataset_path,
                metadata_path=result.metadata_path,
                record_count=result.record_count,
                stage_durations_ms=dict(self._stage_durations_ms),
                handshake=self._handshake_meta,
            )
        except Exception as exc:
            self.on_error(exc, {"stage_durations_ms": dict(self._stage_durations_ms)})
            self.error("run", event="failed", error=str(exc), duration_ms=_duration_ms(run_start))
            raise

    def handshake_endpoint(self, config: PipelineConfig) -> Optional[str]:
        """Optional endpoint used for release handshake."""

        del config
        return None

    def on_start(self, config: PipelineConfig) -> None:
        """Hook executed before pipeline stages begin."""

        self.info("run", event="on_start", config=asdict(config))

    def on_page(self, index: int, page_meta: Mapping[str, Any]) -> None:
        """Hook executed when a new page is processed."""

        self.info("extract", event="on_page", page=index, meta=page_meta)

    def on_error(self, exc: BaseException, ctx: Mapping[str, Any]) -> None:
        """Hook invoked when execution fails."""

        self.error("run", event="on_error", error=str(exc), context=ctx)

    def on_finish(self, stats: Mapping[str, Any]) -> None:
        """Hook executed after successful completion."""

        self.info("run", event="on_finish", stats=dict(stats))

    def _collect_raw_records(
        self,
        client: Any,
        query: Mapping[str, Any],
        config: PipelineConfig,
    ) -> List[Any]:
        """Collect raw records while logging pagination details."""

        stage_start = time.perf_counter()
        pages = self.extract_pages(client, query)
        records: List[Any] = []
        for index, page in enumerate(pages, start=1):
            page_records, meta = self._normalize_page_payload(page)
            self.on_page(index, meta)
            records.extend(page_records)
        duration = _duration_ms(stage_start)
        self._stage_durations_ms["extract"] = duration
        self.info(
            "extract",
            event="completed",
            records=len(records),
            duration_ms=duration,
            query=dict(query),
            config=asdict(config),
        )
        return records

    def _run_normalize_stage(self, raw_records: Iterable[Any]) -> Any:
        """Run normalization stage with logging."""

        stage_start = time.perf_counter()
        with self.log_stage("normalize"):
            normalized = self.normalize(raw_records)
        duration = _duration_ms(stage_start)
        self._stage_durations_ms["normalize"] = duration
        row_count = len(normalized) if hasattr(normalized, "__len__") else None
        self.info("normalize", event="completed", records=row_count, duration_ms=duration)
        return normalized

    def _run_map_schema_stage(self, data: Any) -> Any:
        """Apply schema mapping and compute row identifiers."""

        stage_start = time.perf_counter()
        if pd is not None and isinstance(data, pd.DataFrame):
            mapped = data.apply(self.map_schema, axis=1)
            if isinstance(mapped, pd.DataFrame):
                df = mapped
            else:
                df = pd.DataFrame(mapped.tolist())
            df["row_id"] = df.apply(self.row_id, axis=1).astype(str)
            result = df
        else:
            records = _as_records(data)
            mapped_records: List[Row] = []
            for record in records:
                series_in = _to_series(record)
                mapped_series = self.map_schema(series_in)
                mapped_mapping: Row
                if pd is not None and isinstance(mapped_series, pd.Series):
                    mapped_mapping = cast(Row, dict(mapped_series.to_dict()))
                elif isinstance(mapped_series, Mapping):
                    mapped_mapping = cast(Row, dict(mapped_series))
                else:
                    mapped_mapping = cast(Row, dict(SimpleSeries(cast(Mapping[str, Any], mapped_series))))
                row_id_series = _to_series(mapped_mapping)
                mapped_mapping["row_id"] = str(self.row_id(row_id_series))
                mapped_records.append(mapped_mapping)
            result = _to_dataframe(mapped_records)
        duration = _duration_ms(stage_start)
        self._stage_durations_ms["map_schema"] = duration
        self.info("map_schema", event="completed", duration_ms=duration)
        return result

    def _run_validate_stage(self, data: Any) -> Any:
        """Validate dataset and register duration."""

        stage_start = time.perf_counter()
        validated = self.validate(data)
        duration = _duration_ms(stage_start)
        self._stage_durations_ms["validate"] = duration
        return validated

    def _normalize_page_payload(self, page: Any) -> Tuple[List[Any], Mapping[str, Any]]:
        """Support various page payload structures."""

        if isinstance(page, ExtractedPage):
            return list(page.items), page.meta
        if isinstance(page, Mapping) and "items" in page:
            items = page.get("items", [])
            meta = {key: value for key, value in page.items() if key != "items"}
            return list(items), meta
        if isinstance(page, Sequence) and not isinstance(page, (str, bytes)):
            return list(page), {}
        return [page], {}

