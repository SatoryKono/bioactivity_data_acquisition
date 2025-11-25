"""Configuration objects for Chembl batch pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal, Optional

LogFunction = Callable[[str, dict[str, Any]], None]


@dataclass(slots=True)
class PipelineConfig:
    """Common configuration for Chembl batch pipelines.

    Attributes
    ----------
    table_name:
        Name of the Chembl table to query.
    id_field:
        Primary identifier column of the table.
    batch_size:
        How many identifiers to fetch per database call.
    save_mode:
        What to do with validated data: return them, write to file, or save via db client.
    output_path:
        Target path for ``save_mode="file"``.
    raise_on_validation_error:
        Whether validation failures should raise or be collected in the result object.
    include_related:
        Whether to request related entities during extraction (handled by concrete pipelines).
    log_level:
        Minimal log level for callback logging.
    log_fn:
        Optional structured log callback accepting a message and context dict.
    """

    table_name: str
    id_field: str
    batch_size: int = 500
    save_mode: Literal["return", "file", "db"] = "return"
    output_path: Optional[str] = None
    raise_on_validation_error: bool = True
    include_related: bool = False
    log_level: str = "INFO"
    log_fn: Optional[LogFunction] = None


__all__ = ["PipelineConfig", "LogFunction"]
