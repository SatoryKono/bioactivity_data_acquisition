"""Parsing utilities for nested assay payload attributes."""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.normalizers import registry

logger = UnifiedLogger.get(__name__)


class AssayParser:
    """Parse and expand nested assay payload components."""

    def expand_parameters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expand assay parameters JSON into long-format rows."""

        if "assay_parameters_json" not in df.columns:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        records: list[dict[str, Any]] = []

        for _, row in df.iterrows():
            params_raw = row.get("assay_parameters_json")
            if not params_raw or (isinstance(params_raw, float) and pd.isna(params_raw)):
                continue

            try:
                params = json.loads(params_raw) if isinstance(params_raw, str) else params_raw
            except (TypeError, ValueError):
                logger.warning("assay_param_parse_failed", assay_chembl_id=row.get("assay_chembl_id"))
                continue

            if not isinstance(params, Iterable):
                continue

            index = 0
            for param in params:
                if not isinstance(param, dict):
                    continue

                record = {
                    "assay_chembl_id": row.get("assay_chembl_id"),
                    "row_subtype": "param",
                    "row_index": index,
                    "assay_param_type": param.get("type"),
                    "assay_param_relation": registry.normalize(
                        "chemistry.relation",
                        param.get("relation"),
                        default="=",
                    ),
                    "assay_param_value": param.get("value"),
                    "assay_param_units": registry.normalize(
                        "chemistry.units",
                        param.get("units"),
                    ),
                    "assay_param_text_value": param.get("text_value"),
                    "assay_param_standard_type": param.get("standard_type"),
                    "assay_param_standard_value": param.get("standard_value"),
                    "assay_param_standard_units": registry.normalize(
                        "chemistry.units",
                        param.get("standard_units"),
                    ),
                }
                records.append(record)
                index += 1

        if not records:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        return pd.DataFrame(records)

    def expand_classifications(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expand assay classifications JSON into long-format rows."""

        if "assay_classifications" not in df.columns:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        records: list[dict[str, Any]] = []

        for _, row in df.iterrows():
            class_raw = row.get("assay_classifications")
            if not class_raw or (isinstance(class_raw, float) and pd.isna(class_raw)):
                continue

            parsed: Iterable[dict[str, Any]] | None = None
            if isinstance(class_raw, str):
                try:
                    parsed_json = json.loads(class_raw)
                    if isinstance(parsed_json, list):
                        parsed = parsed_json
                except (TypeError, ValueError):
                    logger.warning(
                        "assay_class_parse_failed",
                        assay_chembl_id=row.get("assay_chembl_id"),
                    )
            elif isinstance(class_raw, Iterable):
                parsed = class_raw

            if not parsed:
                continue

            index = 0
            for classification in parsed:
                if not isinstance(classification, dict):
                    continue

                record = {
                    "assay_chembl_id": row.get("assay_chembl_id"),
                    "row_subtype": "class",
                    "row_index": index,
                    "assay_class_id": classification.get("assay_class_id"),
                    "assay_class_bao_id": classification.get("bao_id"),
                    "assay_class_type": classification.get("class_type"),
                    "assay_class_l1": classification.get("l1"),
                    "assay_class_l2": classification.get("l2"),
                    "assay_class_l3": classification.get("l3"),
                    "assay_class_description": classification.get("description"),
                }
                records.append(record)
                index += 1

        if not records:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        return pd.DataFrame(records)
