from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd
import pandera as pa
import yaml

from bioetl.core.pipeline.base import PipelineBase
from bioetl.core.validation.pandera_validator import PanderaSchemaProvider, PanderaValidator


@dataclass(frozen=True)
class PipelineSpec:
    name: str
    business_key: tuple[str, ...]
    required_fields: tuple[str, ...]
    optional_fields: tuple[str, ...] = ()
    require_hash_business_key: bool = False
    enforce_source_value: bool = False


class FileBasedPipeline(PipelineBase):
    """Базовый пайплайн для источников ChEMBL, работающих с локальными файлами."""

    def __init__(
        self,
        run_id: str,
        *,
        config: Mapping[str, object],
        spec: PipelineSpec,
        schema: pa.DataFrameSchema,
        strict_validation: bool = False,
    ) -> None:
        self.config = config
        self.spec = spec
        validator = PanderaValidator(PanderaSchemaProvider(schema))
        super().__init__(run_id, validator=validator, strict_validation=strict_validation)

    def extract(self) -> pd.DataFrame:
        source_conf = self.config.get("source", {}) if isinstance(self.config, Mapping) else {}
        if not isinstance(source_conf, Mapping):
            raise ValueError("source конфигурация должна быть словарём")
        path = source_conf.get("path")
        if not path:
            raise ValueError("source.path обязателен для загрузки данных")
        input_path = Path(str(path))
        if not input_path.exists():
            raise FileNotFoundError(f"Источник данных не найден: {input_path}")

        if input_path.suffix.lower() == ".csv":
            df = pd.read_csv(input_path)
        elif input_path.suffix.lower() in {".json", ".ndjson"}:
            df = pd.read_json(input_path, lines=input_path.suffix.lower() == ".ndjson")
        else:
            raise ValueError(f"Неподдерживаемый формат источника: {input_path.suffix}")

        self.logger.info(
            "extracted", stage="extract", rows=df.shape[0], columns=df.columns.tolist()
        )
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = self._ensure_load_meta(df)
        df = self._ensure_source(df)
        self._ensure_business_key_columns(df)
        self._ensure_required_fields(df)
        self._ensure_hash_business_key(df)
        self._ensure_hash_row(df)
        df = self._order_columns(df)
        self.logger.info("transformed", stage="transform", rows=df.shape[0])
        return df

    def _ensure_load_meta(self, df: pd.DataFrame) -> pd.DataFrame:
        load_meta = self.config.get("load_meta_id") if isinstance(self.config, Mapping) else None
        if "load_meta_id" not in df.columns:
            if not load_meta:
                raise ValueError("load_meta_id должен быть в данных или конфигурации")
            df["load_meta_id"] = load_meta
        return df

    def _ensure_source(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.spec.enforce_source_value:
            source_value = None
            if isinstance(self.config, Mapping):
                source_value = self.config.get("source_name") or self.config.get("source", {}).get("name")
            if source_value and "source" not in df.columns:
                df["source"] = source_value
            if "source" not in df.columns or df["source"].isna().any():
                raise ValueError("Колонка source обязательна для данного пайплайна")
        return df

    def _ensure_business_key_columns(self, df: pd.DataFrame) -> None:
        missing = [col for col in self.spec.business_key if col not in df.columns]
        if missing:
            raise ValueError(f"В данных отсутствуют колонки бизнес-ключа: {missing}")
        for col in self.spec.business_key:
            if df[col].isna().any():
                raise ValueError(f"Колонка бизнес-ключа {col} не может содержать NA")

    def _ensure_required_fields(self, df: pd.DataFrame) -> None:
        missing = [col for col in self.spec.required_fields if col not in df.columns]
        if missing:
            raise ValueError(f"Отсутствуют обязательные поля: {missing}")
        for col in self.spec.required_fields:
            if df[col].isna().any():
                raise ValueError(f"Обязательное поле {col} не может содержать NA")

    def _ensure_hash_business_key(self, df: pd.DataFrame) -> None:
        if "hash_business_key" not in df.columns:
            df["hash_business_key"] = None
        missing_mask = df["hash_business_key"].isna()
        if missing_mask.any():
            df.loc[missing_mask, "hash_business_key"] = df.loc[
                missing_mask, self.spec.business_key
            ].apply(lambda row: self._hash_values(row.tolist()), axis=1)
        if self.spec.require_hash_business_key and df["hash_business_key"].isna().any():
            raise ValueError("hash_business_key обязателен для данного пайплайна")

    def _ensure_hash_row(self, df: pd.DataFrame) -> None:
        if "hash_row" not in df.columns:
            df["hash_row"] = None
        missing_mask = df["hash_row"].isna()
        if missing_mask.any():
            columns = sorted(df.columns)
            df.loc[missing_mask, "hash_row"] = df.loc[missing_mask, columns].apply(
                lambda row: self._hash_values(row.tolist()), axis=1
            )

    def _order_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        required = list(dict.fromkeys(self.spec.business_key + ("load_meta_id", "hash_row")))
        if "hash_business_key" in df.columns:
            required.insert(0, "hash_business_key")
        remaining = [col for col in df.columns if col not in required]
        return df[required + remaining]

    @staticmethod
    def _hash_values(values: Iterable[object]) -> str:
        normalized = "|".join("" if v is None else str(v) for v in values)
        return sha256(normalized.encode("utf-8")).hexdigest()


def load_pipeline_config(path: str | Path, pipeline_name: str) -> Mapping[str, object]:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config {config_path} не найден")
    config = yaml.safe_load(config_path.read_text()) or {}
    pipelines_cfg = config.get("pipelines") if isinstance(config, Mapping) else None
    if isinstance(pipelines_cfg, Mapping) and pipeline_name in pipelines_cfg:
        return pipelines_cfg[pipeline_name] or {}
    return config
