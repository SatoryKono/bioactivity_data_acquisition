from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Ensure 'src' is on sys.path when running as: python src/scripts/get_target_data.py
_THIS_DIR = Path(__file__).resolve().parent
_SRC_DIR = _THIS_DIR.parent
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from library.clients.chembl import ChEMBLClient  # noqa: E402
from library.config import Config, DeterminismSettings, SortSettings  # noqa: E402
from library.etl.load import write_deterministic_csv  # noqa: E402
from library.pipelines.target.chembl_target import (  # noqa: E402
    iter_target_batches_with_retry,
)
from library.pipelines.target.defaults import TARGET_MODE_DEFAULTS  # noqa: E402
from library.pipelines.target.iuphar_target import (  # noqa: E402
    IupharApiCfg,
    enrich_targets_with_iuphar,
)
from library.pipelines.target.pipeline import run_pipeline  # noqa: E402
from library.pipelines.target.postprocessing import (  # noqa: E402
    finalise_targets,
    postprocess_targets,
)
from library.pipelines.target.uniprot_target import (  # noqa: E402
    UniprotApiCfg,
    enrich_targets_with_uniprot,
)
from library.target.config import (  # noqa: E402
    ApiCfg,
    TargetChemblBatchRetryCfg,
    UniprotMappingCfg,
)

logger = logging.getLogger(__name__)


def _iter_chunks(ids: list[str], *, chunk_size: int) -> Iterator[Iterable[str]]:
    for i in range(0, len(ids), chunk_size):
        yield ids[i : i + chunk_size]


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and process target data")
    parser.add_argument("--mode", choices=("chembl", "uniprot", "iuphar", "all"), default="all")
    parser.add_argument("--config", dest="config", default=str(Path("configs/config_target_full.yaml")))
    parser.add_argument("--input", dest="input_csv", required=True)
    parser.add_argument("--id-column", dest="id_column")
    parser.add_argument("--chunk-size", dest="chunk_size", type=int)
    parser.add_argument("--timeout", dest="timeout", type=float)
    parser.add_argument("--limit", dest="limit", type=int)
    parser.add_argument("--offset", dest="offset", type=int)
    parser.add_argument("--batch-size", dest="batch_size", type=int, default=100)
    parser.add_argument("--output", dest="output_csv")
    parser.add_argument("--log-json", dest="log_json", action="store_true")
    args = parser.parse_args()

    cfg = Config.load(args.config)

    defaults = TARGET_MODE_DEFAULTS[args.mode]
    id_column = args.id_column or defaults.column
    chunk_size = args.chunk_size or defaults.chunk_size
    timeout = args.timeout or defaults.timeout
    offset = args.offset or defaults.offset
    limit = args.limit

    df = pd.read_csv(args.input_csv, dtype=str, sep=',')
    if id_column not in df.columns:
        raise SystemExit(f"Missing required column: {id_column}")
    ids = df[id_column].dropna().astype(str).tolist()
    if offset:
        ids = ids[offset:]
    if limit is not None:
        ids = ids[:limit]

    chembl_client_cfg = next((c for c in cfg.clients if c.name == "chembl"), None)
    if chembl_client_cfg is None:
        raise SystemExit("chembl client config is required in Config")
    # Используем base_url без endpoint, чтобы не дублировать /target
    base_url = str(chembl_client_cfg.base_url).rstrip("/")
    chembl_api = ApiCfg(chembl_base=base_url, timeout_read=chembl_client_cfg.timeout)
    client = ChEMBLClient(chembl_client_cfg)
    # Настройки маппинга из конфига
    # Получаем настройки из YAML конфига напрямую
    import yaml

    with open(args.config, encoding="utf-8") as f:
        yaml_cfg = yaml.safe_load(f)

    uniprot_mapping_config = yaml_cfg.get("target", {}).get("uniprot", {}).get("mapping", {})
    mapping_cfg = UniprotMappingCfg(
        enabled=uniprot_mapping_config.get("enabled", False),
        mapping_file=uniprot_mapping_config.get("fallback", {}).get("mapping_file"),
        mapping_columns=uniprot_mapping_config.get("fallback", {}).get("mapping_columns"),
        mapping_service_url=uniprot_mapping_config.get("mapping_service_url"),
    )
    retry_cfg = TargetChemblBatchRetryCfg(enable=True)

    def chunk_iterator() -> Iterator[Iterable[str]]:
        return _iter_chunks(ids, chunk_size=chunk_size)

    def chembl_fetcher(iterator: Iterator[Iterable[str]], api_cfg: ApiCfg, *, batch_size: int = 100) -> Iterator[pd.DataFrame]:
        del batch_size  # unused, kept for interface compatibility
        for batch in iterator:
            batch_list = list(batch)
            for _, _, parsed in iter_target_batches_with_retry(
                batch_list,
                cfg=api_cfg,
                client=client,
                mapping_cfg=mapping_cfg,
                chunk_size=len(batch_list),
                timeout=timeout,
                retry_cfg=retry_cfg,
            ):
                yield parsed

    # Настройки UniProt API
    uniprot_cfg = UniprotApiCfg(
        base_url="https://rest.uniprot.org",
        timeout=120.0,
        batch_size=100,  # Увеличено для лучшей производительности
        rate_limit_delay=0.1,
    )

    def uniprot_fetcher(chembl_df: pd.DataFrame, cfg: UniprotApiCfg) -> pd.DataFrame:
        """UniProt fetcher function for pipeline."""
        return enrich_targets_with_uniprot(chembl_df, cfg, uniprot_id_column="mapping_uniprot_id", batch_size=cfg.batch_size)

    # Настройки IUPHAR: приоритет локальные словари, fallback на API
    iuphar_cfg = IupharApiCfg(
        use_csv=True,
        dictionary_path="configs/dictionary/_target/",
        family_file="_IUPHAR_family.csv",
        target_file="_IUPHAR_target.csv",
        base_url="https://www.guidetopharmacology.org/services",
        timeout=30.0,
        batch_size=50,
        rate_limit_delay=0.2,
    )

    def iuphar_fetcher(chembl_df: pd.DataFrame, cfg: IupharApiCfg) -> pd.DataFrame:
        """IUPHAR fetcher using dictionaries with API fallback."""
        return enrich_targets_with_iuphar(chembl_df, cfg, batch_size=cfg.batch_size)

    result = run_pipeline(
        chunk_iterator,
        chembl_api,
        chembl_fetcher=chembl_fetcher,
        uniprot_fetcher=uniprot_fetcher,
        uniprot_cfg=uniprot_cfg,
        iuphar_fetcher=iuphar_fetcher,
        iuphar_cfg=iuphar_cfg,
        batch_size=args.batch_size,
    )

    if isinstance(result.chembl, pd.DataFrame):
        merged = result.chembl
    else:
        merged = pd.concat(list(result.chembl), ignore_index=True)

    # Если есть данные UniProt, объединяем их
    if result.uniprot is not None:
        if isinstance(result.uniprot, pd.DataFrame):
            uniprot_data = result.uniprot
        else:
            uniprot_data = pd.concat(list(result.uniprot), ignore_index=True)

        # Проверяем, что есть данные для объединения
        if not uniprot_data.empty and "uniprot_id_primary" in uniprot_data.columns:
            # Объединяем данные ChEMBL с данными UniProt
            merged = merged.merge(uniprot_data, left_on="mapping_uniprot_id", right_on="uniprot_id_primary", how="left")
        else:
            logger.warning("No valid UniProt data to merge")

    # Если есть данные IUPHAR, объединяем их
    if result.iuphar is not None:
        if isinstance(result.iuphar, pd.DataFrame):
            iuphar_data = result.iuphar
        else:
            iuphar_data = pd.concat(list(result.iuphar), ignore_index=True)

        # Проверяем, что есть данные для объединения
        if not iuphar_data.empty and "iuphar_target_id" in iuphar_data.columns:
            # Объединяем данные с данными IUPHAR
            # Используем индекс для объединения, так как у нас нет прямого ключа
            merged = merged.merge(iuphar_data, left_index=True, right_index=True, how="left")
        else:
            logger.warning("No valid IUPHAR data to merge")

    logger.info(f"Data before postprocessing: {len(merged)} rows")
    logger.info(f"Columns before postprocessing: {list(merged.columns)}")
    processed = postprocess_targets(merged)
    logger.info(f"Data after postprocessing: {len(processed)} rows")
    logger.info(f"Columns after postprocessing: {list(processed.columns)}")
    if "uniprotkb_Id" in processed.columns:
        logger.info(f"uniprotkb_Id values: {processed['uniprotkb_Id'].tolist()}")
    final = finalise_targets(processed)
    logger.info(f"Data after finalization: {len(final)} rows")

    if args.output_csv:
        out_path = Path(args.output_csv)
    else:
        out_dir = Path("data/output/target")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"target_{datetime.now(timezone.utc):%Y%m%d}.csv"

    determinism = DeterminismSettings(
        sort=SortSettings(by=["target_chembl_id"], ascending=[True], na_position="last"),
        column_order=list(final.columns),
    )
    write_deterministic_csv(final, out_path, determinism=determinism, output=cfg.io.output)


if __name__ == "__main__":
    main()
