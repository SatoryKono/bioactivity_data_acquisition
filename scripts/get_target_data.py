from __future__ import annotations

import argparse
from collections.abc import Iterable, Iterator
from datetime import datetime
from pathlib import Path

import pandas as pd

from library.clients.chembl import ChEMBLClient
from library.config import Config
from library.etl.load import write_deterministic_csv
from library.pipelines.target.chembl_target import iter_target_batches_with_retry
from library.pipelines.target.defaults import TARGET_MODE_DEFAULTS
from library.pipelines.target.pipeline import run_pipeline
from library.pipelines.target.postprocessing import finalise_targets, postprocess_targets
from library.target.config import ApiCfg, TargetChemblBatchRetryCfg, UniprotMappingCfg


def _iter_chunks(ids: list[str], *, chunk_size: int) -> Iterator[Iterable[str]]:
    for i in range(0, len(ids), chunk_size):
        yield ids[i : i + chunk_size]


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and process target data")
    parser.add_argument("mode", choices=("chembl", "uniprot", "iuphar", "all"))
    parser.add_argument("--config", dest="config", default=str(Path("src/library/configs/config_target_full.yaml")))
    parser.add_argument("--input-csv", dest="input_csv", required=True)
    parser.add_argument("--id-column", dest="id_column")
    parser.add_argument("--chunk-size", dest="chunk_size", type=int)
    parser.add_argument("--timeout", dest="timeout", type=float)
    parser.add_argument("--limit", dest="limit", type=int)
    parser.add_argument("--offset", dest="offset", type=int)
    parser.add_argument("--batch-size", dest="batch_size", type=int, default=100)
    parser.add_argument("--output-csv", dest="output_csv")
    parser.add_argument("--log-json", dest="log_json", action="store_true")
    args = parser.parse_args()

    cfg = Config.load(args.config)

    defaults = TARGET_MODE_DEFAULTS[args.mode]
    id_column = args.id_column or defaults.column
    chunk_size = args.chunk_size or defaults.chunk_size
    timeout = args.timeout or defaults.timeout
    offset = args.offset or defaults.offset
    limit = args.limit

    df = pd.read_csv(args.input_csv, dtype=str)
    ids = df[id_column].dropna().astype(str).tolist()
    if offset:
        ids = ids[offset:]
    if limit is not None:
        ids = ids[:limit]

    chembl_client_cfg = next((c for c in cfg.clients if c.name == "chembl"), None)
    if chembl_client_cfg is None:
        raise SystemExit("chembl client config is required in Config")
    chembl_api = ApiCfg(chembl_base=chembl_client_cfg.resolved_base_url, timeout_read=chembl_client_cfg.timeout)
    client = ChEMBLClient(chembl_client_cfg)

    mapping_cfg = UniprotMappingCfg(enabled=False)
    retry_cfg = TargetChemblBatchRetryCfg(enable=True)

    def chunk_iterator() -> Iterator[Iterable[str]]:
        return _iter_chunks(ids, chunk_size=chunk_size)

    def chembl_fetcher(it, api_cfg):
        for batch in it:
            yield from (parsed for _, _, parsed in iter_target_batches_with_retry(
                list(batch), cfg=api_cfg, client=client, mapping_cfg=mapping_cfg, chunk_size=len(list(batch)), timeout=timeout, retry_cfg=retry_cfg
            ))

    result = run_pipeline(
        chunk_iterator,
        chembl_api,
        chembl_fetcher=chembl_fetcher,  # downstream fetchers могут быть добавлены по мере интеграции
        batch_size=args.batch_size,
    )

    if isinstance(result.chembl, pd.DataFrame):
        merged = result.chembl
    else:
        merged = pd.concat(list(result.chembl), ignore_index=True)

    processed = postprocess_targets(merged)
    final = finalise_targets(processed)

    if args.output_csv:
        out_path = Path(args.output_csv)
    else:
        out_dir = Path("data/output/target")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"target_{datetime.utcnow():%Y%m%d}.csv"

    write_deterministic_csv(final, out_path, determinism=cfg.determinism, output=cfg.io.output)


if __name__ == "__main__":
    main()


