from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from library.config import Config
from library.etl.load import write_deterministic_csv
from library.pipelines.target.postprocessing import finalise_targets, postprocess_targets


def main() -> None:
    parser = argparse.ArgumentParser(description="Postprocess target table")
    parser.add_argument("--config", dest="config", default=str(Path("src/library/configs/config_target_full.yaml")))
    parser.add_argument("--input-csv", dest="input_csv", required=True)
    parser.add_argument("--output-csv", dest="output_csv", required=True)
    parser.add_argument("--sep", dest="sep")
    parser.add_argument("--encoding", dest="encoding")
    args = parser.parse_args()

    cfg = Config.load(args.config)

    df = pd.read_csv(args.input_csv, dtype=str, sep=args.sep or ",", encoding=args.encoding or "utf-8")
    processed = postprocess_targets(df)
    final = finalise_targets(processed)

    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_deterministic_csv(final, out_path, determinism=cfg.determinism, output=cfg.io.output)


if __name__ == "__main__":
    main()


