from __future__ import annotations

from pathlib import Path
import shutil

from bioetl.pipelines.chembl.activity.run import ChemblActivityPipeline
from tests.support.factories import build_pipeline_config, load_sample_activity_dataframe


def main() -> None:
    snapshot_root = Path("tests/golden/activity_chembl/v1").resolve()
    build_root = snapshot_root / "_build"
    output_root = build_root / "output"
    output_root.mkdir(parents=True, exist_ok=True)

    config = build_pipeline_config(output_root)
    config.validation.schema_out = "bioetl.schemas.chembl_activity_schema:ActivitySchema"
    config.determinism.sort.by = ["activity_id"]
    config.determinism.sort.ascending = [True]
    config.determinism.hashing.business_key_fields = ("activity_id",)

    pipeline = ChemblActivityPipeline(config=config, run_id="golden-activity-v1")
    frame = load_sample_activity_dataframe()
    transformed = pipeline.transform(frame)
    validated = pipeline.validate(transformed)
    result = pipeline.write(validated, pipeline.pipeline_directory, extended=True)

    copy_pairs: list[tuple[Path, Path]] = [
        (
            result.write_result.dataset,
            snapshot_root / "dataset" / result.write_result.dataset.name,
        ),
    ]

    if result.write_result.metadata is not None:
        copy_pairs.append(
            (
                result.write_result.metadata,
                snapshot_root / "meta" / result.write_result.metadata.name,
            )
        )
    if result.write_result.quality_report is not None:
        copy_pairs.append(
            (
                result.write_result.quality_report,
                snapshot_root / "qc" / result.write_result.quality_report.name,
            )
        )
    if result.write_result.correlation_report is not None:
        copy_pairs.append(
            (
                result.write_result.correlation_report,
                snapshot_root / "qc" / result.write_result.correlation_report.name,
            )
        )
    if result.write_result.qc_metrics is not None:
        copy_pairs.append(
            (
                result.write_result.qc_metrics,
                snapshot_root / "qc" / result.write_result.qc_metrics.name,
            )
        )
    for src, dst in copy_pairs:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    if result.manifest is not None:
        manifest_dst = snapshot_root / "manifest" / result.manifest.name
        manifest_dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(result.manifest, manifest_dst)

    shutil.rmtree(build_root)


if __name__ == "__main__":
    main()

