from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path
from uuid import UUID

import pytest
import yaml
from jsonschema import Draft202012Validator

from library.release.meta import META_SCHEMA, ReleaseMetadataError, write_meta


def test_write_meta_creates_valid_files(tmp_path: Path) -> None:
    data_dir = tmp_path / "outputs" / "final"
    data_dir.mkdir(parents=True)
    data_path = data_dir / "activities.csv"
    data_path.write_text("col\n1\n", encoding="utf-8")

    meta_dir = tmp_path / "outputs" / "meta"
    meta_path = write_meta(
        meta_dir=meta_dir,
        pipeline_version="1.2.3",
        chembl_release="ChEMBL_32",
        chembl_release_source="cli",
        data_paths=[data_path],
        row_count=1,
    )

    assert meta_path.exists()

    meta = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
    Draft202012Validator(META_SCHEMA).validate(meta)

    UUID(meta["run_id"], version=4)
    started = datetime.strptime(meta["started_at"], "%Y-%m-%dT%H:%M:%SZ")
    finished = datetime.strptime(meta["finished_at"], "%Y-%m-%dT%H:%M:%SZ")
    assert finished >= started
    assert meta["current_year"] >= 2000
    assert meta["checksums"][data_path.name] == hashlib.sha256(data_path.read_bytes()).hexdigest()

    checksum_path = meta_dir / "meta.sha256"
    assert checksum_path.exists()
    assert checksum_path.read_text(encoding="utf-8").strip() == hashlib.sha256(meta_path.read_bytes()).hexdigest()


def test_write_meta_rejects_unknown_release(tmp_path: Path) -> None:
    with pytest.raises(ReleaseMetadataError) as exc:
        write_meta(
            meta_dir=tmp_path / "meta",
            pipeline_version="0.1.0",
            chembl_release="unknown",
            chembl_release_source="cli",
            data_paths=[],
            row_count=0,
        )
    assert exc.value.code == "chembl_release_unknown"
