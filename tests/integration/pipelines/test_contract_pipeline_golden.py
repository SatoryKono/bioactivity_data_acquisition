"""Contract fixture backed E2E test for deterministic pipeline outputs.""" 

from __future__ import annotations

import json as jsonlib
import time
import types
from collections.abc import Mapping, Sequence
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import pytest
from requests import Response
from requests.structures import CaseInsensitiveDict
from structlog.testing import capture_logs

from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.hashing import generate_hash_business_key, generate_hash_row
from bioetl.core.logger import UnifiedLogger
from bioetl.core.pagination import PageNumberPaginationStrategy, PageNumberRequest
from bioetl.pipelines.base import PipelineBase

from tests.golden.helpers import (
    compare_files_bitwise,
    snapshot_artifacts,
    verify_bit_identical_outputs,
)

pytestmark = pytest.mark.integration


class ContractHTTPFixture:
    """Replay recorded HTTP contract interactions for a UnifiedAPIClient."""

    def __init__(self, entries: Sequence[Mapping[str, object]]) -> None:
        self._entries = list(entries)
        self._index = 0

    def execute(
        self,
        _client: UnifiedAPIClient,
        *,
        method: str,
        url: str,
        params: Mapping[str, object] | None,
        data: Mapping[str, object] | None,
        json: Mapping[str, object] | None,
        stream: bool,
        headers: Mapping[str, str] | None,
    ) -> Response:
        if self._index >= len(self._entries):  # pragma: no cover - defensive guard
            raise AssertionError(f"Unexpected HTTP call: {method} {url}")

        entry = self._entries[self._index]
        self._index += 1

        expected = entry["request"]
        response_spec = entry["response"]

        parsed = urlparse(url)
        assert parsed.path == expected["path"]
        assert method.upper() == str(expected["method"]).upper()

        expected_params = {
            str(key): str(value)
            for key, value in expected.get("params", {}).items()
        }
        actual_params = {
            str(key): str(value)
            for key, value in (params or {}).items()
        }
        assert actual_params == expected_params

        response = Response()
        response.status_code = int(response_spec.get("status", 200))
        response._content = jsonlib.dumps(response_spec.get("body", {})).encode("utf-8")
        response.headers = CaseInsensitiveDict(
            response_spec.get("headers", {"Content-Type": "application/json"})
        )
        response.url = url
        return response

    def assert_consumed(self) -> None:
        """Ensure all contract entries have been replayed."""

        assert self._index == len(self._entries)


class ContractPipeline(PipelineBase):
    """Pipeline that extracts via a recorded HTTP contract fixture."""

    PIPELINE_VERSION = "1.2.3-contract"
    SOURCE_SYSTEM = "contract-fixture"
    CHEMBL_RELEASE = "ChEMBL_33"
    CLIENT_CONFIG = APIConfig(
        name="contract",
        base_url="https://contract.test/api",
        cache_enabled=False,
    )

    def __init__(
        self,
        config: types.SimpleNamespace,
        run_id: str,
        http_fixture: ContractHTTPFixture,
    ) -> None:
        super().__init__(config, run_id)
        self.http_fixture = http_fixture
        self.client = UnifiedAPIClient(self.CLIENT_CONFIG)
        self.client._execute = types.MethodType(http_fixture.execute, self.client)

    @staticmethod
    def _parse_batch(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
        results = payload.get("results")
        if isinstance(results, Sequence):
            return [
                dict(item)
                for item in results
                if isinstance(item, Mapping)
            ]
        return []

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:  # noqa: D401 - stub
        UnifiedLogger.set_context(
            run_id=self.run_id,
            stage="extract",
            actor="pipeline",
            source=self.SOURCE_SYSTEM,
        )
        log = UnifiedLogger.get(__name__)

        start = time.perf_counter()
        strategy = PageNumberPaginationStrategy(
            self.client,
            page_param="page",
            page_size_param="pageSize",
            offset_param=None,
            limit_param=None,
        )
        request = PageNumberRequest(
            path="/compounds",
            page_size=2,
            unique_key="compound_id",
            parser=self._parse_batch,
        )
        batches = strategy.paginate(request)
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        log.info(
            "contract_extract_complete",
            rows_in=len(batches),
            rows_out=len(batches),
            elapsed_ms=round(elapsed_ms, 3),
        )

        frame = pd.DataFrame(batches)
        if not frame.empty:
            frame["extracted_at"] = "2024-01-01T00:00:00+00:00"
        return frame

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - stub
        if df.empty:
            return df.copy()

        working = df.copy()
        working.insert(0, "index", range(1, len(working) + 1))
        working["hash_business_key"] = working["compound_id"].apply(
            lambda value: generate_hash_business_key({"compound_id": value})
        )
        hash_inputs = working[
            [
                "compound_id",
                "assay_type",
                "canonical_smiles",
                "result_value",
                "extracted_at",
            ]
        ].to_dict("records")
        working["hash_row"] = [generate_hash_row(payload) for payload in hash_inputs]
        working["pipeline_version"] = self.PIPELINE_VERSION
        working["run_id"] = self.run_id
        working["source_system"] = self.SOURCE_SYSTEM
        working["chembl_release"] = self.CHEMBL_RELEASE

        ordered_columns = [
            "index",
            "hash_row",
            "hash_business_key",
            "pipeline_version",
            "run_id",
            "source_system",
            "chembl_release",
            "compound_id",
            "assay_type",
            "canonical_smiles",
            "result_value",
            "extracted_at",
        ]
        ordered_frame = working[ordered_columns]

        self.set_export_metadata_from_dataframe(
            ordered_frame,
            pipeline_version=self.PIPELINE_VERSION,
            source_system=self.SOURCE_SYSTEM,
            chembl_release=self.CHEMBL_RELEASE,
        )
        self.set_qc_metrics({"rows_in": int(len(df)), "rows_out": int(len(ordered_frame))})
        self.refresh_validation_issue_summary()
        return ordered_frame

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - stub
        return df

    def close_resources(self) -> None:  # noqa: D401 - stub
        self.http_fixture.assert_consumed()


def _load_contract_entries() -> list[Mapping[str, object]]:
    path = Path(__file__).with_name("data") / "contract_pipeline_http.json"
    payload = jsonlib.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):  # pragma: no cover - defensive guard
        raise TypeError("Contract payload must be a list of request/response entries")
    return [entry for entry in payload if isinstance(entry, Mapping)]


def _make_config() -> types.SimpleNamespace:
    determinism_section = types.SimpleNamespace(
        column_order=[
            "index",
            "hash_row",
            "hash_business_key",
            "pipeline_version",
            "run_id",
            "source_system",
            "chembl_release",
            "compound_id",
            "assay_type",
            "canonical_smiles",
            "result_value",
            "extracted_at",
        ],
        float_precision=4,
        datetime_format="iso8601",
        sort=types.SimpleNamespace(by=["index"], ascending=[True]),
        hash_policy_version=None,
    )
    pipeline_section = types.SimpleNamespace(
        name="contract",
        entity="contract",
        version=ContractPipeline.PIPELINE_VERSION,
    )
    qc_section = types.SimpleNamespace(severity_threshold="error")
    sources = {"contract": types.SimpleNamespace(enabled=True)}

    return types.SimpleNamespace(
        pipeline=pipeline_section,
        qc=qc_section,
        cli={},
        determinism=determinism_section,
        config_hash="contract-config-hash",
        sources=sources,
    )


@pytest.mark.determinism
@pytest.mark.usefixtures("frozen_time")
def test_contract_pipeline_matches_golden_snapshot(tmp_path: Path) -> None:
    """Replay recorded contract interactions and verify deterministic artefacts."""

    config = _make_config()
    output_path = tmp_path / "materialised" / "datasets" / "contract.csv"

    first_fixture = ContractHTTPFixture(_load_contract_entries())
    pipeline = ContractPipeline(config, run_id="contract-run", http_fixture=first_fixture)

    with capture_logs() as logs:
        first_artifacts = pipeline.run(output_path, extended=True)
    first_fixture.assert_consumed()

    first_snapshot = snapshot_artifacts(first_artifacts, tmp_path / "snapshot_first")
    dataset_path = first_snapshot.dataset
    assert dataset_path is not None and dataset_path.exists()

    golden_dataset = Path(__file__).with_name("golden") / "contract_pipeline_dataset.csv"
    identical, diagnostic = compare_files_bitwise(dataset_path, golden_dataset)
    assert identical, diagnostic

    events = [event for event in logs if event.get("event") == "contract_extract_complete"]
    assert events, "Expected a contract_extract_complete log entry"
    metrics = events[0]
    assert metrics["rows_in"] == 2
    assert metrics["rows_out"] == 2
    assert metrics["elapsed_ms"] >= 0

    second_fixture = ContractHTTPFixture(_load_contract_entries())
    second_pipeline = ContractPipeline(config, run_id="contract-run", http_fixture=second_fixture)
    second_artifacts = second_pipeline.run(output_path, extended=True)
    second_fixture.assert_consumed()

    second_snapshot = snapshot_artifacts(second_artifacts, tmp_path / "snapshot_second")
    identical, errors = verify_bit_identical_outputs(first_snapshot, second_snapshot)
    assert identical, "Outputs diverged:\n- " + "\n- ".join(errors)
