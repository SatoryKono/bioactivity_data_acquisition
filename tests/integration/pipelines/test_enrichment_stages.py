"""Tests for enrichment stage inclusion logic via the global registry."""
# ruff: noqa: E402  # allow path tweaks before imports in this test module

from __future__ import annotations

import sys
import types
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[3]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bioetl.pipelines.base import enrichment_stage_registry
from bioetl.pipelines.document import DocumentPipeline
from bioetl.pipelines.target import TargetPipeline


def _make_target_stub() -> TargetPipeline:
    """Create a minimal TargetPipeline instance without running __init__."""

    pipeline = TargetPipeline.__new__(TargetPipeline)  # type: ignore[call-arg]
    pipeline.runtime_options = {}
    pipeline.stage_context = {}
    pipeline.qc_summary_data = {}
    pipeline.qc_metrics = {}
    pipeline.validation_issues = []
    pipeline.qc_enrichment_metrics = pd.DataFrame()
    pipeline.qc_missing_mappings = pd.DataFrame()
    pipeline.additional_tables = {}
    pipeline.export_metadata = None
    pipeline.debug_dataset_path = None
    pipeline.uniprot_client = object()
    pipeline.iuphar_client = object()
    pipeline.config = types.SimpleNamespace(
        qc=types.SimpleNamespace(thresholds={}, severity_threshold="warning"),
        pipeline=types.SimpleNamespace(name="target"),
    )
    return pipeline


def _make_document_stub() -> DocumentPipeline:
    """Create a minimal DocumentPipeline instance without running __init__."""

    pipeline = DocumentPipeline.__new__(DocumentPipeline)  # type: ignore[call-arg]
    pipeline.runtime_options = {}
    pipeline.stage_context = {}
    pipeline.qc_summary_data = {}
    pipeline.qc_metrics = {}
    pipeline.validation_issues = []
    pipeline.qc_enrichment_metrics = pd.DataFrame()
    pipeline.qc_missing_mappings = pd.DataFrame()
    pipeline.additional_tables = {}
    pipeline.export_metadata = None
    pipeline.debug_dataset_path = None
    pipeline.external_adapters = {}
    pipeline.config = types.SimpleNamespace(
        qc=types.SimpleNamespace(thresholds={}, severity_threshold="warning"),
        pipeline=types.SimpleNamespace(name="document"),
    )
    return pipeline


def test_target_uniprot_stage_disabled() -> None:
    """UniProt stage should report disabled when runtime option is false."""

    pipeline = _make_target_stub()
    stage = {s.name: s for s in enrichment_stage_registry.get(TargetPipeline)}["uniprot"]
    pipeline.runtime_options["with_uniprot"] = False
    df = pd.DataFrame({"uniprot_accession": ["P12345"]})

    include, reason = stage.should_run(pipeline, df)

    assert include is False
    assert reason == "disabled"


def test_target_uniprot_stage_included_with_accessions() -> None:
    """UniProt stage should run when accessions and client are available."""

    pipeline = _make_target_stub()
    stage = {s.name: s for s in enrichment_stage_registry.get(TargetPipeline)}["uniprot"]
    pipeline.runtime_options["with_uniprot"] = True
    df = pd.DataFrame({"uniprot_accession": ["P12345"]})

    include, reason = stage.should_run(pipeline, df)

    assert include is True
    assert reason is None


def test_target_iuphar_stage_toggle() -> None:
    """IUPHAR stage honours runtime option and client availability."""

    stage = {s.name: s for s in enrichment_stage_registry.get(TargetPipeline)}["iuphar"]
    pipeline = _make_target_stub()
    pipeline.runtime_options["with_iuphar"] = False
    df = pd.DataFrame({"target_chembl_id": ["CHEMBL1"]})

    include_disabled, reason_disabled = stage.should_run(pipeline, df)
    assert include_disabled is False
    assert reason_disabled == "disabled"

    pipeline.runtime_options["with_iuphar"] = True
    include_enabled, reason_enabled = stage.should_run(pipeline, df)
    assert include_enabled is True
    assert reason_enabled is None

    pipeline.iuphar_client = None
    include_missing_client, reason_missing_client = stage.should_run(pipeline, df)
    assert include_missing_client is False
    assert reason_missing_client == "client_unavailable"


def test_document_pubmed_stage_disabled_without_option() -> None:
    """Document pubmed stage should skip when runtime flag is disabled."""

    stage = {s.name: s for s in enrichment_stage_registry.get(DocumentPipeline)}["pubmed"]
    pipeline = _make_document_stub()
    pipeline.external_adapters = {"pubmed": object()}
    pipeline.runtime_options["with_pubmed"] = False
    df = pd.DataFrame({"document_chembl_id": ["CHEMBL1"]})

    include, reason = stage.should_run(pipeline, df)

    assert include is False
    assert reason == "disabled"


def test_document_pubmed_stage_requires_adapters() -> None:
    """Document pubmed stage only runs when adapters are configured."""

    stage = {s.name: s for s in enrichment_stage_registry.get(DocumentPipeline)}["pubmed"]
    pipeline = _make_document_stub()
    pipeline.runtime_options["with_pubmed"] = True
    df = pd.DataFrame({"document_chembl_id": ["CHEMBL1"]})

    include_no_adapters, reason_no_adapters = stage.should_run(pipeline, df)
    assert include_no_adapters is False
    assert reason_no_adapters == "no_external_adapters"

    pipeline.external_adapters = {"pubmed": object()}
    include_with_adapters, reason_with_adapters = stage.should_run(pipeline, df)
    assert include_with_adapters is True
    assert reason_with_adapters is None
