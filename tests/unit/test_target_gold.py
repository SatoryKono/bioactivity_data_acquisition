"""Unit tests for gold-layer helpers."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Load target_gold helpers without importing the full bioetl package.
# The production package depends on optional third-party libraries (e.g.
# cachetools) that are not required for unit tests.  To keep the test fixture
# lightweight we register a stub ``bioetl.core.logger`` module that provides the
# ``UnifiedLogger`` interface expected by ``target_gold``.
# ---------------------------------------------------------------------------


class _StubLogger:
    """Minimal logger stub used during unit tests."""

    def __getattr__(self, _: str):  # pragma: no cover - trivial passthrough
        def _noop(*args: object, **kwargs: object) -> None:
            return None

        return _noop


class _StubUnifiedLogger:
    """Replacement for :class:`bioetl.core.logger.UnifiedLogger`."""

    @staticmethod
    def get(name: str) -> _StubLogger:  # pragma: no cover - trivial
        return _StubLogger()


logger_module = types.ModuleType("bioetl.core.logger")
logger_module.UnifiedLogger = _StubUnifiedLogger

bioetl_module = sys.modules.setdefault("bioetl", types.ModuleType("bioetl"))
core_module = sys.modules.setdefault("bioetl.core", types.ModuleType("bioetl.core"))
core_module.logger = logger_module
sys.modules["bioetl.core.logger"] = logger_module

module_path = Path(__file__).resolve().parents[2] / "src" / "bioetl" / "pipelines" / "target_gold.py"
spec = importlib.util.spec_from_file_location("target_gold", module_path)
target_gold = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
sys.modules[spec.name] = target_gold
spec.loader.exec_module(target_gold)  # type: ignore[attr-defined]

coalesce_by_priority = target_gold.coalesce_by_priority
merge_components = target_gold.merge_components
expand_xrefs = target_gold.expand_xrefs
annotate_source_rank = target_gold.annotate_source_rank


def test_coalesce_by_priority_selects_first_available_value() -> None:
    """The first non-null value in priority order should be selected."""

    df = pd.DataFrame(
        {
            "pref_name": ["Chembl", None, None],
            "uniprot_protein_name": [None, "UniProt", None],
            "iuphar_name": ["IUPHAR", "IUPHAR-2", "IUPHAR-3"],
        }
    )

    mapping = {
        "preferred_name": [
            ("pref_name", "chembl"),
            ("uniprot_protein_name", "uniprot"),
            ("iuphar_name", "iuphar"),
        ]
    }

    result = coalesce_by_priority(df, mapping)

    assert result["preferred_name"].tolist() == ["Chembl", "UniProt", "IUPHAR-3"]
    assert result["preferred_name_source"].tolist() == ["chembl", "uniprot", "iuphar"]


def test_merge_components_combines_isoforms_and_creates_missing_canonicals() -> None:
    """Merge components should blend canonical records with isoform enrichment."""

    chembl_components = pd.DataFrame(
        [
            {
                "target_chembl_id": "CHEMBL1",
                "component_id": 10,
                "accession": "P12345",
                "component_type": "PROTEIN",
            },
            {
                "target_chembl_id": "CHEMBL2",
                "component_id": 11,
                "accession": "Q11111",
                "component_type": "PROTEIN",
            },
        ]
    )

    enrichment_components = pd.DataFrame(
        [
            {
                "canonical_accession": "P12345",
                "isoform_accession": "P12345",
                "isoform_name": "Canonical",
                "is_canonical": True,
                "sequence_length": 500,
                "source": "uniprot",
            },
            {
                "canonical_accession": "P12345",
                "isoform_accession": "P12345-2",
                "isoform_name": "Isoform 2",
                "is_canonical": False,
                "sequence_length": 450,
                "source": "uniprot",
            },
            {
                "canonical_accession": "X99999",
                "isoform_accession": "X99999",
                "is_canonical": True,
                "sequence_length": 320,
                "source": "ortholog",
            },
            {
                "canonical_accession": "X99999",
                "isoform_accession": "X99999-1",
                "isoform_name": "Ortholog Isoform",
                "is_canonical": False,
                "sequence_length": 300,
                "source": "ortholog",
            },
        ]
    )

    targets = pd.DataFrame(
        [
            {"target_chembl_id": "CHEMBL3", "uniprot_canonical_accession": "X99999"},
        ]
    )

    result = merge_components(chembl_components, enrichment_components, targets=targets)

    # Canonical sequence length propagated to ChEMBL record
    canonical = result[(result["target_chembl_id"] == "CHEMBL1") & (result["isoform_accession"] == "P12345")]
    assert int(canonical.iloc[0]["sequence_length"]) == 500

    # Isoform row created with dedicated accession
    isoform = result[(result["target_chembl_id"] == "CHEMBL1") & (result["isoform_accession"] == "P12345-2")]
    assert not isoform.empty
    assert bool(isoform.iloc[0]["is_canonical"]) is False

    # New canonical component created for CHEMBL3 via enrichment
    created = result[(result["target_chembl_id"] == "CHEMBL3") & (result["isoform_accession"] == "X99999")]
    assert not created.empty
    assert bool(created.iloc[0]["is_canonical"]) is True

    # Additional ortholog isoform present
    ortholog_isoform = result[(result["target_chembl_id"] == "CHEMBL3") & (result["isoform_accession"] == "X99999-1")]
    assert not ortholog_isoform.empty
    assert bool(ortholog_isoform.iloc[0]["is_canonical"]) is False

    # Component identifiers remain unique
    assert result["component_id"].astype(str).is_unique


def test_expand_xrefs_supports_strings_and_lists() -> None:
    """JSON encoded cross-references should expand into flat rows."""

    df = pd.DataFrame(
        [
            {
                "target_chembl_id": "CHEMBL1",
                "component_xrefs": '[{"xref_src_db": "UniProt", "xref_id": "P12345", "component_id": 10}]',
            },
            {
                "target_chembl_id": "CHEMBL2",
                "component_xrefs": [{"xref_src_db": "PDB", "xref_id": "2XAA"}],
            },
            {"target_chembl_id": "CHEMBL3", "component_xrefs": None},
        ]
    )

    result = expand_xrefs(df)

    assert len(result) == 2
    assert set(result["xref_src_db"]) == {"UniProt", "PDB"}
    assert set(result["target_chembl_id"]) == {"CHEMBL1", "CHEMBL2"}


def test_annotate_source_rank_assigns_priority_values() -> None:
    """Ranking column is assigned based on configured priority order."""

    df = pd.DataFrame({"data_origin": ["chembl", "uniprot", "fallback", None]})

    ranked = annotate_source_rank(df)

    assert ranked["merge_rank"].tolist() == [0, 1, 4, 5]
