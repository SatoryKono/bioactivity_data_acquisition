from __future__ import annotations

import pandas as pd

from bioetl.pipelines.chembl.activity.normalize import (
    enrich_with_assay,
    enrich_with_compound_record,
    enrich_with_data_validity,
)
from bioetl.pipelines.chembl.assay.normalize import (
    enrich_with_assay_classifications,
    enrich_with_assay_parameters,
)
from bioetl.pipelines.chembl.document.normalize import enrich_with_document_terms


class FakeActivityClient:
    def fetch_assays_by_ids(self, ids, fields, page_limit):  # type: ignore[override]
        return {
            "A1": {
                "assay_organism": "Homo sapiens",
                "assay_tax_id": 9606,
            },
        }

    def fetch_compound_records_by_pairs(self, pairs, fields, page_limit):  # type: ignore[override]
        return {
            ("CHEM1", "DOC1"): {
                "compound_name": "Cmp",
                "compound_key": "KEY",
                "curated": True,
            },
        }

    def paginate(self, *args, **kwargs):  # type: ignore[override]
        return iter([])

    def fetch_data_validity_lookup(self, comments, fields, page_limit):  # type: ignore[override]
        return {comment: {"description": f"desc-{comment}"} for comment in comments}


class FakeAssayClient:
    def fetch_assay_class_map_by_assay_ids(self, ids, fields, page_limit):  # type: ignore[override]
        return {assay_id: [{"assay_class_id": "C1"}] for assay_id in ids}

    def fetch_assay_classifications_by_class_ids(self, class_ids, fields, page_limit):  # type: ignore[override]
        return {"C1": {"l1": "Level1", "pref_name": "Class"}}

    def fetch_assay_parameters_by_assay_ids(
        self,
        ids,
        fields,
        page_limit,
        active_only,
    ):  # type: ignore[override]
        return {
            assay_id: [{field: f"value-{field}" for field in fields if field != "assay_chembl_id"}]
            for assay_id in ids
        }  # type: ignore[return-value]


class FakeDocumentClient:
    def fetch_document_terms_by_ids(self, ids, fields, page_limit):  # type: ignore[override]
        return {
            doc_id: [{"document_chembl_id": doc_id, "term": "alpha", "weight": 0.9}]
            for doc_id in ids
        }  # type: ignore[return-value]


def test_activity_assay_enrichment_schema() -> None:
    df = pd.DataFrame({"assay_chembl_id": ["A1"]})
    result = enrich_with_assay(
        df,
        client=FakeActivityClient(),
        cfg={"fields": ["assay_chembl_id", "assay_organism", "assay_tax_id"]},
    )

    assert pd.api.types.is_string_dtype(result["assay_organism"].dtype)
    assert pd.api.types.is_integer_dtype(result["assay_tax_id"].dtype)


def test_activity_compound_record_enrichment_schema() -> None:
    df = pd.DataFrame(
        {
            "molecule_chembl_id": ["CHEM1"],
            "document_chembl_id": ["DOC1"],
            "record_id": ["REC1"],
        },
    )
    result = enrich_with_compound_record(df, client=FakeActivityClient(), cfg={})

    assert pd.api.types.is_string_dtype(result["compound_name"].dtype)
    assert pd.api.types.is_bool_dtype(result["curated"].dtype)


def test_activity_data_validity_enrichment_schema() -> None:
    df = pd.DataFrame({"data_validity_comment": ["OK"]})
    result = enrich_with_data_validity(df, client=FakeActivityClient(), cfg={})

    assert pd.api.types.is_string_dtype(result["data_validity_description"].dtype)


def test_assay_classification_enrichment_schema() -> None:
    df = pd.DataFrame({"assay_chembl_id": ["A1"]})
    result = enrich_with_assay_classifications(df, client=FakeAssayClient(), cfg={})

    assert pd.api.types.is_string_dtype(result["assay_classifications"].dtype)
    assert pd.api.types.is_string_dtype(result["assay_class_id"].dtype)


def test_assay_parameters_enrichment_schema() -> None:
    df = pd.DataFrame({"assay_chembl_id": ["A1"]})
    result = enrich_with_assay_parameters(df, client=FakeAssayClient(), cfg={})

    assert pd.api.types.is_string_dtype(result["assay_parameters"].dtype)


def test_document_terms_enrichment_schema() -> None:
    df = pd.DataFrame({"document_chembl_id": ["DOC1"]})
    result = enrich_with_document_terms(df, client=FakeDocumentClient(), cfg={})

    assert pd.api.types.is_string_dtype(result["term"].dtype)
    assert pd.api.types.is_string_dtype(result["weight"].dtype)
