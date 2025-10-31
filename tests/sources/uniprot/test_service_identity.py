from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from bioetl.sources.uniprot import UniProtNormalizer, UniProtService


def test_uniprot_service_is_alias_of_normalizer() -> None:
    assert UniProtService is UniProtNormalizer


def test_enrich_targets_identity_on_fixture() -> None:
    search_client = MagicMock()
    search_client.request_json.return_value = {"results": []}

    df = pd.DataFrame({"uniprot_accession": ["UNKNOWN"], "gene_symbol": ["ABC1"]})

    svc = UniProtService(search_client=search_client)
    norm = UniProtNormalizer(search_client=search_client)

    r1 = svc.enrich_targets(df)
    r2 = norm.enrich_targets(df)

    pd.testing.assert_frame_equal(r1.dataframe, r2.dataframe)
    pd.testing.assert_frame_equal(r1.silver, r2.silver)
    pd.testing.assert_frame_equal(r1.components, r2.components)
    assert r1.metrics == r2.metrics

