"""Tests for the PubChem API client wrapper."""

from __future__ import annotations

from bioetl.config.loader import load_config
from bioetl.sources.pubchem.client.pubchem_client import PubChemClient
from tests.unit.sources.pubchem import StubUnifiedAPIClient


def test_resolve_cids_batch_uppercases_inchikeys() -> None:
    stub = StubUnifiedAPIClient(
        responses={"/compound/inchikey/ABCDEF/cids/JSON": {"IdentifierList": {"CID": [42]}}}
    )
    client = PubChemClient(stub)

    results = client.resolve_cids_batch(["abcdef"])

    assert results["ABCDEF"]["cid"] == 42
    assert stub.requests == ["/compound/inchikey/ABCDEF/cids/JSON"]


def test_fetch_properties_batch_returns_records() -> None:
    payload = {
        "PropertyTable": {
            "Properties": [
                {"CID": 1, "CanonicalSMILES": "C"},
                {"CID": 2, "CanonicalSMILES": "CC"},
            ]
        }
    }
    stub = StubUnifiedAPIClient(
        responses={
            "/compound/cid/1,2/property/MolecularFormula,MolecularWeight,CanonicalSMILES,IsomericSMILES,InChI,InChIKey,IUPACName/JSON": payload
        }
    )
    client = PubChemClient(stub)

    records = client.fetch_properties_batch([1, 2])

    assert len(records) == 2


def test_enrich_batch_combines_resolution_and_properties() -> None:
    stub = StubUnifiedAPIClient(
        responses={
            "/compound/inchikey/KEYONE/cids/JSON": {"IdentifierList": {"CID": [111]}},
            "/compound/cid/111/property/MolecularFormula,MolecularWeight,CanonicalSMILES,IsomericSMILES,InChI,InChIKey,IUPACName/JSON": {
                "PropertyTable": {
                    "Properties": [
                        {
                            "CID": 111,
                            "CanonicalSMILES": "CC",
                            "IsomericSMILES": "C[C@H]H",
                        }
                    ]
                }
            },
            "/compound/cid/111/synonyms/JSON": {
                "InformationList": {
                    "Information": [
                        {
                            "CID": 111,
                            "Synonym": ["ethane"],
                        }
                    ]
                }
            },
            "/compound/cid/111/xrefs/RegistryID,RN/JSON": {
                "InformationList": {
                    "Information": [
                        {
                            "CID": 111,
                            "RN": ["64-17-5"],
                            "RegistryID": ["64-17-5"],
                        }
                    ]
                }
            },
        }
    )
    client = PubChemClient(stub, batch_size=10)

    records = client.enrich_batch(["keyone"])

    assert records[0]["CID"] == 111
    assert records[0]["CanonicalSMILES"] == "CC"
    assert records[0]["RegistryID"] == "64-17-5"
    assert records[0]["RN"] == "64-17-5"
    assert records[0]["Synonym"] == ["ethane"]
    assert records[0]["_source_identifier"] == "KEYONE"


def test_from_config_respects_disabled_source(monkeypatch) -> None:
    config = load_config("configs/pipelines/pubchem.yaml").model_copy(deep=True)
    config.sources["pubchem"].enabled = False

    client, api_client = PubChemClient.from_config(config)

    assert client is None
    assert api_client is None
