from __future__ import annotations

import pandas as pd

from domain.chembl.molecule_joiner import MoleculeJoiner


class StubActivityRepo:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame

    def fetch_by_ids(self, activity_ids, *, fields):
        return self.frame


class StubCompoundRepo:
    def __init__(self, mapping: dict[str, dict[str, object]]) -> None:
        self.mapping = mapping
        self.requested: list[str] | None = None

    def fetch_by_record_ids(self, record_ids):
        self.requested = list(record_ids)
        return self.mapping


class StubMoleculeRepo:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame
        self.requested: list[str] | None = None

    def fetch_by_ids(self, molecule_ids, *, fields=None, page_limit=None):
        self.requested = list(molecule_ids)
        if fields:
            missing = [col for col in fields if col not in self.frame.columns]
            if missing:
                return pd.DataFrame(columns=fields)
            return self.frame[fields]
        return self.frame


def sample_activity_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "activity_id": [1, 2, 3, 4, 5],
            "record_id": [100, 101, 102, 103, None],
            "molecule_chembl_id": [
                "CHEMBL1",
                "CHEMBL2",
                "CHEMBL3",
                "CHEMBL4",
                "CHEMBL5",
            ],
        }
    )


def test_joiner_adds_expected_columns():
    activity_repo = StubActivityRepo(sample_activity_df())
    compound_repo = StubCompoundRepo(
        {
            "100": {"compound_key": "KEY1", "compound_name": "Compound1"},
            "101": {"compound_key": "KEY2", "compound_name": "Compound2"},
        }
    )
    molecule_repo = StubMoleculeRepo(
        pd.DataFrame(
            [
                {
                    "molecule_chembl_id": "CHEMBL1",
                    "pref_name": "Aspirin",
                    "molecule_synonyms": [],
                },
                {
                    "molecule_chembl_id": "CHEMBL2",
                    "pref_name": "Ibuprofen",
                    "molecule_synonyms": [],
                },
            ]
        )
    )

    joiner = MoleculeJoiner(activity_repo, compound_repo, molecule_repo)
    result = joiner.join(sample_activity_df())

    assert list(result.columns) == [
        "activity_id",
        "molecule_key",
        "molecule_name",
        "compound_key",
        "compound_name",
    ]
    assert compound_repo.requested == ["100", "101", "102", "103"]
    assert molecule_repo.requested == [
        "CHEMBL1",
        "CHEMBL2",
        "CHEMBL3",
        "CHEMBL4",
        "CHEMBL5",
    ]


def test_joiner_pref_name_priority():
    activity_repo = StubActivityRepo(sample_activity_df().iloc[[0]])
    compound_repo = StubCompoundRepo({"100": {"compound_key": "KEY1"}})
    molecule_repo = StubMoleculeRepo(
        pd.DataFrame(
            [
                {
                    "molecule_chembl_id": "CHEMBL1",
                    "pref_name": "Preferred",
                    "molecule_synonyms": ["Synonym1"],
                }
            ]
        )
    )

    joiner = MoleculeJoiner(activity_repo, compound_repo, molecule_repo)
    result = joiner.join(sample_activity_df().iloc[[0]])

    assert result.iloc[0]["molecule_name"] == "Preferred"


def test_joiner_synonym_fallback():
    activity_repo = StubActivityRepo(sample_activity_df().iloc[[1]])
    compound_repo = StubCompoundRepo({})
    molecule_repo = StubMoleculeRepo(
        pd.DataFrame(
            [
                {
                    "molecule_chembl_id": "CHEMBL2",
                    "pref_name": None,
                    "molecule_synonyms": [{"molecule_synonym": "Synonym1"}],
                }
            ]
        )
    )

    joiner = MoleculeJoiner(activity_repo, compound_repo, molecule_repo)
    result = joiner.join(sample_activity_df().iloc[[1]])

    assert result.iloc[0]["molecule_name"] == "Synonym1"


def test_joiner_handles_empty_dataframe():
    empty_frame = pd.DataFrame()
    activity_repo = StubActivityRepo(empty_frame)
    joiner = MoleculeJoiner(activity_repo, StubCompoundRepo({}), StubMoleculeRepo(empty_frame))

    result = joiner.join(empty_frame)
    assert result.empty
    assert list(result.columns) == [
        "activity_id",
        "molecule_key",
        "molecule_name",
        "compound_key",
        "compound_name",
    ]


def test_joiner_missing_columns_returns_empty():
    bad_frame = pd.DataFrame({"activity_id": [1], "record_id": [1]})
    activity_repo = StubActivityRepo(bad_frame)
    joiner = MoleculeJoiner(activity_repo, StubCompoundRepo({}), StubMoleculeRepo(bad_frame))

    result = joiner.join(bad_frame)
    assert result.empty


def test_joiner_missing_molecules_fall_back_to_key():
    subset = sample_activity_df().iloc[[0]]
    activity_repo = StubActivityRepo(subset)
    compound_repo = StubCompoundRepo({"100": {"compound_key": "CK", "compound_name": "CN"}})
    molecule_repo = StubMoleculeRepo(pd.DataFrame())

    joiner = MoleculeJoiner(activity_repo, compound_repo, molecule_repo)
    result = joiner.join(subset)

    assert result.iloc[0]["molecule_name"] == "CHEMBL1"
    assert result.iloc[0]["molecule_key"] == "CHEMBL1"
