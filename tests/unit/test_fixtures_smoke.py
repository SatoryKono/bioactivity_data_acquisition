"""Smoke tests ensuring shared fixtures are importable and usable."""

from __future__ import annotations

from tests import conftest


def test_shared_fixtures_provide_expected_objects(
    temp_dir,
    sample_chembl_data,
    sample_activity_data,
    sample_document_data,
):
    """Verify the foundational fixtures instantiate without raising errors."""

    assert temp_dir.exists(), "Temporary directory fixture should provide a valid path"
    assert not sample_chembl_data.empty, "Sample ChEMBL dataset should not be empty"
    assert not sample_activity_data.empty, "Sample activity dataset should not be empty"
    assert not sample_document_data.empty, "Sample document dataset should not be empty"


def test_faker_fixture_is_configured() -> None:
    """Ensure the module-level Faker instance is available for tests."""

    assert isinstance(conftest.fake, conftest.Faker)
    generated_name = conftest.fake.name()
    assert isinstance(generated_name, str)
    assert generated_name

