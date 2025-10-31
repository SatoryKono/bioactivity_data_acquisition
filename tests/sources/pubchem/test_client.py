"""PubChem client tests."""

from __future__ import annotations

from unittest.mock import patch

from tests.sources.pubchem import PubChemAdapterTestCase


class TestPubChemClient(PubChemAdapterTestCase):
    """Validate PubChem client batching and fetch helpers."""

    def test_fetch_by_ids_delegates_to_batch_helper(self) -> None:
        """The custom fetcher still leverages the shared batching helper."""

        adapter = self.ADAPTER_CLASS(
            self.api_config,
            self.make_adapter_config(batch_size=25),
        )
        identifiers = ["AAA", "BBB"]

        with (
            patch.object(adapter, "_fetch_batch", return_value=[{"CID": 1}]) as batch_mock,
            patch.object(adapter, "_fetch_in_batches", wraps=adapter._fetch_in_batches) as helper_mock,
        ):
            result = adapter.fetch_by_ids(identifiers)

        helper_mock.assert_called_once_with(
            identifiers,
            batch_size=25,
            log_event="pubchem_batch_fetch_failed",
        )
        batch_mock.assert_called_once()
        self.assertEqual(result, [{"CID": 1}])

    def test_fetch_by_ids_uses_non_recursive_batch_impl(self) -> None:
        """Ensure ``_fetch_batch`` does not recurse back into ``_fetch_in_batches``."""

        adapter = self.adapter
        identifiers = ["AAA", "BBB"]
        resolution = {
            "AAA": {"cid": 123, "cid_source": "inchikey", "attempt": 1, "fallback_used": False},
            "BBB": {"cid": None, "cid_source": "failed", "attempt": 2, "fallback_used": True},
        }
        property_records = [{"CID": 123, "Some": "value"}]

        with (
            patch.object(adapter, "_resolve_cids_batch", return_value=resolution) as resolve_mock,
            patch.object(adapter, "_fetch_properties_batch", return_value=property_records) as properties_mock,
            patch.object(adapter, "_fetch_in_batches", wraps=adapter._fetch_in_batches) as helper_mock,
        ):
            results = adapter.fetch_by_ids(identifiers)

        helper_mock.assert_called_once()
        resolve_mock.assert_called_once_with(identifiers)
        properties_mock.assert_called_once_with([123])
        self.assertEqual(len(results), len(identifiers))
        self.assertEqual(results[0]["CID"], 123)
        self.assertEqual(results[1]["_source_identifier"], "BBB")
