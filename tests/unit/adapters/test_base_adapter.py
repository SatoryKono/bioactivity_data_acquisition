"""Unit tests for the ExternalAdapter batching helper."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from bioetl.adapters.base import ExternalAdapter
from tests.unit.adapters._mixins import AdapterTestMixin


class DummyAdapter(ExternalAdapter):
    """Minimal adapter used to exercise :meth:`_fetch_in_batches`."""

    DEFAULT_BATCH_SIZE = 3

    def normalize_record(self, record: dict[str, object]) -> dict[str, object]:
        return record

    def _fetch_batch(self, ids: list[str]) -> list[dict[str, object]]:
        return [{"id": identifier} for identifier in ids]


class TestExternalAdapterHelper(AdapterTestMixin, unittest.TestCase):
    """Tests for :meth:`ExternalAdapter._fetch_in_batches`."""

    ADAPTER_CLASS = DummyAdapter
    API_CONFIG_OVERRIDES = {"name": "dummy"}
    ADAPTER_CONFIG_OVERRIDES = {"batch_size": 2}

    def test_fetch_in_batches_empty_ids(self) -> None:
        """Helper returns empty list and skips batch calls for empty input."""

        with patch.object(self.adapter, "_fetch_batch", autospec=True) as batch_mock:
            result = self.adapter._fetch_in_batches([])

        self.assertEqual(result, [])
        batch_mock.assert_not_called()

    def test_fetch_in_batches_logs_and_continues_on_error(self) -> None:
        """Helper preserves logging semantics when batch fetching fails."""

        with patch.object(
            self.adapter,
            "_fetch_batch",
            side_effect=RuntimeError("boom"),
            autospec=True,
        ) as batch_mock, patch.object(self.adapter.logger, "error") as error_log:
            result = self.adapter._fetch_in_batches(["id1", "id2"], batch_size=1)

        self.assertEqual(result, [])
        self.assertEqual(batch_mock.call_count, 2)
        error_log.assert_called()
        last_call = error_log.call_args
        self.assertIn("boom", last_call.kwargs.get("error", ""))
        self.assertEqual(last_call.kwargs.get("batch"), 1)
        self.assertEqual(last_call.kwargs.get("batch_index"), 1)

    def test_fetch_by_ids_uses_class_default_when_config_disabled(self) -> None:
        """``DEFAULT_BATCH_SIZE`` is respected when config batch size is falsy."""

        adapter = self.ADAPTER_CLASS(
            self.api_config,
            self.make_adapter_config(batch_size=0),
        )

        identifiers = [f"id{i}" for i in range(7)]
        with patch.object(adapter, "_fetch_batch", return_value=[], autospec=True) as batch_mock:
            adapter.fetch_by_ids(identifiers)

        self.assertEqual(batch_mock.call_count, 3)
        batch_sizes = [len(call.args[1]) for call in batch_mock.call_args_list]
        self.assertEqual(batch_sizes[:-1], [adapter.DEFAULT_BATCH_SIZE] * (len(batch_sizes) - 1))
        remainder = len(identifiers) % adapter.DEFAULT_BATCH_SIZE
        expected_last = remainder or adapter.DEFAULT_BATCH_SIZE
        self.assertEqual(batch_sizes[-1], expected_last)

