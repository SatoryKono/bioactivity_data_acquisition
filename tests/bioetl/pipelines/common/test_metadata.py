from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from bioetl.pipelines.common.metadata import normalise_metadata_value


@pytest.mark.unit
class TestNormaliseMetadataValue:
    """Unit tests for the metadata normalisation helper."""

    def test_nested_structures_sorted_and_normalised(self) -> None:
        payload = {
            "beta": {
                "gamma": 5,
                "delta": datetime(2024, 1, 1, 8, 30, tzinfo=timezone.utc),
            },
            2: [datetime(2024, 2, 2, 3, 4, 5), "preserve-me"],
            "alpha": (
                "text",
                datetime(2024, 3, 3, 12, 0, tzinfo=timezone(timedelta(hours=2))),
            ),
        }

        result = normalise_metadata_value(payload)

        assert list(result.keys()) == ["2", "alpha", "beta"]
        assert result["2"][0] == "2024-02-02T03:04:05Z"
        assert result["alpha"] == ["text", "2024-03-03T10:00:00Z"]
        assert result["beta"] == {
            "delta": "2024-01-01T08:30:00Z",
            "gamma": 5,
        }

    def test_naive_datetime_normalised_to_utc_string(self) -> None:
        naive = datetime(2024, 6, 1, 12, 45, 30)

        result = normalise_metadata_value({"timestamp": naive})

        assert result == {"timestamp": "2024-06-01T12:45:30Z"}

    def test_non_iterables_preserved(self) -> None:
        payload = {
            "string": "value",
            "bytes": b"binary",
            "integer": 42,
            "float": 3.14,
            "none": None,
        }

        result = normalise_metadata_value(payload)

        assert result == {
            "bytes": b"binary",
            "float": 3.14,
            "integer": 42,
            "none": None,
            "string": "value",
        }
