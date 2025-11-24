from __future__ import annotations

import pandas as pd

from application.pipelines.specs.mixins import FlattenNestedMixin, FlattenSpec


class _StubLogger:
    def __init__(self) -> None:
        self.debug_calls: list[tuple[str, dict[str, object]]] = []

    def debug(self, event: str, **kwargs: object) -> None:
        self.debug_calls.append((event, kwargs))


class _DummyPipeline(FlattenNestedMixin):
    def nested_flatten_specs(self):
        return (
            FlattenSpec("nested", cols=("a", "b"), prefix=""),
            FlattenSpec("meta", prefix="meta_", drop_source=True),
        )


def test_flatten_nested_structures_with_dicts() -> None:
    source = pd.DataFrame(
        {
            "nested": [
                {"a": 1, "b": 2},
                {"a": 3, "b": None},
            ],
            "meta": [
                {"x": "one", "y": "two"},
                {"x": "three", "y": "four"},
            ],
            "other": [10, 20],
        }
    )

    logger = _StubLogger()
    pipeline = _DummyPipeline()

    flattened = pipeline._flatten_nested_structures(source, logger)

    assert "a" in flattened.columns
    assert "b" in flattened.columns
    assert "meta_x" in flattened.columns
    assert "meta_y" in flattened.columns
    assert "meta" not in flattened.columns
    pd.testing.assert_series_equal(flattened["a"], pd.Series([1, 3], name="a"))
    pd.testing.assert_series_equal(flattened["meta_y"], pd.Series(["two", "four"], name="meta_y"))
    assert logger.debug_calls
