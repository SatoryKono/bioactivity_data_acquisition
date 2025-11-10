from __future__ import annotations

import pytest

from bioetl.pipelines.common.select_fields import (
    SelectFieldsMixin,
    normalize_select_fields,
)


@pytest.mark.unit
class TestNormalizeSelectFields:
    """Unit tests for the ``normalize_select_fields`` helper."""

    def test_existing_fields_preserve_order_and_add_required(self) -> None:
        result = normalize_select_fields(
            ("field_a", "field_b", "field_a"),
            required=("field_b", "field_c"),
        )

        assert result == ("field_a", "field_b", "field_c")

    def test_default_fields_used_when_missing(self) -> None:
        result = normalize_select_fields(
            None,
            default=("field_x", "field_y"),
            required=("field_z",),
        )

        assert result == ("field_x", "field_y", "field_z")

    def test_preserve_none_returns_none(self) -> None:
        result = normalize_select_fields(
            None,
            required=("field_a",),
            preserve_none=True,
        )

        assert result is None

    def test_empty_tuple_when_no_values(self) -> None:
        assert normalize_select_fields(None) == ()


@pytest.mark.unit
class TestSelectFieldsMixin:
    """Unit tests covering the mixin wrapper."""

    def test_mixin_delegates_to_helper(self) -> None:
        class DummyMixinUser(SelectFieldsMixin):
            def __init__(self) -> None:
                super().__init__()

        dummy = DummyMixinUser()

        result = dummy.normalize_select_fields(
            ("base",),
            required=("extra",),
        )

        assert result == ("base", "extra")

    def test_mixin_preserve_none(self) -> None:
        class DummyMixinUser(SelectFieldsMixin):
            def __init__(self) -> None:
                super().__init__()

        dummy = DummyMixinUser()

        assert dummy.normalize_select_fields(None, preserve_none=True) is None
