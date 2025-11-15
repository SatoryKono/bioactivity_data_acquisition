"""Tests for source configuration mixins and behaviors."""

from __future__ import annotations

import pytest

from bioetl.config.common import BatchSizeLimitMixin
from bioetl.config.document import DocumentSourceConfig
from bioetl.config.target import TargetSourceConfig


@pytest.mark.unit
@pytest.mark.parametrize(
    "config_cls",
    (DocumentSourceConfig, TargetSourceConfig),
)
def test_batch_size_limited_configs_apply_caps(config_cls: type[BatchSizeLimitMixin]) -> None:
    config = config_cls(batch_size=100)

    assert config.batch_size == config_cls.default_batch_size


@pytest.mark.unit
@pytest.mark.parametrize(
    "config_cls",
    (DocumentSourceConfig, TargetSourceConfig),
)
def test_batch_size_limited_configs_preserve_values_within_cap(
    config_cls: type[BatchSizeLimitMixin],
) -> None:
    config = config_cls(batch_size=10)

    assert config.batch_size == 10


@pytest.mark.unit
@pytest.mark.parametrize(
    "config_cls",
    (DocumentSourceConfig, TargetSourceConfig),
)
def test_batch_size_limited_configs_default_when_none(
    config_cls: type[BatchSizeLimitMixin],
) -> None:
    config = config_cls(batch_size=None)

    assert config.batch_size == config_cls.default_batch_size
