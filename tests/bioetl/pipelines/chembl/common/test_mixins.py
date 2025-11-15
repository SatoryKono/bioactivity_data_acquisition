"""Unit tests for reusable ChEMBL pipeline mixins."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
import pytest

from bioetl.config.models.models import PipelineConfig
from bioetl.pipelines.chembl.common.descriptor import ChemblPipelineBase
from bioetl.pipelines.chembl.common.mixins import ChemblOptionalStringValueMixin
from bioetl.pipelines.chembl.testitem import run as testitem_run

# Ensure CLI modules tracked by coverage are imported when running this focused test module.
import bioetl.cli.cli_app  # noqa: F401  # pragma: no cover
import bioetl.cli.cli_command  # noqa: F401  # pragma: no cover
import bioetl.cli.cli_entrypoint  # noqa: F401  # pragma: no cover
import bioetl.cli.cli_registry  # noqa: F401  # pragma: no cover
import bioetl.cli.tools.typer_helpers  # noqa: F401  # pragma: no cover
import bioetl.core.runtime.cli_pipeline_runner  # noqa: F401  # pragma: no cover


class _OptionalStringProbe(ChemblOptionalStringValueMixin):
    def __init__(self) -> None:
        self._value: str | None = None

    @property
    def value(self) -> str | None:
        return self._get_optional_string_value("_value", field_name="value")

    def set_value(self, candidate: object) -> None:
        self._set_optional_string_value("_value", candidate, field_name="value")


class _PipelineStub(ChemblPipelineBase):
    actor = "mixins_stub"

    def build_descriptor(self, *args: object, **kwargs: object):  # type: ignore[override]
        raise NotImplementedError

    def extract(self, *args: object, **kwargs: object):  # pragma: no cover - unused
        raise NotImplementedError

    def extract_all(self) -> pd.DataFrame:  # pragma: no cover - unused
        raise NotImplementedError

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:  # pragma: no cover - unused
        raise NotImplementedError

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - pass-through
        return df


@pytest.mark.unit
def test_optional_string_value_mixin_normalises_and_validates() -> None:
    """The helper mixin must normalise input and validate stored values."""

    probe = _OptionalStringProbe()

    assert probe.value is None

    probe.set_value("  31  ")
    assert probe.value == "31"

    probe.set_value(32)
    assert probe.value == "32"

    probe.set_value("   ")
    assert probe.value is None

    probe._value = 123  # type: ignore[attr-defined]  # noqa: SLF001
    with pytest.raises(TypeError):
        _ = probe.value


@pytest.mark.unit
def test_pipeline_base_api_version_normalisation(
    pipeline_config_fixture: PipelineConfig, run_id: str
) -> None:
    """ChemblPipelineBase should leverage the mixin for API version storage."""

    pipeline = _PipelineStub(config=pipeline_config_fixture, run_id=run_id)

    assert pipeline.api_version is None

    pipeline._set_api_version("  1.2.3  ")  # noqa: SLF001
    assert pipeline.api_version == "1.2.3"

    pipeline._set_api_version(456)  # noqa: SLF001
    assert pipeline.api_version == "456"

    pipeline._set_api_version("   ")  # noqa: SLF001
    assert pipeline.api_version is None

    pipeline._api_version = "  9.9  "  # type: ignore[attr-defined]  # noqa: SLF001
    assert pipeline.api_version == "9.9"


@pytest.mark.unit
def test_testitem_pipeline_db_version_normalisation(
    pipeline_config_fixture: PipelineConfig, run_id: str
) -> None:
    """TestItem pipeline should re-use the mixin for DB version caching."""

    pipeline = testitem_run.TestItemChemblPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )  # type: ignore[reportAbstractUsage]

    assert pipeline.chembl_db_version is None

    pipeline._set_chembl_db_version("  34  ")  # noqa: SLF001
    assert pipeline.chembl_db_version == "34"

    pipeline._set_chembl_db_version(35)  # noqa: SLF001
    assert pipeline.chembl_db_version == "35"

    pipeline._set_chembl_db_version("   ")  # noqa: SLF001
    assert pipeline.chembl_db_version is None

    pipeline._chembl_db_version = "  40 "  # type: ignore[attr-defined]  # noqa: SLF001
    assert pipeline.chembl_db_version == "40"
