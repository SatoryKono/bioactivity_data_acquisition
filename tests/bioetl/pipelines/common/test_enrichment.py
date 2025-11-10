"""Unit tests for enrichment rule helper behaviour."""

from __future__ import annotations

from typing import Any, Mapping

from unittest.mock import MagicMock

from structlog.stdlib import BoundLogger

from bioetl.pipelines.common.enrichment import FunctionEnrichmentRule


def _stub_enrichment(
    df: Any,
    client: Any,
    config: Mapping[str, Any],
) -> Any:
    return df


def test_handle_missing_config_logs_default() -> None:
    rule = FunctionEnrichmentRule(
        name="sample-rule",
        config_path=("section", "rule"),
        function=_stub_enrichment,
        requires_client=False,
    )
    logger = MagicMock(spec=BoundLogger)

    rule.handle_missing_config(logger)

    logger.debug.assert_called_once_with(  # type: ignore[attr-defined]
        "enrichment_rule_skipped_missing_config",
        rule="sample-rule",
        config_path=("section", "rule"),
    )


def test_handle_disabled_prefers_hook() -> None:
    hook = MagicMock()
    rule = FunctionEnrichmentRule(
        name="sample-rule",
        config_path=("section", "rule"),
        function=_stub_enrichment,
        requires_client=False,
        on_disabled=hook,
    )
    logger = MagicMock(spec=BoundLogger)

    rule.handle_disabled(logger)

    hook.assert_called_once_with(logger)
    logger.debug.assert_not_called()  # type: ignore[attr-defined]

