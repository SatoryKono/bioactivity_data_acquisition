"""Join helpers for linking activity records to molecule metadata."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

import pandas as pd

from bioetl.chembl.domain.molecule_joiner import MoleculeJoiner
from bioetl.chembl.repos.chembl_repositories import (
    ChemblActivityRepository,
    ChemblCompoundRecordRepository,
    ChemblMoleculeRepository,
)
from bioetl.core.logging import UnifiedLogger

if TYPE_CHECKING:
    from bioetl.clients.client_chembl import ChemblClient

__all__ = ["join_activity_with_molecule"]


def join_activity_with_molecule(
    activity_ids: Sequence[str] | pd.DataFrame,
    client: "ChemblClient",
    cfg: Mapping[str, Any] | None,
) -> pd.DataFrame:
    """Join activity records with compound_record and molecule metadata."""

    log = UnifiedLogger.get(__name__).bind(component="activity_molecule_join")

    # Normalise configuration to a mapping
    if isinstance(cfg, Mapping):
        config_mapping: Mapping[str, Any] = cfg
    else:
        config_mapping = {}

    # Resolve page_limit with a safe int cast and fallback
    page_limit_raw = config_mapping.get("page_limit", 1000)
    if page_limit_raw is None:
        page_limit: int | None = None
    else:
        try:
            page_limit = int(page_limit_raw)
        except (TypeError, ValueError):
            page_limit = 1000

    # Resolve batch sizes separately to preserve previous defaults
    activity_batch_raw = config_mapping.get("batch_size", 25)
    try:
        activity_batch_size = int(activity_batch_raw)
    except (TypeError, ValueError):
        activity_batch_size = 25
    if activity_batch_size <= 0:
        activity_batch_size = 25

    compound_batch_raw = config_mapping.get("batch_size", 100)
    try:
        compound_batch_size = int(compound_batch_raw)
    except (TypeError, ValueError):
        compound_batch_size = 100
    if compound_batch_size <= 0:
        compound_batch_size = 100

    activity_repo = ChemblActivityRepository(
        client,
        batch_size=activity_batch_size,
    )
    compound_repo = ChemblCompoundRecordRepository(
        client,
        page_limit=page_limit,
        batch_size=compound_batch_size,
    )
    molecule_repo = ChemblMoleculeRepository(
        client,
        page_limit=page_limit,
    )

    joiner = MoleculeJoiner(activity_repo, compound_repo, molecule_repo)
    result = joiner.join(activity_ids)

    log.info("activity_molecule_join_complete", rows=len(result))
    return result
