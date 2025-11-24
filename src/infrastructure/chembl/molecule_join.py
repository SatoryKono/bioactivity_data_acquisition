"""Join helpers for linking activity records to molecule metadata."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

import pandas as pd

from domain.chembl.molecule_joiner import MoleculeJoiner
from infrastructure.chembl.repos.chembl_repositories import (
    ChemblActivityRepository,
    ChemblCompoundRecordRepository,
    ChemblMoleculeRepository,
)
from infrastructure.logging import UnifiedLogger

if TYPE_CHECKING:
    from infrastructure.clients.client_chembl import ChemblClient

__all__ = ["join_activity_with_molecule"]


def join_activity_with_molecule(
    activity_ids: Sequence[str] | pd.DataFrame,
    client: "ChemblClient",
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Join activity records with compound_record and molecule metadata."""

    log = UnifiedLogger.get(__name__).bind(component="activity_molecule_join")

    cfg = cfg or {}
    page_limit_cfg = cfg.get("page_limit", 1000)
    batch_size_cfg = cfg.get("batch_size", 25)

    activity_repo = ChemblActivityRepository(client, batch_size=int(batch_size_cfg) or 25)
    compound_repo = ChemblCompoundRecordRepository(
        client,
        page_limit=int(page_limit_cfg) if page_limit_cfg is not None else None,
        batch_size=int(cfg.get("batch_size", 100)) or 100,
    )
    molecule_repo = ChemblMoleculeRepository(
        client,
        page_limit=int(page_limit_cfg) if page_limit_cfg is not None else None,
    )

    joiner = MoleculeJoiner(activity_repo, compound_repo, molecule_repo)
    result = joiner.join(activity_ids)

    log.info("activity_molecule_join_complete", rows=len(result))
    return result
