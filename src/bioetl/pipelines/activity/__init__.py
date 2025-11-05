"""Activity ChEMBL pipeline modules."""

from bioetl.pipelines.activity_chembl.join_molecule import join_activity_with_molecule

__all__ = ["join_activity_with_molecule"]
