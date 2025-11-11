"""Generic helpers for specification-driven ChEMBL entity pipelines."""

from __future__ import annotations

from typing import ClassVar

import pandas as pd

from ..chembl_base import ChemblExtractionDescriptor, ChemblPipelineBase
from ..specs import ChemblEntitySpec


class GenericChemblEntityPipeline(ChemblPipelineBase):
    """Pipeline base building extraction descriptors from entity specifications."""

    entity_spec: ClassVar[ChemblEntitySpec]

    @classmethod
    def get_entity_spec(cls) -> ChemblEntitySpec:
        """Return the entity specification associated with the pipeline class."""

        spec = getattr(cls, "entity_spec", None)
        if spec is None:
            msg = f"{cls.__name__} must define a non-None 'entity_spec' attribute"
            raise AttributeError(msg)
        return spec

    def build_descriptor(self) -> ChemblExtractionDescriptor:
        """Materialise a :class:`ChemblExtractionDescriptor` from the spec."""

        return self.get_entity_spec().to_descriptor()

    def extract_all(self) -> pd.DataFrame:
        """Extract all entities using the specification-backed descriptor."""

        descriptor = self.build_descriptor()
        return self.run_extract_all(descriptor)

    @classmethod
    def required_enrichers(cls) -> tuple[str, ...]:
        """Return the enrichers required by the pipeline specification."""

        spec = cls.get_entity_spec()
        return tuple(spec.required_enrichers)
