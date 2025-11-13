"""Unified Pandera column factory for data schemas."""

from __future__ import annotations

from collections.abc import Collection
from typing import Any, ClassVar

import pandas as pd
import pandera as pa
from pandera import Check, Column

from .vocabulary_bindings import VOCAB_METADATA_KEY


class SchemaColumnFactory:
    """Factory for reusable Pandera columns."""

    CHEMBL_ID_PATTERN: ClassVar[str] = r"^CHEMBL\d+$"
    BAO_ID_PATTERN: ClassVar[str] = r"^BAO_\d{7}$"
    DOI_PATTERN: ClassVar[str] = r"^10\.\d{4,9}/\S+$"
    UUID_PATTERN: ClassVar[str] = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"

    @classmethod
    def string(
        cls,
        *,
        nullable: bool = True,
        unique: bool = False,
        pattern: str | None = None,
        isin: Collection[str] | None = None,
        length: tuple[int, int] | None = None,
        vocabulary: str | None = None,
        vocabulary_allowed_statuses: Collection[str] | None = None,
        vocabulary_required: bool = True,
    ) -> Column:
        """Create a string column with optional constraints."""

        if vocabulary is not None and isin is not None:
            message = "isin constraint cannot be combined with vocabulary metadata."
            raise ValueError(message)

        checks: list[Check] = []
        if pattern is not None:
            checks.append(Check.str_matches(pattern))
        if isin is not None:
            checks.append(Check.isin(list(isin)))
        if length is not None:
            checks.append(Check.str_length(length[0], length[1]))

        metadata: dict[str, object] | None = None
        if vocabulary is not None:
            normalized_vocab = vocabulary.strip()
            if not normalized_vocab:
                message = "vocabulary identifier must be a non-empty string."
                raise ValueError(message)
            metadata_payload: dict[str, object] = {"id": normalized_vocab, "required": bool(vocabulary_required)}
            if vocabulary_allowed_statuses:
                metadata_payload["allowed_statuses"] = tuple(
                    str(status) for status in vocabulary_allowed_statuses
                )
            metadata = {VOCAB_METADATA_KEY: metadata_payload}

        dtype: Any = pa.String
        return Column(
            dtype,
            checks=checks or None,
            nullable=nullable,
            unique=unique,
            metadata=metadata,
        )

    @classmethod
    def int64(
        cls,
        *,
        nullable: bool = True,
        ge: int | None = None,
        le: int | None = None,
        isin: Collection[int] | None = None,
        unique: bool = False,
        pandas_nullable: bool = False,
    ) -> Column:
        """Create an Int64 column with optional constraints."""

        checks: list[Check] = []
        if ge is not None:
            checks.append(Check.ge(ge))
        if le is not None:
            checks.append(Check.le(le))
        if isin is not None:
            checks.append(Check.isin(list(isin)))

        dtype: Any = pd.Int64Dtype() if pandas_nullable else pa.Int64
        return Column(
            dtype,
            checks=checks or None,
            nullable=nullable,
            unique=unique,
        )

    @classmethod
    def float64(
        cls,
        *,
        nullable: bool = True,
        ge: float | None = None,
        le: float | None = None,
    ) -> Column:
        """Create a Float64 column with optional boundaries."""

        checks: list[Check] = []
        if ge is not None:
            checks.append(Check.ge(ge))
        if le is not None:
            checks.append(Check.le(le))

        return Column(
            pa.Float64,
            checks=checks or None,
            nullable=nullable,
        )

    @classmethod
    def boolean_flag(cls, *, use_boolean_dtype: bool = True) -> Column:
        """Create a nullable boolean flag column."""

        if use_boolean_dtype:
            return Column(pd.BooleanDtype(), nullable=True)
        return Column(
            pd.Int64Dtype(),
            Check.isin([0, 1]),
            nullable=True,
        )

    @classmethod
    def object(cls, *, nullable: bool = True) -> Column:
        """Create an object column."""

        return Column(pa.Object, nullable=nullable)

    @classmethod
    def row_metadata(cls) -> dict[str, Column]:
        """Return the standard set of row metadata columns."""

        return {
            "row_subtype": cls.string(nullable=False),
            "row_index": cls.int64(nullable=False, ge=0),
        }

    @classmethod
    def chembl_id(cls, *, nullable: bool = True, unique: bool = False) -> Column:
        """Build a column for ChEMBL identifiers."""

        return cls.string(pattern=cls.CHEMBL_ID_PATTERN, nullable=nullable, unique=unique)

    @classmethod
    def bao_id(cls, *, nullable: bool = True) -> Column:
        """Build a column for BAO identifiers."""

        return cls.string(pattern=cls.BAO_ID_PATTERN, nullable=nullable)

    @classmethod
    def doi(cls, *, nullable: bool = True) -> Column:
        """Build a column for DOI values."""

        return cls.string(pattern=cls.DOI_PATTERN, nullable=nullable)

    @classmethod
    def uuid(cls, *, nullable: bool = False, unique: bool = False) -> Column:
        """Build a UUID column in canonical format."""

        return cls.string(pattern=cls.UUID_PATTERN, nullable=nullable, unique=unique)

    @classmethod
    def chembl_molecule_id(cls, *, nullable: bool = True, unique: bool = False) -> Column:
        """Alias for ChEMBL molecule identifier columns."""

        return cls.chembl_id(nullable=nullable, unique=unique)

    @classmethod
    def chembl_assay_id(cls, *, nullable: bool = True, unique: bool = False) -> Column:
        """Alias for ChEMBL assay identifier columns."""

        return cls.chembl_id(nullable=nullable, unique=unique)

    @classmethod
    def chembl_target_id(cls, *, nullable: bool = True, unique: bool = False) -> Column:
        """Alias for ChEMBL target identifier columns."""

        return cls.chembl_id(nullable=nullable, unique=unique)

    @classmethod
    def chembl_document_id(cls, *, nullable: bool = True, unique: bool = False) -> Column:
        """Alias for ChEMBL document identifier columns."""

        return cls.chembl_id(nullable=nullable, unique=unique)

    @classmethod
    def chembl_testitem_id(cls, *, nullable: bool = True, unique: bool = False) -> Column:
        """Alias for ChEMBL test item identifier columns."""

        return cls.chembl_id(nullable=nullable, unique=unique)

    @classmethod
    def bao_term_id(cls, *, nullable: bool = True) -> Column:
        """Alias for BAO term identifier columns."""

        return cls.bao_id(nullable=nullable)

