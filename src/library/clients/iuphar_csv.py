from __future__ import annotations

import pandas as pd


_EXPECTED_TARGET_COLUMNS_CANONICAL: tuple[str, ...] = (
    "target_id",
    "uniprot_id",
    "hgnc_name",
    "hgnc_id",
    "gene_name",
    "synonyms",
    "family_id",
    "target_name",
    "type",
)


_EXPECTED_FAMILY_COLUMNS_CANONICAL: tuple[str, ...] = (
    "family_id",
    "family_name",
    "parent_family_id",
    "target_id",
    "type",
)


def _normalise_target_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise columns of _IUPHAR_target.csv to a canonical set.

    The bundled dictionaries may use mixed casing. This helper renames
    well-known columns to the canonical form expected by the integration layer.
    """

    rename_map = {
        # UniProt accession
        "swissprot": "uniprot_id",
        "SWISSPROT": "uniprot_id",
        # HGNC
        "HGNC_NAME": "hgnc_name",
        "HGNC_name": "hgnc_name",
        "HGNC_ID": "hgnc_id",
        "HGNC_id": "hgnc_id",
        # Names
        "name": "target_name",
        "Type_name": "type",
        "Type": "type",
    }
    df = df.rename(columns=rename_map)
    # Coalesce duplicate columns created by renaming (e.g., 'Type' and 'Type_name' -> 'type')
    if df.columns.duplicated().any():
        for name in list({c for c in df.columns[df.columns.duplicated()] } | {c for c in df.columns if (df.columns == c).sum() > 1}):
            # Select all columns with this duplicate name
            cols = [i for i, c in enumerate(df.columns) if c == name]
            if len(cols) <= 1:
                continue
            group = df.iloc[:, cols]
            # Row-wise choose first non-empty string
            merged = group.apply(lambda row: next((str(x) for x in row if str(x).strip()), ""), axis=1)
            # Drop all duplicates and keep one
            keep_idx = cols[0]
            df.iloc[:, keep_idx] = merged
            # Build mask of columns to drop (all but keep_idx)
            drop_cols = [df.columns[i] for i in cols[1:]]
            # Drop by position-safe: use boolean mask is tricky with duplicates; use .drop with level=0 keeps first
            # Here we drop by index name and keep one; to be safe, reassign by building new columns
            keep_cols = [j for j in range(df.shape[1]) if j not in cols[1:]]
            df = df.iloc[:, keep_cols]
    # Ensure all expected columns exist
    for col in _EXPECTED_TARGET_COLUMNS_CANONICAL:
        if col not in df.columns:
            df[col] = ""
    return df


def _normalise_family_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise columns of _IUPHAR_family.csv to a canonical set."""

    rename_map = {
        "family_name_backup": "family_name",
        "Type_name": "type",
        "Type": "type",
    }
    df = df.rename(columns=rename_map)
    # Coalesce duplicate columns after renaming
    if df.columns.duplicated().any():
        for name in list({c for c in df.columns[df.columns.duplicated()] } | {c for c in df.columns if (df.columns == c).sum() > 1}):
            cols = [i for i, c in enumerate(df.columns) if c == name]
            if len(cols) <= 1:
                continue
            group = df.iloc[:, cols]
            merged = group.apply(lambda row: next((str(x) for x in row if str(x).strip()), ""), axis=1)
            keep_idx = cols[0]
            df.iloc[:, keep_idx] = merged
            keep_cols = [j for j in range(df.shape[1]) if j not in cols[1:]]
            df = df.iloc[:, keep_cols]
    for col in _EXPECTED_FAMILY_COLUMNS_CANONICAL:
        if col not in df.columns:
            df[col] = ""
    return df


def load_targets(path: str) -> pd.DataFrame:
    """Load and normalise the IUPHAR target dictionary CSV.

    Parameters
    ----------
    path: str
        Path to ``_IUPHAR_target.csv``
    """

    df = pd.read_csv(path, dtype=str).fillna("")
    df = _normalise_target_columns(df)
    missing = [c for c in _EXPECTED_TARGET_COLUMNS_CANONICAL if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in IUPHAR target dictionary: {', '.join(missing)}")
    return df


def load_families(path: str) -> pd.DataFrame:
    """Load and normalise the IUPHAR family dictionary CSV.

    Parameters
    ----------
    path: str
        Path to ``_IUPHAR_family.csv``
    """

    df = pd.read_csv(path, dtype=str).fillna("")
    df = _normalise_family_columns(df)
    missing = [c for c in _EXPECTED_FAMILY_COLUMNS_CANONICAL if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in IUPHAR family dictionary: {', '.join(missing)}")
    return df


