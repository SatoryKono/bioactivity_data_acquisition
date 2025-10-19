from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class IUPHARData:
    """Container for IUPHAR target and family data with lookup helpers."""

    target_df: pd.DataFrame
    family_df: pd.DataFrame
    _target_by_id: pd.DataFrame = field(init=False, repr=False)
    _family_by_id: pd.DataFrame = field(init=False, repr=False)
    _family_by_target: dict[str, pd.Series] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self._target_by_id = (
            self.target_df.set_index("target_id", drop=False)
            if "target_id" in self.target_df.columns
            else self.target_df
        )
        self._family_by_id = (
            self.family_df.set_index("family_id", drop=False)
            if "family_id" in self.family_df.columns
            else self.family_df
        )
        # Build target_id -> family row map using the pipe-delimited field in family table
        self._family_by_target = {}
        if "target_id" in self.family_df.columns:
            col = self.family_df["target_id"].fillna("")
            for idx, raw in col.items():
                if not raw:
                    continue
                for tok in str(raw).split("|"):
                    tok = tok.strip()
                    if tok:
                        self._family_by_target.setdefault(tok, self.family_df.iloc[idx])

    # -------------------- family chain helpers --------------------
    def family_chain(self, start_id: str) -> list[str]:
        chain: list[str] = []
        current = start_id
        visited: set[str] = set()
        while current:
            if current in visited:
                break
            visited.add(current)
            chain.append(current)
            try:
                row = self._family_by_id.loc[current]
            except KeyError:
                break
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            parent = str(row.get("parent_family_id", "") or "")
            if not parent:
                break
            current = parent
        return chain

    def all_id(self, target_id: str) -> str:
        if not target_id:
            return ""
        fam = self.from_target_family_id(target_id)
        if not fam:
            return target_id
        return f"{target_id}#" + ">".join(self.family_chain(fam))

    def all_name(self, target_id: str) -> str:
        if not target_id:
            return ""
        fam = self.from_target_family_id(target_id)
        if not fam:
            return self.from_target_name(target_id)
        names: list[str] = []
        for fam_id in self.family_chain(fam):
            rec = self.from_family_record(fam_id)
            if rec is None:
                continue
            name = rec.get("family_name", "")
            if name and str(name).lower() not in {"enzyme"}:
                names.append(str(name))
        return f"{self.from_target_name(target_id)}#" + ">".join(names)

    # -------------------- mapping helpers --------------------
    def _select_target_ids(self, mask: pd.Series) -> list[str]:
        series = self.target_df.loc[mask, "target_id"].dropna().astype(str)
        return list(series.unique())

    @staticmethod
    def _normalise_uniprot_values(value: Any) -> list[str]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return []
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none", "na"}:
            return []
        return [tok.strip() for tok in text.split("|") if tok and tok.strip()]

    def _resolve_target_id_from_accessions(self, accessions: list[str]) -> str:
        for acc in accessions:
            ids = self._select_target_ids(self.target_df["uniprot_id"].eq(acc))
            if ids:
                return ids[0]
        return ""

    def target_id_by_uniprot(self, uniprot_id: str | list[str]) -> str:
        accs = (
            uniprot_id if isinstance(uniprot_id, list) else self._normalise_uniprot_values(uniprot_id)
        )
        return self._resolve_target_id_from_accessions(accs)

    def target_id_by_hgnc_name(self, hgnc_name: str) -> str:
        if not hgnc_name:
            return ""
        ids = self._select_target_ids(self.target_df["hgnc_name"].eq(hgnc_name))
        return "|".join(ids) if ids else ""

    def target_id_by_hgnc_id(self, hgnc_id: str) -> str:
        if not hgnc_id:
            return ""
        ids = self._select_target_ids(self.target_df["hgnc_id"].eq(hgnc_id))
        return "|".join(ids) if ids else ""

    def target_id_by_gene(self, gene_name: str) -> str:
        if not gene_name:
            return ""
        ids = self._select_target_ids(self.target_df["gene_name"].eq(gene_name))
        return "|".join(ids) if ids else ""

    def target_id_by_name(self, target_name: str) -> str:
        if not target_name:
            return ""
        mask = (
            self.target_df["synonyms"].fillna("").str.contains(target_name, case=False, na=False, regex=False)
        )
        ids = self._select_target_ids(mask)
        return "|".join(ids) if ids else ""

    def from_target_record(self, target_id: str) -> pd.Series | None:
        if not target_id:
            return None
        try:
            row = self._target_by_id.loc[target_id]
        except KeyError:
            return None
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        return row

    def from_target_family_id(self, target_id: str) -> str:
        rec = self.from_target_record(target_id)
        if rec is None:
            return ""
        value = rec.get("family_id", "")
        return str(value) if pd.notna(value) else ""

    def from_target_name(self, target_id: str) -> str:
        rec = self.from_target_record(target_id)
        if rec is None:
            return ""
        return str(rec.get("target_name", ""))

    def from_family_record(self, family_id: str) -> pd.Series | None:
        if not family_id:
            return None
        try:
            row = self._family_by_id.loc[family_id]
        except KeyError:
            return None
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        return row


