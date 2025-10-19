from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from .iuphar_data import IUPHARData


@dataclass
class ClassificationRecord:
    IUPHAR_target_id: str = "N/A"
    IUPHAR_family_id: str = "N/A"
    IUPHAR_class: str = "Other Protein Target"
    IUPHAR_subclass: str = "Other Protein Target"
    IUPHAR_tree: list[str] = field(default_factory=lambda: ["0864-1", "0864"])  # default chain
    IUPHAR_type: str = "Other Protein Target.Other Protein Target"
    IUPHAR_name: str = "N/A"
    STATUS: str = "N/A"


class IUPHARClassifier:
    def __init__(self, data: IUPHARData):
        self.data = data

    @staticmethod
    def _is_valid_parameter(parameter: str | None) -> bool:
        return bool(parameter) and parameter not in {"N/A", "Other Protein Target"}

    @staticmethod
    def _is_valid_list(values: Iterable[str]) -> bool:
        items = [v for v in values if v and v != "N/A"]
        if not items:
            return False
        if items[0] == "0864-1" and len(items) == 2:
            return False
        return True

    def _family_to_type(self, family_id: str) -> str:
        if self._is_valid_parameter(family_id):
            rec = self.data.from_family_record(family_id)
            if rec is not None:
                val = rec.get("type", "")
                # Some dataframes may return a Series for ambiguous access; coerce to string safely
                try:
                    if isinstance(val, str):
                        if val:
                            return val
                    else:
                        text = str(val)
                        if text and text != "Series([], )":
                            return text
                except Exception:
                    pass
        return "N/A"

    def _family_to_chain(self, family_id: str) -> list[str]:
        if self._is_valid_parameter(family_id):
            return self.data.family_chain(family_id)
        return []

    def _target_to_type(self, target_id: str) -> str:
        rec = self.data.from_target_record(target_id)
        if rec is None:
            return "Other Protein Target.Other Protein Target"
        fam_id = str(rec.get("family_id", ""))
        type1 = str(rec.get("type", ""))
        type2 = self._family_to_type(fam_id)
        if self._is_valid_parameter(type1) and not self._is_valid_parameter(type2):
            type_val = type1
        elif self._is_valid_parameter(type2) and not self._is_valid_parameter(type1):
            type_val = type2
        elif not (self._is_valid_parameter(type1) or self._is_valid_parameter(type2)):
            type_val = "Other Protein Target.Other Protein Target"
        elif type1 == type2:
            type_val = type1
        elif self._is_valid_parameter(type1):
            type_val = type1
        else:
            type_val = "N/A.N/A"
        return type_val or "Other Protein Target.Other Protein Target"

    _CHAIN_MAP = {
        "Enzyme.Oxidoreductase": ["0690-1", "0690"],
        "Enzyme.Transferase": ["0690-2", "0690"],
        "Enzyme.Multifunctional": ["0690-3", "0690"],
        "Enzyme.Hydrolase": ["0690-4", "0690"],
        "Enzyme.Isomerase": ["0690-5", "0690"],
        "Enzyme.Lyase": ["0690-6", "0690"],
        "Enzyme.Ligase": ["0690-6", "0690"],
        "Receptor.Catalytic receptor": ["0862", "0688"],
        "Receptor.G protein-coupled receptor": ["0694", "0688"],
        "Receptor.Nuclear hormone receptor": ["0095", "0688"],
        "Transporter.ATP-binding cassette transporter family": ["0136", "0691"],
        "Transporter.F-type and V-type ATPase": ["0137", "0691"],
        "Transporter.P-type ATPase": ["0138", "0691"],
        "Transporter.SLC superfamily of solute carrier": ["0863", "0691"],
        "Ion channel.Ligand-gated ion channel": ["0697", "0689"],
        "Ion channel.Other ion channel": ["0861", "0689"],
        "Ion channel.Voltage-gated ion channel": ["0696", "0689"],
    }

    @staticmethod
    def _name_to_type(name: str) -> str:
        if not name:
            return "Other Protein Target.Other Protein Target"
        text = name.lower()
        if "kinase" in text:
            return "Enzyme.Transferase"
        if "oxidase" in text or "reductase" in text:
            return "Enzyme.Oxidoreductase"
        if "hydrolase" in text or "protease" in text or "phosphatases" in text:
            return "Enzyme.Hydrolase"
        if "atpase" in text:
            return "Transporter.N/A"
        if "solute carrier" in text:
            return "Transporter.SLC superfamily of solute carrier"
        if "transport" in text:
            return "Transporter.N/A"
        if "channel" in text:
            return "Ion channel.N/A"
        if "hormone" in text:
            return "Receptor.Nuclear hormone receptor"
        return "Other Protein Target.Other Protein Target"

    @classmethod
    def _ec_number_to_type(cls, ec_numbers: list[str]) -> str:
        if not ec_numbers:
            return ""
        prefixes = {n.split(".")[0] for n in ec_numbers if "." in n and n.split(".")[0]}
        if not prefixes:
            return ""
        if len(prefixes) > 1:
            return "Enzyme.Multifunctional"
        mapping = {
            "1": "Enzyme.Oxidoreductase",
            "2": "Enzyme.Transferase",
            "3": "Enzyme.Hydrolase",
            "4": "Enzyme.Lyase",
            "5": "Enzyme.Isomerase",
            "6": "Enzyme.Ligase",
            "7": "Enzyme.Translocase",
        }
        return mapping.get(prefixes.pop(), "")

    @classmethod
    def _ec_number_to_chain(cls, ec_numbers: list[str]) -> list[str]:
        t = cls._ec_number_to_type(ec_numbers)
        return cls._CHAIN_MAP.get(t, ["0864-1", "0864"])

    # -------------------- public API --------------------
    def by_target_id(self, target_id: str, optional_name: str | None = None) -> ClassificationRecord:
        if not self._is_valid_parameter(target_id) or "|" in target_id:
            return ClassificationRecord()
        fam_id = self.data.from_target_family_id(target_id)
        type_val = self._target_to_type(target_id)
        parts = type_val.split(".")
        cls_part = parts[0] if parts else "Other Protein Target"
        sub_part = parts[1] if len(parts) > 1 else "Other Protein Target"
        return ClassificationRecord(
            IUPHAR_target_id=target_id,
            IUPHAR_family_id=fam_id if fam_id else "N/A",
            IUPHAR_type=type_val,
            IUPHAR_class=cls_part,
            IUPHAR_subclass=sub_part,
            IUPHAR_tree=(self._family_to_chain(fam_id) if fam_id else ["0864-1", "0864"]),
            IUPHAR_name=optional_name or self.data.from_target_name(target_id) or "N/A",
            STATUS="target_id",
        )

    def by_family_id(self, family_id: str, optional_name: str | None = None) -> ClassificationRecord:
        if not self._is_valid_parameter(family_id) or "|" in family_id:
            return ClassificationRecord()
        type_val = self._family_to_type(family_id)
        parts = type_val.split(".") if type_val else []
        cls_part = parts[0] if parts else "Other Protein Target"
        sub_part = parts[1] if len(parts) > 1 else "Other Protein Target"
        return ClassificationRecord(
            IUPHAR_family_id=family_id,
            IUPHAR_type=type_val or "Other Protein Target.Other Protein Target",
            IUPHAR_class=cls_part,
            IUPHAR_subclass=sub_part,
            IUPHAR_tree=self._family_to_chain(family_id) or ["0864-1", "0864"],
            IUPHAR_name=optional_name or "N/A",
            STATUS="family_id",
        )

    def by_ec_number(self, ec_number: str, optional_name: str | None = None) -> ClassificationRecord:
        numbers = [n.strip() for n in ec_number.split("|") if n.strip()] if ec_number else []
        if not numbers:
            return ClassificationRecord()
        t = self._ec_number_to_type(numbers) or "Other Protein Target.Other Protein Target"
        parts = t.split(".")
        cls_part = parts[0] if parts else "Other Protein Target"
        sub_part = parts[1] if len(parts) > 1 else "Other Protein Target"
        return ClassificationRecord(
            IUPHAR_type=t,
            IUPHAR_class=cls_part,
            IUPHAR_subclass=sub_part,
            IUPHAR_tree=self._ec_number_to_chain(numbers),
            IUPHAR_name=optional_name or "N/A",
            STATUS="ec_number",
        )

    def by_molecular_function(self, function: str) -> ClassificationRecord:
        if not self._is_valid_parameter(function):
            return ClassificationRecord()
        t = self._name_to_type(function)
        parts = t.split(".")
        cls_part = parts[0] if parts else "Other Protein Target"
        sub_part = parts[1] if len(parts) > 1 else "Other Protein Target"
        return ClassificationRecord(
            IUPHAR_type=t,
            IUPHAR_class=cls_part,
            IUPHAR_subclass=sub_part,
            IUPHAR_tree=self._CHAIN_MAP.get(t, ["0864-1", "0864"]),
            IUPHAR_name=function,
            STATUS="molecular_function",
        )


