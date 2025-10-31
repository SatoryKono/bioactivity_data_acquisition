"""Parser utilities for TestItem molecule payloads."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from bioetl.normalizers import registry
from bioetl.utils.json import canonical_json

__all__ = ["TestItemParser"]


class TestItemParser:
    """Parse and normalize molecule payloads returned by the ChEMBL API."""

    def __init__(
        self,
        *,
        expected_columns: Sequence[str],
        property_fields: Sequence[str],
        structure_fields: Sequence[str],
        json_fields: Sequence[str],
        text_fields: Sequence[str],
        fallback_fields: Sequence[str],
    ) -> None:
        self._expected_columns = list(expected_columns)
        self._property_fields = list(property_fields)
        self._structure_fields = list(structure_fields)
        self._json_fields = list(json_fields)
        self._text_fields = list(text_fields)
        self._fallback_fields = list(fallback_fields)

    def empty_record(self) -> dict[str, Any]:
        """Return a template record with all expected columns."""

        return dict.fromkeys(self._expected_columns)

    def parse(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse a ChEMBL payload into a normalized record."""

        record = self.empty_record()

        record["molecule_chembl_id"] = payload.get("molecule_chembl_id")
        record["molregno"] = payload.get("molregno")

        pref_name = payload.get("pref_name")
        record["pref_name"] = pref_name
        record["pref_name_key"] = self._normalize_pref_name(pref_name)

        self._copy_core_fields(payload, record)
        record.update(self._normalize_properties(payload))
        record.update(self._normalize_structures(payload))
        record.update(self._normalize_synonyms(payload))
        record.update(self._flatten_json_fields(payload))

        for field in self._fallback_fields:
            record[field] = None

        return record

    def _copy_core_fields(self, payload: dict[str, Any], record: dict[str, Any]) -> None:
        """Populate simple scalar fields from the payload."""

        field_map = {
            "parent_chembl_id": "parent_chembl_id",
            "parent_molregno": "parent_molregno",
            "therapeutic_flag": "therapeutic_flag",
            "structure_type": "structure_type",
            "molecule_type": "molecule_type",
            "molecule_type_chembl": "molecule_type",
            "max_phase": "max_phase",
            "first_approval": "first_approval",
            "dosed_ingredient": "dosed_ingredient",
            "availability_type": "availability_type",
            "chirality": "chirality",
            "chirality_chembl": "chirality",
            "mechanism_of_action": "mechanism_of_action",
            "direct_interaction": "direct_interaction",
            "molecular_mechanism": "molecular_mechanism",
            "oral": "oral",
            "parenteral": "parenteral",
            "topical": "topical",
            "black_box_warning": "black_box_warning",
            "natural_product": "natural_product",
            "first_in_class": "first_in_class",
            "prodrug": "prodrug",
            "inorganic_flag": "inorganic_flag",
            "polymer_flag": "polymer_flag",
            "usan_year": "usan_year",
            "usan_stem": "usan_stem",
            "usan_substem": "usan_substem",
            "usan_stem_definition": "usan_stem_definition",
            "indication_class": "indication_class",
            "withdrawn_flag": "withdrawn_flag",
            "withdrawn_year": "withdrawn_year",
            "withdrawn_country": "withdrawn_country",
            "withdrawn_reason": "withdrawn_reason",
            "drug_chembl_id": "drug_chembl_id",
            "drug_name": "drug_name",
            "drug_type": "drug_type",
            "drug_substance_flag": "drug_substance_flag",
            "drug_indication_flag": "drug_indication_flag",
            "drug_antibacterial_flag": "drug_antibacterial_flag",
            "drug_antiviral_flag": "drug_antiviral_flag",
            "drug_antifungal_flag": "drug_antifungal_flag",
            "drug_antiparasitic_flag": "drug_antiparasitic_flag",
            "drug_antineoplastic_flag": "drug_antineoplastic_flag",
            "drug_immunosuppressant_flag": "drug_immunosuppressant_flag",
            "drug_antiinflammatory_flag": "drug_antiinflammatory_flag",
        }

        for target_field, source_field in field_map.items():
            record[target_field] = payload.get(source_field)

        record.update(self._flatten_molecule_hierarchy(payload))

    @staticmethod
    def _normalize_pref_name(pref_name: Any) -> str | None:
        normalized = registry.normalize("chemistry.string", pref_name)
        if normalized is None:
            return None
        lowered = normalized.lower()
        return lowered or None

    def _flatten_molecule_hierarchy(self, molecule: dict[str, Any]) -> dict[str, Any]:
        flattened: dict[str, Any] = {
            "parent_chembl_id": molecule.get("parent_chembl_id"),
            "parent_molregno": molecule.get("parent_molregno"),
            "molecule_hierarchy": None,
        }

        hierarchy = molecule.get("molecule_hierarchy")
        if isinstance(hierarchy, dict) and hierarchy:
            flattened["parent_chembl_id"] = hierarchy.get("parent_chembl_id")
            flattened["parent_molregno"] = hierarchy.get("parent_molregno")
            flattened["molecule_hierarchy"] = canonical_json(hierarchy)

        return flattened

    def _normalize_properties(self, molecule: dict[str, Any]) -> dict[str, Any]:
        flattened = dict.fromkeys(self._property_fields)
        flattened["molecule_properties"] = None

        props = molecule.get("molecule_properties")
        if not isinstance(props, dict) or not props:
            return flattened

        mapping = {
            "mw_freebase": "mw_freebase",
            "alogp": "alogp",
            "hba": "hba",
            "hbd": "hbd",
            "psa": "psa",
            "rtb": "rtb",
            "ro3_pass": "ro3_pass",
            "num_ro5_violations": "num_ro5_violations",
            "acd_most_apka": "acd_most_apka",
            "acd_most_bpka": "acd_most_bpka",
            "acd_logp": "acd_logp",
            "acd_logd": "acd_logd",
            "molecular_species": "molecular_species",
            "full_mwt": "full_mwt",
            "aromatic_rings": "aromatic_rings",
            "heavy_atoms": "heavy_atoms",
            "qed_weighted": "qed_weighted",
            "mw_monoisotopic": "mw_monoisotopic",
            "full_molformula": "full_molformula",
            "hba_lipinski": "hba_lipinski",
            "hbd_lipinski": "hbd_lipinski",
            "num_lipinski_ro5_violations": "num_lipinski_ro5_violations",
            "lipinski_ro5_violations": "num_lipinski_ro5_violations",
            "lipinski_ro5_pass": "lipinski_ro5_pass",
        }

        for field, source in mapping.items():
            flattened[field] = props.get(source)

        if flattened["rtb"] is None and "num_rotatable_bonds" in props:
            flattened["rtb"] = props.get("num_rotatable_bonds")

        if "lipinski_ro5_pass" not in props and "ro3_pass" in props:
            flattened["lipinski_ro5_pass"] = props.get("ro3_pass")

        flattened["ro3_pass"] = self._coerce_bool(flattened.get("ro3_pass"))
        flattened["lipinski_ro5_pass"] = self._coerce_bool(flattened.get("lipinski_ro5_pass"))

        flattened["molecule_properties"] = canonical_json(props)
        return flattened

    def _normalize_structures(self, molecule: dict[str, Any]) -> dict[str, Any]:
        flattened: dict[str, Any] = {
            "molecule_structures": None,
        }

        for field in self._structure_fields:
            flattened[field] = None

        structures = molecule.get("molecule_structures")
        if not isinstance(structures, dict) or not structures:
            return flattened

        flattened["standardized_smiles"] = registry.normalize(
            "chemistry",
            structures.get("canonical_smiles"),
        )
        flattened["standard_inchi"] = registry.normalize(
            "chemistry",
            structures.get("standard_inchi"),
        )
        flattened["standard_inchi_key"] = registry.normalize(
            "chemistry.string",
            structures.get("standard_inchi_key"),
            uppercase=True,
        )
        flattened["molecule_structures"] = canonical_json(structures)
        return flattened

    def _normalize_synonyms(self, molecule: dict[str, Any]) -> dict[str, Any]:
        flattened: dict[str, Any] = {
            "all_names": None,
            "molecule_synonyms": None,
        }

        synonyms = molecule.get("molecule_synonyms")
        if not isinstance(synonyms, list) or not synonyms:
            return flattened

        synonym_names: list[str] = []
        normalized_entries: list[Any] = []

        for entry in synonyms:
            if isinstance(entry, dict):
                normalized_entry = entry.copy()
                value = normalized_entry.get("molecule_synonym")
                if isinstance(value, str):
                    trimmed_value = value.strip()
                    normalized_entry["molecule_synonym"] = trimmed_value
                    synonym_names.append(trimmed_value)
                normalized_entries.append(normalized_entry)
            elif isinstance(entry, str):
                trimmed = entry.strip()
                synonym_names.append(trimmed)
                normalized_entries.append(trimmed)

        unique_names = sorted({name.strip() for name in synonym_names if name}, key=str.lower)
        if unique_names:
            flattened["all_names"] = "; ".join(unique_names)

        if normalized_entries:
            flattened["molecule_synonyms"] = canonical_json(self._sorted_synonym_entries(normalized_entries))

        return flattened

    def _flatten_json_fields(self, molecule: dict[str, Any]) -> dict[str, Any]:
        flattened: dict[str, Any] = {}
        for field in self._json_fields:
            if field == "molecule_synonyms":
                # already normalized via _normalize_synonyms
                continue
            value = molecule.get(field)
            if value in (None, ""):
                flattened[field] = None
            else:
                flattened[field] = canonical_json(value)
        return flattened

    @staticmethod
    def _sorted_synonym_entries(synonyms: list[Any]) -> list[Any]:
        def synonym_key(entry: Any) -> str:
            if isinstance(entry, dict):
                value = entry.get("molecule_synonym")
            else:
                value = entry
            if not isinstance(value, str):
                return ""
            return value.strip().lower()

        return sorted(synonyms, key=synonym_key)

    @staticmethod
    def _coerce_bool(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            if pd.isna(value):
                return None
            return bool(int(value))
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"y", "yes", "true", "t", "1"}:
                return True
            if normalized in {"n", "no", "false", "f", "0"}:
                return False
        return value

