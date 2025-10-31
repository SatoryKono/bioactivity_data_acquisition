"""Parser for ChEMBL activity API payloads."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Sequence

from bioetl.core.logger import UnifiedLogger
from bioetl.normalizers import registry
from bioetl.utils.json import normalize_json_list

from ..normalizer.activity_normalizer import ActivityNormalizer

__all__ = ["ActivityParser", "ACTIVITY_FALLBACK_BUSINESS_COLUMNS"]

logger = UnifiedLogger.get(__name__)

ACTIVITY_FALLBACK_BUSINESS_COLUMNS: tuple[str, ...] = (
    "activity_id",
    "molecule_chembl_id",
    "assay_chembl_id",
    "target_chembl_id",
    "document_chembl_id",
    "published_type",
    "published_relation",
    "published_value",
    "published_units",
    "standard_type",
    "standard_relation",
    "standard_value",
    "standard_units",
    "standard_flag",
    "lower_bound",
    "upper_bound",
    "is_censored",
    "pchembl_value",
    "activity_comment",
    "data_validity_comment",
    "bao_endpoint",
    "bao_format",
    "bao_label",
    "canonical_smiles",
    "target_organism",
    "target_tax_id",
    "activity_properties",
    "compound_key",
    "is_citation",
    "high_citation_rate",
    "exact_data_citation",
    "rounded_data_citation",
    "potential_duplicate",
    "uo_units",
    "qudt_units",
    "src_id",
    "action_type",
    "bei",
    "sei",
    "le",
    "lle",
    "chembl_release",
    "source_system",
    "fallback_reason",
    "fallback_error_type",
    "fallback_error_code",
    "fallback_error_message",
    "fallback_http_status",
    "fallback_retry_after_sec",
    "fallback_attempt",
    "fallback_timestamp",
    "extracted_at",
)


class ActivityParser:
    """Parse and normalise raw activity payloads from the ChEMBL API."""

    def __init__(
        self,
        *,
        normalizer: ActivityNormalizer | None = None,
        chembl_release: str | None = None,
    ) -> None:
        self._normalizer = normalizer or ActivityNormalizer()
        self._chembl_release = chembl_release

    def set_chembl_release(self, release: str | None) -> None:
        """Update the release metadata injected into parsed records."""

        self._chembl_release = release

    def parse(self, activity: Mapping[str, Any]) -> dict[str, Any]:
        """Convert a raw activity payload into the flattened record shape."""

        normalizer = self._normalizer

        activity_id = normalizer.normalize_int_scalar(activity.get("activity_id"))
        molecule_id = registry.normalize("chemistry.chembl_id", activity.get("molecule_chembl_id"))
        assay_id = registry.normalize("chemistry.chembl_id", activity.get("assay_chembl_id"))
        target_id = registry.normalize("chemistry.chembl_id", activity.get("target_chembl_id"))
        document_id = registry.normalize("chemistry.chembl_id", activity.get("document_chembl_id"))

        published_type = registry.normalize(
            "chemistry.string",
            activity.get("type") or activity.get("published_type"),
            uppercase=True,
        )
        published_relation = registry.normalize(
            "chemistry.relation",
            activity.get("relation") or activity.get("published_relation"),
            default="=",
        )
        published_value = registry.normalize(
            "chemistry.non_negative_float",
            activity.get("value") or activity.get("published_value"),
            column="published_value",
        )
        published_units = registry.normalize(
            "chemistry.units",
            activity.get("units") or activity.get("published_units"),
        )

        standard_type = registry.normalize(
            "chemistry.string",
            activity.get("standard_type"),
            uppercase=True,
        )
        standard_relation = registry.normalize(
            "chemistry.relation",
            activity.get("standard_relation"),
            default="=",
        )
        standard_value = registry.normalize(
            "chemistry.non_negative_float",
            activity.get("standard_value"),
            column="standard_value",
        )
        standard_units = registry.normalize(
            "chemistry.units",
            activity.get("standard_units"),
            default="nM",
        )
        standard_flag = normalizer.normalize_int_scalar(activity.get("standard_flag"))

        lower_bound = registry.normalize(
            "numeric", activity.get("standard_lower_value") or activity.get("lower_value")
        )
        upper_bound = registry.normalize(
            "numeric", activity.get("standard_upper_value") or activity.get("upper_value")
        )
        is_censored = normalizer.derive_is_censored(standard_relation)

        pchembl_value = registry.normalize("numeric", activity.get("pchembl_value"))
        activity_comment = registry.normalize("chemistry.string", activity.get("activity_comment"))
        data_validity_comment = registry.normalize(
            "chemistry.string", activity.get("data_validity_comment")
        )

        bao_endpoint = registry.normalize("chemistry.bao_id", activity.get("bao_endpoint"))
        bao_format = registry.normalize("chemistry.bao_id", activity.get("bao_format"))
        bao_label = registry.normalize("chemistry.string", activity.get("bao_label"), max_length=128)

        canonical_smiles = registry.normalize("chemistry.string", activity.get("canonical_smiles"))
        target_organism = registry.normalize(
            "chemistry.target_organism", activity.get("target_organism")
        )
        target_tax_id = normalizer.normalize_int_scalar(activity.get("target_tax_id"))

        potential_duplicate = normalizer.normalize_int_scalar(activity.get("potential_duplicate"))
        uo_units = registry.normalize("chemistry.string", activity.get("uo_units"), uppercase=True)
        qudt_units = registry.normalize("chemistry.string", activity.get("qudt_units"))
        src_id = normalizer.normalize_int_scalar(activity.get("src_id"))

        action_type_raw = activity.get("action_type")
        action_type_metadata: dict[str, str | None] | None = None
        if isinstance(action_type_raw, Mapping):
            action_type_metadata = {
                "action_type": registry.normalize(
                    "chemistry.string", action_type_raw.get("action_type")
                ),
                "label": registry.normalize(
                    "chemistry.string", action_type_raw.get("label")
                ),
                "description": registry.normalize(
                    "chemistry.string", action_type_raw.get("description")
                ),
            }
            action_type_raw = action_type_metadata.get("action_type")
            metadata_payload = {key: value for key, value in action_type_metadata.items() if value}
            if metadata_payload:
                logger.debug(
                    "activity_action_type_payload",
                    activity_id=activity_id,
                    **metadata_payload,
                )

        action_type = registry.normalize("chemistry.string", action_type_raw)

        properties_str, properties = normalize_json_list(activity.get("activity_properties"))
        ligand_efficiency = activity.get("ligand_efficiency") or activity.get("ligand_eff")
        bei, sei, le, lle = registry.normalize("chemistry.ligand_efficiency", ligand_efficiency)

        properties_sequence: Sequence[Mapping[str, Any]] = tuple(properties)
        compound_key = normalizer.derive_compound_key(molecule_id, standard_type, target_id)
        is_citation = normalizer.derive_is_citation(document_id, properties_sequence)
        exact_citation = normalizer.derive_exact_data_citation(
            data_validity_comment, properties_sequence
        )
        rounded_citation = normalizer.derive_rounded_data_citation(
            data_validity_comment, properties_sequence
        )
        high_citation_rate = normalizer.derive_high_citation_rate(properties_sequence)

        record: dict[str, Any] = {
            "activity_id": activity_id,
            "molecule_chembl_id": molecule_id,
            "assay_chembl_id": assay_id,
            "target_chembl_id": target_id,
            "document_chembl_id": document_id,
            "published_type": published_type,
            "published_relation": published_relation,
            "published_value": published_value,
            "published_units": published_units,
            "standard_type": standard_type,
            "standard_relation": standard_relation,
            "standard_value": standard_value,
            "standard_units": standard_units,
            "standard_flag": standard_flag,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "is_censored": is_censored,
            "pchembl_value": pchembl_value,
            "activity_comment": activity_comment,
            "data_validity_comment": data_validity_comment,
            "bao_endpoint": bao_endpoint,
            "bao_format": bao_format,
            "bao_label": bao_label,
            "canonical_smiles": canonical_smiles,
            "target_organism": target_organism,
            "target_tax_id": target_tax_id,
            "activity_properties": properties_str,
            "compound_key": compound_key,
            "is_citation": is_citation,
            "high_citation_rate": high_citation_rate,
            "exact_data_citation": exact_citation,
            "rounded_data_citation": rounded_citation,
            "potential_duplicate": potential_duplicate,
            "uo_units": uo_units,
            "qudt_units": qudt_units,
            "src_id": src_id,
            "action_type": action_type,
            "bei": bei,
            "sei": sei,
            "le": le,
            "lle": lle,
            "chembl_release": self._chembl_release,
            "source_system": "chembl",
            "fallback_reason": None,
            "fallback_error_type": None,
            "fallback_error_code": None,
            "fallback_error_message": None,
            "fallback_http_status": None,
            "fallback_retry_after_sec": None,
            "fallback_attempt": None,
            "fallback_timestamp": None,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

        return record
