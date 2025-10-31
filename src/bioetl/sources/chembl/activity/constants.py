"""Constants used across the ChEMBL activity source modules."""

from __future__ import annotations

# Canonical activity type aliases derived from the public ChEMBL API
# documentation. The mapping is case-insensitive and collapses whitespace,
# hyphen and underscore variants that frequently appear in historical dumps.
ACTIVITY_TYPE_ALIASES: dict[str, str] = {
    "ic50": "IC50",
    "ic_50": "IC50",
    "ic-50": "IC50",
    "ic 50": "IC50",
    "ec50": "EC50",
    "ec_50": "EC50",
    "ec-50": "EC50",
    "ec 50": "EC50",
    "ac50": "AC50",
    "ac_50": "AC50",
    "ac-50": "AC50",
    "ac 50": "AC50",
    "kd": "Kd",
    "k_d": "Kd",
    "kon": "Kon",
    "koff": "Koff",
    "ki": "Ki",
    "k_i": "Ki",
    "pic50": "pIC50",
    "pic_50": "pIC50",
    "pic-50": "pIC50",
    "pic 50": "pIC50",
    "pki": "pKi",
    "pk_i": "pKi",
    "pkd": "pKd",
    "pk_d": "pKd",
    "gii": "GII",
    "residualactivity": "Residual Activity",
    "residual_activity": "Residual Activity",
}


# Unit ontology (UO) identifiers exposed by the official ChEMBL API mapped to
# their human readable counterparts. Only the subset currently referenced by
# the activity feed is enumerated; new values fall back to the API payload.
UO_UNITS_TO_STANDARD: dict[str, str] = {
    "UO:0000065": "nM",  # nanomolar concentration
    "UO:0000064": "µM",  # micromolar concentration
    "UO:0000062": "mM",  # millimolar concentration
    "UO:0000063": "M",  # molar concentration
    "UO:0000101": "mg/mL",
    "UO:0000175": "µg/mL",
    "UO:0000102": "mg/L",
    "UO:0000174": "µg/L",
    "UO:0000173": "ng/mL",
    "UO:0000172": "pg/mL",
}


# QUDT unit labels that appear in activity responses mapped to the notation
# expected by downstream QC. The keys are normalised to lower case to simplify
# lookups without assuming a particular capitalisation scheme.
QUDT_UNITS_TO_STANDARD: dict[str, str] = {
    "nanomolar": "nM",
    "micromolar": "µM",
    "millimolar": "mM",
    "molar": "M",
    "milligrampermilliliter": "mg/mL",
    "microgrampermilliliter": "µg/mL",
    "milligramperlitre": "mg/L",
    "microgramperlitre": "µg/L",
    "nanogrampermilliliter": "ng/mL",
    "picogrampermilliliter": "pg/mL",
}


__all__ = [
    "ACTIVITY_TYPE_ALIASES",
    "UO_UNITS_TO_STANDARD",
    "QUDT_UNITS_TO_STANDARD",
]

