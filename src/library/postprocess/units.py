"""Unit normalization helpers for activity post-processing."""

from __future__ import annotations

import math
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from typing import Final

__all__ = [
    "ALLOWED_UNITS",
    "UnitNormalizationError",
    "PChEMBLComputationError",
    "normalize_unit",
    "to_nM",
    "pchembl_from_value",
]

ALLOWED_UNITS: Final[set[str]] = {"pM", "nM", "uM", "mM", "M"}
_UNIT_CANONICAL = {
    "pm": "pM",
    "nm": "nM",
    "um": "uM",
    "mm": "mM",
    "m": "M",
}
_UNIT_FACTORS = {
    "pM": Decimal("1e-3"),
    "nM": Decimal("1"),
    "uM": Decimal("1e3"),
    "mM": Decimal("1e6"),
    "M": Decimal("1e9"),
}


class UnitNormalizationError(ValueError):
    """Raised when an activity unit is not supported."""

    def __init__(self, unit: str) -> None:
        self.unit = unit
        self.code = "unit_unknown"
        super().__init__(unit)


class PChEMBLComputationError(ValueError):
    """Raised when a pChEMBL value cannot be derived."""

    def __init__(self, message: str) -> None:
        self.code = "pchembl_out_of_range"
        super().__init__(message)


def normalize_unit(unit: str) -> str:
    """Normalize raw unit strings into the canonical activity domain."""

    cleaned = unit.strip().replace("µ", "u").replace("μ", "u")
    lowered = cleaned.lower()
    canonical = _UNIT_CANONICAL.get(lowered)
    if canonical is None:
        raise UnitNormalizationError(unit)
    return canonical


def to_nM(value: float, unit: str) -> float:
    """Convert a value expressed in ``unit`` into nanomolar."""

    if unit not in ALLOWED_UNITS:
        raise UnitNormalizationError(unit)
    factor = _UNIT_FACTORS[unit]
    decimal_value = Decimal(str(value))
    converted = decimal_value * factor
    return float(converted)


def pchembl_from_value(value_nM: float) -> float:
    """Compute pChEMBL value from nanomolar concentration."""

    decimal_value = Decimal(str(value_nM))
    if decimal_value <= 0:
        raise PChEMBLComputationError("value must be positive")
    with localcontext() as ctx:
        ctx.prec = 50
        log_value = math.log10(float(decimal_value))
    pchembl = Decimal(str(9 - log_value))
    if pchembl < 0 or pchembl > 20:
        raise PChEMBLComputationError("pChEMBL outside [0, 20]")
    quantized = pchembl.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
    return float(quantized)
