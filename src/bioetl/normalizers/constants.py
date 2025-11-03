"""Константы для нормализации данных."""

# Строки, которые считаются NA/пустыми значениями
NA_STRINGS = {"", "na", "n/a", "none", "null", "nan"}

# Алиасы знаков сравнения для нормализации
RELATION_ALIASES = {
    "==": "=",
    "=": "=",
    "≡": "=",
    "<": "<",
    "≤": "<=",
    "⩽": "<=",
    "⩾": ">=",
    "≥": ">=",
    ">": ">",
    "~": "~",
}

# Синонимы единиц измерения для нормализации
UNIT_SYNONYMS = {
    "nm": "nM",
    "nanomolar": "nM",
    "μm": "µM",
    "µm": "µM",
    "um": "µM",
    "μmolar": "µM",
    "µmolar": "µM",
    "mum": "µM",
    "μmol/l": "µmol/L",
    "µmol/l": "µmol/L",
    "umol/l": "µmol/L",
    "mmol/l": "mmol/L",
    "mol/l": "mol/L",
    "μg/ml": "µg/mL",
    "μg/mL": "µg/mL",
    "µg/ml": "µg/mL",
    "µg/mL": "µg/mL",
    "ug/ml": "µg/mL",
    "ug/mL": "µg/mL",
    "ng/ml": "ng/mL",
    "pg/ml": "pg/mL",
    "mg/ml": "mg/mL",
    "mg/l": "mg/L",
    "ug/l": "µg/L",
    "μg/l": "µg/L",
    "µg/l": "µg/L",
}

# Строки, которые считаются True
BOOLEAN_TRUE = {"true", "1", "yes", "y", "t"}

# Строки, которые считаются False
BOOLEAN_FALSE = {"false", "0", "no", "n", "f"}
