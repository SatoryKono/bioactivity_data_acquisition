"""
DEPRECATED: Базовый нормализатор для унификации всех пайплайнов.

Этот модуль устарел и будет удален в будущей версии.
Используйте library.normalizers вместо этого модуля.

Содержит унифицированные функции нормализации для специальных форматов.
"""

import hashlib
import re
import warnings
from datetime import timezone
from typing import Any

import pandas as pd

from library.normalizers import normalize_boolean as _normalize_boolean
from library.normalizers import normalize_chembl_id as _normalize_chembl_id
from library.normalizers import (
    normalize_datetime_iso8601 as _normalize_datetime_iso8601,
)
from library.normalizers import normalize_doi as _normalize_doi
from library.normalizers import normalize_float as _normalize_float
from library.normalizers import normalize_inchi as _normalize_inchi
from library.normalizers import normalize_inchi_key as _normalize_inchi_key
from library.normalizers import normalize_int as _normalize_int
from library.normalizers import normalize_pmid as _normalize_pmid
from library.normalizers import normalize_string_strip as _normalize_string_strip
from library.normalizers import normalize_uniprot_id as _normalize_uniprot_id

warnings.warn("library.common.base_normalizer is deprecated. Use library.normalizers instead.", DeprecationWarning, stacklevel=2)


class BaseNormalizer:
    """
    Базовый класс для нормализации данных.

    Содержит унифицированные функции нормализации для:
    - DOI идентификаторов
    - ChEMBL ID
    - UniProt ID
    - PMID
    - InChI/InChI Key
    - Дат и времени
    - Булевых значений
    - Числовых значений
    """

    @staticmethod
    def normalize_doi(value: Any) -> str | None:
        """
        Нормализация DOI идентификатора.

        Args:
            value: DOI значение для нормализации

        Returns:
            Нормализованный DOI или None
        """
        if pd.isna(value) or value is None:
            return None

        doi_str = str(value).strip()

        # Удаляем URL префиксы
        url_prefixes = ["https://doi.org/", "http://doi.org/", "doi.org/", "https://dx.doi.org/", "http://dx.doi.org/", "dx.doi.org/"]

        for prefix in url_prefixes:
            if doi_str.lower().startswith(prefix.lower()):
                doi_str = doi_str[len(prefix) :]
                break

        # Приводим к нижнему регистру
        doi_str = doi_str.lower()

        # Проверяем соответствие паттерну DOI
        doi_pattern = r"^10\.\d+/[^\s]+$"
        if re.match(doi_pattern, doi_str):
            return doi_str

        return None

    @staticmethod
    def normalize_chembl_id(value: Any) -> str | None:
        """
        Нормализация ChEMBL ID.

        Args:
            value: ChEMBL ID для нормализации

        Returns:
            Нормализованный ChEMBL ID или None
        """
        if pd.isna(value) or value is None:
            return None

        chembl_str = str(value).strip().upper()

        # Проверяем соответствие паттерну
        chembl_pattern = r"^CHEMBL\d+$"
        if re.match(chembl_pattern, chembl_str):
            return chembl_str

        return None

    @staticmethod
    def normalize_uniprot_id(value: Any) -> str | None:
        """
        Нормализация UniProt ID.

        Args:
            value: UniProt ID для нормализации

        Returns:
            Нормализованный UniProt ID или None
        """
        if pd.isna(value) or value is None:
            return None

        uniprot_str = str(value).strip().upper()

        # Проверяем соответствие паттерну UniProt
        uniprot_pattern = r"^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$"
        if re.match(uniprot_pattern, uniprot_str):
            return uniprot_str

        return None

    @staticmethod
    def normalize_pmid(value: Any) -> str | None:
        """
        Нормализация PMID.

        Args:
            value: PMID для нормализации

        Returns:
            Нормализованный PMID или None
        """
        if pd.isna(value) or value is None:
            return None

        pmid_str = str(value).strip()

        # Проверяем, что это число
        if pmid_str.isdigit():
            return pmid_str

        return None

    @staticmethod
    def normalize_inchi_key(value: Any) -> str | None:
        """
        Нормализация InChI Key.

        Args:
            value: InChI Key для нормализации

        Returns:
            Нормализованный InChI Key или None
        """
        if pd.isna(value) or value is None:
            return None

        inchi_key_str = str(value).strip().upper()

        # Проверяем соответствие паттерну InChI Key
        inchi_key_pattern = r"^[A-Z]{14}-[A-Z]{10}-[A-Z]$"
        if re.match(inchi_key_pattern, inchi_key_str):
            return inchi_key_str

        return None

    @staticmethod
    def normalize_inchi(value: Any) -> str | None:
        """
        Нормализация InChI.

        Args:
            value: InChI для нормализации

        Returns:
            Нормализованный InChI или None
        """
        if pd.isna(value) or value is None:
            return None

        inchi_str = str(value).strip()

        # Проверяем соответствие паттерну InChI
        inchi_pattern = r"^InChI=1S?/[^\s]+$"
        if re.match(inchi_pattern, inchi_str):
            return inchi_str

        return None

    @staticmethod
    def normalize_datetime_iso8601(value: Any) -> str | None:
        """
        Нормализация даты/времени в ISO 8601 формат.

        Args:
            value: Дата/время для нормализации

        Returns:
            Нормализованная дата в ISO 8601 или None
        """
        if pd.isna(value) or value is None:
            return None

        try:
            # Пытаемся парсить как datetime
            if isinstance(value, str):
                dt = pd.to_datetime(value)
            else:
                dt = value

            # Приводим к UTC если нужно
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)

            # Форматируем в ISO 8601
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        except (ValueError, TypeError):
            return None

    @staticmethod
    def normalize_boolean(value: Any) -> bool | None:
        """
        Нормализация булевого значения.

        Args:
            value: Значение для нормализации

        Returns:
            Нормализованное булево значение или None
        """
        if pd.isna(value) or value is None:
            return None

        if isinstance(value, bool):
            return value

        value_str = str(value).lower().strip()

        # True значения
        if value_str in ["true", "1", "yes", "y", "t", "on"]:
            return True

        # False значения
        if value_str in ["false", "0", "no", "n", "f", "off"]:
            return False

        return None

    @staticmethod
    def normalize_float(value: Any, precision: int = 6) -> float | None:
        """
        Нормализация числа с плавающей точкой.

        Args:
            value: Значение для нормализации
            precision: Количество знаков после запятой

        Returns:
            Нормализованное число или None
        """
        if pd.isna(value) or value is None:
            return None

        try:
            float_val = float(value)
            return round(float_val, precision)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def normalize_int(value: Any) -> int | None:
        """
        Нормализация целого числа.

        Args:
            value: Значение для нормализации

        Returns:
            Нормализованное целое число или None
        """
        if pd.isna(value) or value is None:
            return None

        try:
            return int(float(value))  # Сначала float, потом int для обработки "1.0"
        except (ValueError, TypeError):
            return None

    @staticmethod
    def compute_hash_row(row: pd.Series, exclude_columns: list | None = None) -> str:
        """
        Вычисление SHA256 хеша строки.

        Args:
            row: Строка DataFrame
            exclude_columns: Колонки для исключения из хеша

        Returns:
            SHA256 хеш строки
        """
        if exclude_columns is None:
            exclude_columns = ["hash_row", "hash_business_key"]

        # Исключаем указанные колонки
        row_for_hash = row.drop(exclude_columns, errors="ignore")

        # Сортируем по именам колонок для детерминизма
        row_for_hash = row_for_hash.sort_index()

        # Создаем строку для хеширования
        hash_string = "|".join([f"{k}:{v}" for k, v in row_for_hash.items()])

        # Вычисляем SHA256
        return hashlib.sha256(hash_string.encode("utf-8")).hexdigest()

    @staticmethod
    def compute_hash_business_key(row: pd.Series, business_key_columns: list) -> str:
        """
        Вычисление SHA256 хеша бизнес-ключа.

        Args:
            row: Строка DataFrame
            business_key_columns: Колонки, составляющие бизнес-ключ

        Returns:
            SHA256 хеш бизнес-ключа
        """
        # Извлекаем только колонки бизнес-ключа
        business_key_values = []
        for col in business_key_columns:
            if col in row.index:
                business_key_values.append(f"{col}:{row[col]}")

        # Создаем строку для хеширования
        hash_string = "|".join(business_key_values)

        # Вычисляем SHA256
        return hashlib.sha256(hash_string.encode("utf-8")).hexdigest()

    @staticmethod
    def normalize_string(value: Any, max_length: int | None = None) -> str | None:
        """
        Нормализация строки.

        Args:
            value: Значение для нормализации
            max_length: Максимальная длина строки

        Returns:
            Нормализованная строка или None
        """
        if pd.isna(value) or value is None:
            return None

        normalized_str = str(value).strip()

        if max_length and len(normalized_str) > max_length:
            normalized_str = normalized_str[:max_length]

        return normalized_str if normalized_str else None


# Глобальный экземпляр нормализатора
normalizer = BaseNormalizer()


# Re-export from the new unified normalizers system for backward compatibility


# Re-export with deprecation warnings
def _deprecated_wrapper(name: str, obj):
    def wrapper(*args, **kwargs):
        warnings.warn(f"library.common.base_normalizer.{name} is deprecated. Use library.normalizers.{name} instead.", DeprecationWarning, stacklevel=3)
        return obj(*args, **kwargs)

    return wrapper


# Re-export functions with deprecation warnings
BaseNormalizer.normalize_doi = _deprecated_wrapper("normalize_doi", _normalize_doi)
BaseNormalizer.normalize_chembl_id = _deprecated_wrapper("normalize_chembl_id", _normalize_chembl_id)
BaseNormalizer.normalize_uniprot_id = _deprecated_wrapper("normalize_uniprot_id", _normalize_uniprot_id)
BaseNormalizer.normalize_pmid = _deprecated_wrapper("normalize_pmid", _normalize_pmid)
BaseNormalizer.normalize_inchi = _deprecated_wrapper("normalize_inchi", _normalize_inchi)
BaseNormalizer.normalize_inchi_key = _deprecated_wrapper("normalize_inchi_key", _normalize_inchi_key)
# BaseNormalizer.normalize_smiles = _deprecated_wrapper("normalize_smiles", _normalize_smiles)
BaseNormalizer.normalize_boolean = _deprecated_wrapper("normalize_boolean", _normalize_boolean)
BaseNormalizer.normalize_float = _deprecated_wrapper("normalize_float", _normalize_float)
BaseNormalizer.normalize_int = _deprecated_wrapper("normalize_int", _normalize_int)
BaseNormalizer.normalize_datetime_iso8601 = _deprecated_wrapper("normalize_datetime_iso8601", _normalize_datetime_iso8601)
BaseNormalizer.normalize_string = _deprecated_wrapper("normalize_string", _normalize_string_strip)
