"""
Базовый нормализатор для унификации всех пайплайнов.
Содержит унифицированные функции нормализации для специальных форматов.
"""

import re
import hashlib
from typing import Any, Optional, Union
import pandas as pd
from datetime import datetime, timezone


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
    def normalize_doi(value: Any) -> Optional[str]:
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
        url_prefixes = [
            'https://doi.org/',
            'http://doi.org/',
            'doi.org/',
            'https://dx.doi.org/',
            'http://dx.doi.org/',
            'dx.doi.org/'
        ]
        
        for prefix in url_prefixes:
            if doi_str.lower().startswith(prefix.lower()):
                doi_str = doi_str[len(prefix):]
                break
        
        # Приводим к нижнему регистру
        doi_str = doi_str.lower()
        
        # Проверяем соответствие паттерну DOI
        doi_pattern = r'^10\.\d+/[^\s]+$'
        if re.match(doi_pattern, doi_str):
            return doi_str
        
        return None
    
    @staticmethod
    def normalize_chembl_id(value: Any) -> Optional[str]:
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
        chembl_pattern = r'^CHEMBL\d+$'
        if re.match(chembl_pattern, chembl_str):
            return chembl_str
        
        return None
    
    @staticmethod
    def normalize_uniprot_id(value: Any) -> Optional[str]:
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
        uniprot_pattern = r'^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$'
        if re.match(uniprot_pattern, uniprot_str):
            return uniprot_str
        
        return None
    
    @staticmethod
    def normalize_pmid(value: Any) -> Optional[str]:
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
    def normalize_inchi_key(value: Any) -> Optional[str]:
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
        inchi_key_pattern = r'^[A-Z]{14}-[A-Z]{10}-[A-Z]$'
        if re.match(inchi_key_pattern, inchi_key_str):
            return inchi_key_str
        
        return None
    
    @staticmethod
    def normalize_inchi(value: Any) -> Optional[str]:
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
        inchi_pattern = r'^InChI=1S?/[^\s]+$'
        if re.match(inchi_pattern, inchi_str):
            return inchi_str
        
        return None
    
    @staticmethod
    def normalize_datetime_iso8601(value: Any) -> Optional[str]:
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
            return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def normalize_boolean(value: Any) -> Optional[bool]:
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
        if value_str in ['true', '1', 'yes', 'y', 't', 'on']:
            return True
        
        # False значения
        if value_str in ['false', '0', 'no', 'n', 'f', 'off']:
            return False
        
        return None
    
    @staticmethod
    def normalize_float(value: Any, precision: int = 6) -> Optional[float]:
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
    def normalize_int(value: Any) -> Optional[int]:
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
    def compute_hash_row(row: pd.Series, exclude_columns: Optional[list] = None) -> str:
        """
        Вычисление SHA256 хеша строки.
        
        Args:
            row: Строка DataFrame
            exclude_columns: Колонки для исключения из хеша
            
        Returns:
            SHA256 хеш строки
        """
        if exclude_columns is None:
            exclude_columns = ['hash_row', 'hash_business_key']
        
        # Исключаем указанные колонки
        row_for_hash = row.drop(exclude_columns, errors='ignore')
        
        # Сортируем по именам колонок для детерминизма
        row_for_hash = row_for_hash.sort_index()
        
        # Создаем строку для хеширования
        hash_string = '|'.join([f"{k}:{v}" for k, v in row_for_hash.items()])
        
        # Вычисляем SHA256
        return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()
    
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
        hash_string = '|'.join(business_key_values)
        
        # Вычисляем SHA256
        return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()
    
    @staticmethod
    def normalize_string(value: Any, max_length: Optional[int] = None) -> Optional[str]:
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
