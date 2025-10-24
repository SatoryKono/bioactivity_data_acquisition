"""
Базовый класс для нормализации данных.

Предоставляет общие методы нормализации для всех типов данных
с едиными стандартами форматирования.
"""

import pandas as pd
import re
from typing import Union, Optional, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class BaseNormalizer:
    """Базовые методы нормализации для всех типов данных."""
    
    @staticmethod
    def normalize_doi(doi: Union[str, None]) -> Optional[str]:
        """Канонический формат DOI: 10.xxxx/yyyy, lowercase, без URL.
        
        Args:
            doi: DOI для нормализации
            
        Returns:
            Нормализованный DOI или None
        """
        if pd.isna(doi) or doi is None or doi == "":
            return None
            
        doi = str(doi).strip().lower()
        
        # Удаление URL префиксов
        doi = re.sub(r'^https?://(?:dx\.)?doi\.org/', '', doi)
        
        # Проверка на валидный формат
        if re.match(r'^10\.\d+/.+$', doi):
            return doi
            
        logger.warning(f"Invalid DOI format: {doi}")
        return None
    
    @staticmethod
    def normalize_chembl_id(cid: Union[str, None]) -> Optional[str]:
        """Uppercase ChEMBL ID: ^CHEMBL\\d+$.
        
        Args:
            cid: ChEMBL ID для нормализации
            
        Returns:
            Нормализованный ChEMBL ID или None
        """
        if pd.isna(cid) or cid is None or cid == "":
            return None
            
        cid = str(cid).strip().upper()
        
        # Проверка на валидный формат
        if re.match(r'^CHEMBL\d+$', cid):
            return cid
            
        logger.warning(f"Invalid ChEMBL ID format: {cid}")
        return None
    
    @staticmethod
    def normalize_pmid(pmid: Union[str, int, None]) -> Optional[str]:
        """Числовой PMID: только цифры.
        
        Args:
            pmid: PMID для нормализации
            
        Returns:
            Нормализованный PMID или None
        """
        if pd.isna(pmid) or pmid is None or pmid == "":
            return None
            
        pmid = str(pmid).strip()
        
        # Проверка на числовой формат
        if pmid.isdigit():
            return pmid
            
        logger.warning(f"Invalid PMID format: {pmid}")
        return None
    
    @staticmethod
    def normalize_datetime_iso8601(dt: Any) -> Optional[str]:
        """ISO 8601: YYYY-MM-DDTHH:MM:SSZ, UTC.
        
        Args:
            dt: Дата/время для нормализации
            
        Returns:
            Нормализованная дата в ISO 8601 или None
        """
        if pd.isna(dt):
            return None
            
        try:
            if isinstance(dt, str):
                # Попытка парсинга строки
                dt = pd.to_datetime(dt, utc=True)
            elif not isinstance(dt, (pd.Timestamp, datetime)):
                # Конвертация в pandas Timestamp
                dt = pd.to_datetime(dt, utc=True)
                
            # Форматирование в ISO 8601
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            
        except Exception as e:
            logger.warning(f"Failed to normalize datetime {dt}: {e}")
            return None
    
    @staticmethod
    def normalize_boolean(value: Any) -> Optional[bool]:
        """Канонические boolean значения.
        
        Args:
            value: Значение для нормализации
            
        Returns:
            Нормализованное boolean или None
        """
        if pd.isna(value):
            return None
            
        if isinstance(value, bool):
            return value
            
        str_val = str(value).lower().strip()
        
        if str_val in ['true', '1', 'yes', 'y', 't']:
            return True
        elif str_val in ['false', '0', 'no', 'n', 'f']:
            return False
            
        logger.warning(f"Invalid boolean value: {value}")
        return None
    
    @staticmethod
    def normalize_float(value: Any, precision: int = 6) -> Optional[float]:
        """Нормализация float с заданной точностью.
        
        Args:
            value: Значение для нормализации
            precision: Количество знаков после запятой
            
        Returns:
            Нормализованное float или None
        """
        if pd.isna(value):
            return None
            
        try:
            float_val = float(value)
            return round(float_val, precision)
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to normalize float {value}: {e}")
            return None
    
    @staticmethod
    def normalize_string_strip(value: Union[str, None]) -> Optional[str]:
        """Удаление пробелов в начале и конце строки.
        
        Args:
            value: Строка для нормализации
            
        Returns:
            Нормализованная строка или None
        """
        if pd.isna(value) or value is None:
            return None
            
        return str(value).strip()
    
    @staticmethod
    def normalize_string_lower(value: Union[str, None]) -> Optional[str]:
        """Приведение к нижнему регистру.
        
        Args:
            value: Строка для нормализации
            
        Returns:
            Нормализованная строка или None
        """
        if pd.isna(value) or value is None:
            return None
            
        return str(value).lower()
    
    @staticmethod
    def normalize_string_upper(value: Union[str, None]) -> Optional[str]:
        """Приведение к верхнему регистру.
        
        Args:
            value: Строка для нормализации
            
        Returns:
            Нормализованная строка или None
        """
        if pd.isna(value) or value is None:
            return None
            
        return str(value).upper()
    
    @staticmethod
    def normalize_string_nfc(value: Union[str, None]) -> Optional[str]:
        """Нормализация Unicode в NFC форме.
        
        Args:
            value: Строка для нормализации
            
        Returns:
            Нормализованная строка или None
        """
        if pd.isna(value) or value is None:
            return None
            
        import unicodedata
        return unicodedata.normalize('NFC', str(value))
    
    @staticmethod
    def normalize_string_whitespace(value: Union[str, None]) -> Optional[str]:
        """Нормализация пробельных символов.
        
        Args:
            value: Строка для нормализации
            
        Returns:
            Нормализованная строка или None
        """
        if pd.isna(value) or value is None:
            return None
            
        # Замена множественных пробелов на одинарные
        return re.sub(r'\s+', ' ', str(value))
    
    @staticmethod
    def normalize_smiles(smiles: Union[str, None]) -> Optional[str]:
        """Нормализация SMILES строки.
        
        Args:
            smiles: SMILES для нормализации
            
        Returns:
            Нормализованный SMILES или None
        """
        if pd.isna(smiles) or smiles is None or smiles == "":
            return None
            
        smiles = str(smiles).strip()
        
        # Базовая проверка на валидность SMILES
        if len(smiles) > 0 and not smiles.isspace():
            return smiles
            
        return None
    
    @staticmethod
    def normalize_inchi_key(inchi_key: Union[str, None]) -> Optional[str]:
        """Нормализация InChI ключа.
        
        Args:
            inchi_key: InChI ключ для нормализации
            
        Returns:
            Нормализованный InChI ключ или None
        """
        if pd.isna(inchi_key) or inchi_key is None or inchi_key == "":
            return None
            
        inchi_key = str(inchi_key).strip()
        
        # Проверка на валидный формат InChI ключа
        if re.match(r'^[A-Z]{14}-[A-Z]{10}-[A-Z]$', inchi_key):
            return inchi_key
            
        logger.warning(f"Invalid InChI key format: {inchi_key}")
        return None


class NormalizationPipeline:
    """Пайплайн для применения цепочки нормализаций."""
    
    def __init__(self, functions: list[str]):
        """Инициализация пайплайна нормализации.
        
        Args:
            functions: Список функций нормализации для применения
        """
        self.functions = functions
        self.normalizer = BaseNormalizer()
    
    def apply(self, value: Any) -> Any:
        """Применение цепочки нормализаций к значению.
        
        Args:
            value: Значение для нормализации
            
        Returns:
            Нормализованное значение
        """
        result = value
        
        for func_name in self.functions:
            if hasattr(self.normalizer, func_name):
                func = getattr(self.normalizer, func_name)
                result = func(result)
                
                # Если функция вернула None, прерываем цепочку
                if result is None:
                    break
            else:
                logger.warning(f"Unknown normalization function: {func_name}")
                
        return result
