"""
Нормализаторы для различных типов идентификаторов.
"""

import re
from typing import Any

from .base import ensure_string, is_empty_value, register_normalizer, safe_normalize


@safe_normalize
def normalize_doi(value: Any) -> str | None:
    """Нормализует DOI согласно спецификации.
    
    - strip(): обрезать пробелы в начале/конце
    - Unicode: привести строку к NFC
    - Снять оболочку: удалить префиксы "doi:", "urn:doi:", "info:doi/"
    - URL-оболочки: удалить "http://doi.org/", "https://doi.org/", "http://dx.doi.org/", "https://dx.doi.org/"
    - Процент-коды: декодировать percent-encoding
    - Пробелы: удалить все пробелы вокруг разделителя "/" и внутри строки
    - Регистр: привести всю строку к lowercase
    - Хвостовая пунктуация: снять завершающие ".", ",", ";", ")", "]", "}" и кавычки
    - Множественные слэши: сократить повторы и оставить ровно один разделитель
    - Валидация формы: итог должен быть вида "<префикс>/<суффикс>"
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный DOI или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    # Trim: обрезать пробелы в начале/конце
    text = text.strip()
    if not text:
        return None
    
    # Unicode: привести строку к NFC
    import unicodedata
    text = unicodedata.normalize('NFC', text)
    
    # Снять оболочку: удалить префиксы
    prefixes_to_remove = [
        r'^doi:\s*',
        r'^urn:doi:\s*',
        r'^info:doi/\s*',
        r'^https?://doi\.org/\s*',
        r'^https?://dx\.doi\.org/\s*'
    ]
    
    for prefix_pattern in prefixes_to_remove:
        text = re.sub(prefix_pattern, '', text, flags=re.IGNORECASE)
    
    # Процент-коды: декодировать percent-encoding
    from urllib.parse import unquote
    text = unquote(text)
    
    # Пробелы: удалить все пробелы вокруг разделителя "/" и внутри строки
    # Сначала нормализуем пробелы вокруг слэша
    text = re.sub(r'\s*/\s*', '/', text)
    # Затем удаляем все остальные пробелы
    text = re.sub(r'\s+', '', text)
    
    # Регистр: привести всю строку к lowercase
    text = text.lower()
    
    # Множественные слэши: сократить повторы и оставить ровно один разделитель
    text = re.sub(r'/+', '/', text)
    
    # Хвостовая пунктуация: снять завершающие символы
    text = re.sub(r'[.,;)\]}\'\"]+$', '', text)
    
    # Удалить завершающие слэши, если они есть
    text = text.rstrip('/')
    
    # Валидация формы: проверяем, что это валидный DOI
    doi_pattern = r'^10\.\d+/.+$'
    if not re.match(doi_pattern, text):
        return None
    
    return text


@safe_normalize
def normalize_chembl_id(value: Any) -> str | None:
    """Нормализует ChEMBL ID.
    
    - strip(): удаление пробелов
    - uppercase(): приведение к верхнему регистру
    - ensure_prefix_chembl(): обеспечение префикса CHEMBL
    - validate_pattern(): проверка формата ^CHEMBL\\d+$
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный ChEMBL ID или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    # Удаляем пробелы
    text = text.strip()
    if not text:
        return None
    
    # Приводим к верхнему регистру
    text = text.upper()
    
    # Удаляем префикс если есть
    if text.startswith('CHEMBL'):
        pass  # Уже есть префикс
    elif text.isdigit():
        text = f"CHEMBL{text}"
    else:
        # Пробуем извлечь число из строки
        match = re.search(r'(\d+)', text)
        if match:
            text = f"CHEMBL{match.group(1)}"
        else:
            return None
    
    # Валидация формата
    if not re.match(r'^CHEMBL\d+$', text):
        return None
    
    return text


@safe_normalize
def normalize_uniprot_id(value: Any) -> str | None:
    """Нормализует UniProt ID.
    
    - strip(): удаление пробелов
    - uppercase(): приведение к верхнему регистру
    - validate_uniprot_format(): проверка формата UniProt
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный UniProt ID или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    # Удаляем пробелы
    text = text.strip()
    if not text:
        return None
    
    # Приводим к верхнему регистру
    text = text.upper()
    
    # Валидация формата UniProt
    # Новый формат: [OPQ][0-9][A-Z0-9]{3}[0-9]
    # Старый формат: [A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}
    uniprot_pattern = r'^([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2})$'
    
    if not re.match(uniprot_pattern, text):
        return None
    
    return text


@safe_normalize
def normalize_iuphar_id(value: Any) -> str | None:
    """Нормализует IUPHAR ID.
    
    - strip(): удаление пробелов
    - remove_prefixes(): удаление префиксов (GTOPDB: и др.)
    - validate_positive_int(): проверка что является положительным целым
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный IUPHAR ID или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    # Удаляем пробелы
    text = text.strip()
    if not text:
        return None
    
    # Удаляем префиксы
    prefixes_to_remove = ['GTOPDB:', 'IUPHAR:', 'iuphar:']
    for prefix in prefixes_to_remove:
        if text.upper().startswith(prefix.upper()):
            text = text[len(prefix):]
            break
    
    # Проверяем что это положительное целое число
    try:
        id_num = int(text)
        if id_num > 0:
            return str(id_num)
    except (ValueError, TypeError):
        pass
    
    return None


@safe_normalize
def normalize_pubchem_cid(value: Any) -> int | None:
    """Нормализует PubChem CID.
    
    - strip(): удаление пробелов
    - remove_prefixes(): удаление CID: или URL-частей
    - validate_positive_int(): проверка что целое > 0
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный PubChem CID или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    # Удаляем пробелы
    text = text.strip()
    if not text:
        return None
    
    # Удаляем префиксы и URL части
    prefixes_to_remove = [
        'CID:',
        'cid:',
        'https://pubchem.ncbi.nlm.nih.gov/compound/',
        'http://pubchem.ncbi.nlm.nih.gov/compound/',
    ]
    
    for prefix in prefixes_to_remove:
        if text.lower().startswith(prefix.lower()):
            text = text[len(prefix):]
            break
    
    # Удаляем URL параметры если есть
    if '?' in text:
        text = text.split('?')[0]
    
    # Проверяем что это положительное целое число
    try:
        cid_num = int(text)
        if cid_num > 0:
            return cid_num
    except (ValueError, TypeError):
        pass
    
    return None


@safe_normalize
def normalize_pmid(value: Any) -> int | None:
    """Нормализует PubMed ID.
    
    - strip(): удаление пробелов
    - validate_positive_int(): проверка что является положительным целым
    
    Args:
        value: Значение для нормализации
        
    Returns:
        Нормализованный PMID или None
    """
    if is_empty_value(value):
        return None
    
    text = ensure_string(value)
    if text is None:
        return None
    
    # Удаляем пробелы
    text = text.strip()
    if not text:
        return None
    
    # Проверяем что это положительное целое число
    try:
        pmid_num = int(text)
        if pmid_num > 0:
            return pmid_num
    except (ValueError, TypeError):
        pass
    
    return None


# Регистрация всех нормализаторов
register_normalizer("normalize_doi", normalize_doi)
register_normalizer("normalize_chembl_id", normalize_chembl_id)
register_normalizer("normalize_uniprot_id", normalize_uniprot_id)
register_normalizer("normalize_iuphar_id", normalize_iuphar_id)
register_normalizer("normalize_pubchem_cid", normalize_pubchem_cid)
register_normalizer("normalize_pmid", normalize_pmid)
