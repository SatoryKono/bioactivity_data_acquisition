"""Утилиты для конвертации списков в строки в API клиентах."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def convert_list_to_string(value: Any, separator: str = "; ") -> str | None:
    """Конвертировать список в строку для сохранения в DataFrame.
    
    Args:
        value: Значение для конвертации (может быть list, string, None)
        separator: Разделитель для элементов списка
        
    Returns:
        Строка с элементами списка или None если значение пустое
    """
    if value is None:
        return None
    
    if isinstance(value, list):
        if not value:
            return None
        
        # Фильтруем None и пустые значения
        filtered_values = [str(v) for v in value if v is not None and str(v).strip()]
        
        if not filtered_values:
            return None
            
        return separator.join(filtered_values)
    
    if isinstance(value, str):
        return value.strip() if value.strip() else None
    
    return str(value) if value else None


def convert_authors_list(authors: Any) -> str | None:
    """Специальная функция для конвертации списка авторов.
    
    Args:
        authors: Список авторов или строка
        
    Returns:
        Строка с авторами, разделенными точкой с запятой
    """
    if authors is None:
        return None
    
    if isinstance(authors, list):
        if not authors:
            return None
        
        # Обрабатываем разные форматы авторов
        author_names = []
        for author in authors:
            if isinstance(author, dict):
                # Crossref формат: {"given": "John", "family": "Doe"}
                given = author.get("given", "")
                family = author.get("family", "")
                if given or family:
                    full_name = f"{given} {family}".strip()
                    author_names.append(full_name)
                elif "name" in author:
                    # Простой формат с полем name
                    author_names.append(str(author["name"]))
                elif "display_name" in author:
                    # OpenAlex формат: {"display_name": "John Doe"}
                    author_names.append(str(author["display_name"]))
            elif isinstance(author, str) and author.strip():
                author_names.append(author.strip())
        
        return "; ".join(author_names) if author_names else None
    
    if isinstance(authors, str):
        return authors.strip() if authors.strip() else None
    
    return None


def convert_issn_list(issn: Any) -> str | None:
    """Специальная функция для конвертации ISSN.
    
    Args:
        issn: ISSN значение (может быть list или string)
        
    Returns:
        Первый ISSN из списка или строка ISSN
    """
    if issn is None:
        return None
    
    if isinstance(issn, list):
        if not issn:
            return None
        # Берем первый ISSN из списка
        first_issn = issn[0]
        return str(first_issn).strip() if first_issn else None
    
    if isinstance(issn, str):
        return issn.strip() if issn.strip() else None
    
    return str(issn).strip() if issn else None


def convert_subject_list(subject: Any) -> str | None:
    """Специальная функция для конвертации предметных областей.
    
    Args:
        subject: Предметная область (может быть list или string)
        
    Returns:
        Строка с предметными областями
    """
    return convert_list_to_string(subject, "; ")


def safe_str_convert(value: Any) -> str | None:
    """Безопасная конвертация любого значения в строку.
    
    Args:
        value: Любое значение для конвертации
        
    Returns:
        Строковое представление или None
    """
    if value is None:
        return None
    
    if isinstance(value, (int, float)):
        return str(value)
    
    if isinstance(value, str):
        return value.strip() if value.strip() else None
    
    if isinstance(value, list):
        return convert_list_to_string(value)
    
    if isinstance(value, dict):
        # Для словарей возвращаем JSON строку или None
        try:
            import json
            return json.dumps(value) if value else None
        except Exception:
            return str(value) if value else None
    
    return str(value) if value else None
