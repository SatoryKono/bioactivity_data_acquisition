"""Утилита для формирования литературных ссылок на статьи."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd


def format_citation(
    journal: Any,
    year: Any,
    volume: Any,
    issue: Any,
    first_page: Any,
    last_page: Any,
) -> str:
    """Формирует литературную ссылку на статью по заданным правилам.
    
    Новый формат:
    - Если issue присутствует:  "<journal>, <year>, <volume> (<issue>). <pages>"
    - Если issue отсутствует:  "<journal>, <year>, <volume>. <pages>"
    
    Правила страницы/страниц (<pages>):
    1) Диапазон:
       Используй "p. <first_page>-<last_page>" только если выполнено ВСЕ:
       - first_page и last_page — целые числа;
       - 10000 < last_page > first_page;
       - last_page != first_page.

    2) Одна страница или аномальный last_page:
       Используй просто "<first_page>" если:
       - last_page == first_page, ИЛИ
       - last_page > 10000, ИЛИ
       - last_page не число.

    Args:
        journal: Название журнала
        year: Год публикации
        volume: Том журнала
        issue: Номер выпуска
        first_page: Первая страница
        last_page: Последняя страница
        
    Returns:
        Сформированная литературная ссылка
    """
    # Обрезаем пробелы у всех полей
    journal = _clean_field(journal)
    year = _clean_field(year)
    volume = _clean_field(volume)
    issue = _clean_field(issue)
    first_page = _clean_field(first_page)
    last_page = _clean_field(last_page)
    
    # Если journal пустой, возвращаем пустую строку
    if not journal:
        return ""
    
    # Формируем основную часть ссылки
    citation_parts = []
    citation_parts.append(journal)
    
    # Добавляем год если он не пустой
    if year:
        citation_parts.append(year)
    
    # Добавляем volume и issue если они не пустые
    if volume:
        # Конвертируем volume в целое число если возможно
        volume_int = _try_convert_to_int(volume)
        if volume_int is not None:
            volume_str = str(volume_int)
        else:
            volume_str = volume
            
        if issue:
            # С issue: "journal, year, volume (issue)"
            volume_issue = f"{volume_str} ({issue})"
            citation_parts.append(volume_issue)
        else:
            # Без issue: "journal, year, volume"
            citation_parts.append(volume_str)
    
    # Формируем часть со страницами
    pages = _format_pages(first_page, last_page)
    
    # Собираем финальную ссылку
    if pages:
        citation = ", ".join(citation_parts) + ". " + pages
    else:
        citation = ", ".join(citation_parts) + "."
    
    # Убираем двойные пробелы и висячие знаки препинания
    citation = re.sub(r'\s+', ' ', citation)
    citation = re.sub(r'\s+\.$', '.', citation)
    
    return citation


def _clean_field(field: Any) -> str:
    """Очищает поле от пробелов и приводит к строке."""
    if pd.isna(field) or field is None:
        return ""
    return str(field).strip()


def _format_pages(first_page: Any, last_page: Any) -> str:
    """Формирует часть ссылки со страницами согласно правилам."""
    # Пытаемся привести к int
    first_page_int = _try_convert_to_int(first_page)
    last_page_int = _try_convert_to_int(last_page)
    
    # Если first_page отсутствует/нечисловой, а last_page числовой, подставляем last_page
    if first_page_int is None and last_page_int is not None:
        first_page_int = last_page_int
    
    # Если отсутствуют и first_page, и last_page, оставляем пустым
    if first_page_int is None and last_page_int is None:
        # Проверяем, есть ли хоть какое-то значение first_page (нечисловое)
        if first_page and str(first_page).strip():
            return str(first_page).strip()
        return ""
    
    # Если first_page нечисловой, возвращаем его как есть
    if first_page_int is None:
        if first_page and str(first_page).strip():
            return str(first_page).strip()
        return ""
    
    # Если last_page нечисловой, возвращаем только first_page
    if last_page_int is None:
        return str(first_page_int)
    
    # Проверяем условия для диапазона страниц
    if (last_page_int != first_page_int and 
        last_page_int <= 10000 and 
        last_page_int > first_page_int):
        return f"p. {first_page_int}-{last_page_int}"
    else:
        # Одна страница или аномальный last_page
        return str(first_page_int)


def _try_convert_to_int(value: Any) -> int | None:
    """Пытается привести значение к int."""
    if pd.isna(value) or value is None:
        return None
    
    try:
        # Убираем пробелы и пытаемся конвертировать
        cleaned = str(value).strip()
        if not cleaned:
            return None
        
        # Проверяем, что это число (может быть строкой вида "123" или "123.0")
        if re.match(r'^\d+(\.0+)?$', cleaned):
            return int(float(cleaned))
        else:
            return None
    except (ValueError, TypeError):
        return None


def add_citation_column(df: pd.DataFrame, column_mapping: dict[str, str] | None = None) -> pd.DataFrame:
    """Добавляет колонку с литературными ссылками к DataFrame.
    
    Args:
        df: DataFrame с данными документов
        column_mapping: Словарь маппинга колонок для форматирования цитат.
                       Если None, используются стандартные имена колонок.
                       Формат: {"journal": "column_name", "volume": "column_name", ...}
        
    Returns:
        DataFrame с добавленной колонкой 'document_citation'
    """
    df_copy = df.copy()
    
    # Стандартный маппинг колонок
    default_mapping = {
        "journal": "journal",
        "year": "year",
        "volume": "volume", 
        "issue": "issue",
        "first_page": "first_page",
        "last_page": "last_page"
    }
    
    # Используем переданный маппинг или стандартный
    mapping = column_mapping or default_mapping
    
    # Формируем ссылки для каждой строки
    citations = []
    for _, row in df_copy.iterrows():
        citation = format_citation(
            journal=row.get(mapping.get("journal", "journal")),
            year=row.get(mapping.get("year", "year")),
            volume=row.get(mapping.get("volume", "volume")),
            issue=row.get(mapping.get("issue", "issue")),
            first_page=row.get(mapping.get("first_page", "first_page")),
            last_page=row.get(mapping.get("last_page", "last_page"))
        )
        citations.append(citation)
    
    df_copy['document_citation'] = citations
    return df_copy


__all__ = [
    "format_citation",
    "add_citation_column",
]
