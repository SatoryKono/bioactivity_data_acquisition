"""Утилита для формирования литературных ссылок на статьи."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd


def format_citation(
    journal: Any,
    volume: Any,
    issue: Any,
    first_page: Any,
    last_page: Any,
) -> str:
    """Формирует литературную ссылку на статью по заданным правилам.
    
    Формат базовый:
    - Если issue непустой:  "<journal>, <volume> (<issue>). <pages>"
    - Если issue пустой/NaN/"" :  "<journal>, <volume>. <pages>"
    
    Правила страницы/страниц (<pages>):
    1) Диапазон:
       Используй "p. <first_page>-<last_page>" только если выполнено ВСЕ:
       - first_page и last_page — целые числа;
       - 100000 > last_page > first_page;
       - last_page != first_page.

    2) Одна страница или аномальный last_page:
       Используй просто "<first_page>" если:
       - last_page == first_page, ИЛИ
       - last_page >= 100000, ИЛИ
       - last_page не число.

    Args:
        journal: Название журнала
        volume: Том журнала
        issue: Номер выпуска
        first_page: Первая страница
        last_page: Последняя страница
        
    Returns:
        Сформированная литературная ссылка
    """
    # Обрезаем пробелы у всех полей
    journal = _clean_field(journal)
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
    
    # Добавляем volume и issue если они не пустые
    if volume:
        if issue:
            # С issue: "journal, volume (issue)"
            volume_issue = f"{volume} ({issue})"
            citation_parts.append(volume_issue)
        else:
            # Без issue: "journal, volume"
            citation_parts.append(volume)
    
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
        last_page_int < 100000 and 
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
        
        # Проверяем, что это число (может быть строкой вида "123")
        if re.match(r'^\d+$', cleaned):
            return int(cleaned)
        else:
            return None
    except (ValueError, TypeError):
        return None


def add_citation_column(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет колонку с литературными ссылками к DataFrame.
    
    Args:
        df: DataFrame с данными документов
        
    Returns:
        DataFrame с добавленной колонкой 'citation'
    """
    df_copy = df.copy()
    
    # Формируем ссылки для каждой строки
    citations = []
    for _, row in df_copy.iterrows():
        citation = format_citation(
            journal=row.get('journal'),
            volume=row.get('volume'),
            issue=row.get('issue'),
            first_page=row.get('first_page'),
            last_page=row.get('last_page')
        )
        citations.append(citation)
    
    df_copy['citation'] = citations
    return df_copy


__all__ = [
    "format_citation",
    "add_citation_column",
]
