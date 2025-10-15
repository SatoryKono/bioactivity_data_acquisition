"""Утилита для нормализации названий журналов."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

import pandas as pd


def normalize_journal_name(text: Any) -> str | None:
    """Нормализует название журнала согласно установленным правилам.
    
    Args:
        text: Исходное название журнала
        
    Returns:
        Нормализованное название журнала или None для пустых значений
    """
    if pd.isna(text) or text is None:
        return None
    
    # Преобразуем в строку и обрезаем пробелы
    text = str(text).strip()
    if not text:
        return None
    
    # 1. Unicode нормализация: NFC, затем casefold
    text = unicodedata.normalize('NFC', text)
    text = text.casefold()
    
    # 2. Декорации: снимаем внешние кавычки и завершающую пунктуацию
    text = _remove_decorations(text)
    
    # 3. Заменяем & и + на " and "
    text = re.sub(r'[&+]', ' and ', text)
    
    # 4. Убираем точки в сокращениях
    text = re.sub(r'\b([a-z])\.\s*', r'\1 ', text)
    
    # 4.1. Заменяем распространенные сокращения на полные слова
    abbreviations = {
        r'\bj\b': 'journal',
        r'\bchem\b': 'chemical',
        r'\bphys\b': 'physics',
        r'\bbiol\b': 'biology',
        r'\bmath\b': 'mathematics',
        r'\beng\b': 'engineering',
        r'\bmed\b': 'medicine',
        r'\btech\b': 'technology',
        r'\bsci\b': 'science',
        r'\bres\b': 'research',
        r'\bappl\b': 'applied',
        r'\btheor\b': 'theoretical',
        r'\bexp\b': 'experimental',
        r'\bint\b': 'international',
        r'\bproc\b': 'proceedings',
        r'\btrans\b': 'transactions',
        r'\bcomm\b': 'communications',
        r'\blett\b': 'letters',
        r'\brev\b': 'reviews',
        r'\brep\b': 'reports',
        r'\bbull\b': 'bulletin',
        r'\bann\b': 'annals',
        r'\bmag\b': 'magazine',
        r'\bnews\b': 'newsletter',
        r'\bnot\b': 'notes'
    }
    
    for pattern, replacement in abbreviations.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    
    # 5. Пробелы и дефисы: схлопываем множественные пробелы
    text = re.sub(r'\s+', ' ', text)
    # Дефисы окружаем пробелами, затем снова схлопываем
    text = re.sub(r'-', ' - ', text)
    text = re.sub(r'\s+', ' ', text)
    
    # 6. Диакритика: NFKD и удаляем комбинируемые диакритические знаки
    text = unicodedata.normalize('NFKD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    
    # 7. Артикли и лидирующие служебные слова
    text = _remove_leading_articles(text)
    
    # 8. Нормализация типовых слов
    text = _normalize_journal_words(text)
    
    # 9. Предлоги и соединители
    text = _normalize_prepositions(text)
    
    # 10. Римские цифры
    text = _convert_roman_numerals(text)
    
    # 11. Итоговая форма: только [a-z0-9], пробел и дефис
    text = re.sub(r'[^a-z0-9\s-]', '', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\s*-\s*', '-', text)  # Убираем пробелы вокруг дефисов
    text = text.strip()
    
    return text if text else None


def _remove_decorations(text: str) -> str:
    """Удаляет внешние кавычки и завершающую пунктуацию."""
    # Убираем внешние кавычки
    text = re.sub(r'^["\']+|["\']+$', '', text)
    
    # Убираем завершающую пунктуацию
    text = re.sub(r'[.,;:)\]}\s]*$', '', text)
    
    return text.strip()


def _remove_leading_articles(text: str) -> str:
    """Удаляет артикли и служебные слова в начале строки."""
    articles = [
        r'^the\s+',
        r'^a\s+',
        r'^an\s+',
        r'^le\s+',
        r'^la\s+',
        r'^les\s+',
        r'^el\s+',
        r'^los\s+',
        r'^las\s+'
    ]
    
    for pattern in articles:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    
    return text.strip()


def _normalize_journal_words(text: str) -> str:
    """Нормализует типовые слова журналов."""
    # Словарь замен
    replacements = {
        r'\bjournals\b': 'journal',
        r'\btransactions\b': 'transactions',
        r'\bproceedings\b': 'proceedings',
        r'\bletters\b': 'letters',
        r'\breports\b': 'reports',
        r'\bbulletin\b': 'bulletin',
        r'\bcommunications\b': 'communications',
        r'\breviews\b': 'reviews',
        r'\bannals\b': 'annals',
        r'\bmagazine\b': 'magazine',
        r'\bnewsletter\b': 'newsletter',
        r'\bnotes\b': 'notes'
    }
    
    for pattern, replacement in replacements.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    
    return text


def _normalize_prepositions(text: str) -> str:
    """Унифицирует предлоги и соединители."""
    prepositions = {
        r'\bde\b': 'of',
        r'\bdi\b': 'of',
        r'\bder\b': 'of',
        r'\bfür\b': 'of',
        r'\bfor\b': 'of'
    }
    
    for pattern, replacement in prepositions.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    
    return text


def _convert_roman_numerals(text: str) -> str:
    """Конвертирует римские цифры в арабские."""
    # Словарь римских цифр
    roman_to_arabic = {
        'i': '1', 'ii': '2', 'iii': '3', 'iv': '4', 'v': '5',
        'vi': '6', 'vii': '7', 'viii': '8', 'ix': '9', 'x': '10',
        'xi': '11', 'xii': '12', 'xiii': '13', 'xiv': '14', 'xv': '15',
        'xvi': '16', 'xvii': '17', 'xviii': '18', 'xix': '19', 'xx': '20'
    }
    
    # Ищем римские цифры в конце слов
    for roman, arabic in roman_to_arabic.items():
        # Паттерн для римских цифр в конце слова или фразы
        pattern = r'\b' + roman + r'\b'
        text = re.sub(pattern, arabic, text, flags=re.IGNORECASE)
    
    return text


def normalize_journal_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Нормализует все колонки с названиями журналов в DataFrame.
    
    Находит колонки, содержащие 'journal' или 'журнал' в названии (case-insensitive)
    и нормализует их значения.
    
    Args:
        df: DataFrame с данными документов
        
    Returns:
        DataFrame с нормализованными колонками журналов
    """
    df_copy = df.copy()
    
    # Находим колонки с 'journal' или 'журнал' в названии
    journal_columns = []
    for col in df_copy.columns:
        col_lower = col.lower()
        if 'journal' in col_lower or 'журнал' in col_lower:
            journal_columns.append(col)
    
    print(f"Найдено колонок журналов для нормализации: {journal_columns}")
    
    # Нормализуем каждую колонку
    for col in journal_columns:
        if col in df_copy.columns:
            print(f"Нормализация колонки: {col}")
            df_copy[col] = df_copy[col].apply(normalize_journal_name)
    
    return df_copy


def get_journal_columns(df: pd.DataFrame) -> list[str]:
    """Возвращает список колонок с названиями журналов.
    
    Args:
        df: DataFrame для анализа
        
    Returns:
        Список названий колонок, содержащих 'journal' или 'журнал'
    """
    journal_columns = []
    for col in df.columns:
        col_lower = col.lower()
        if 'journal' in col_lower or 'журнал' in col_lower:
            journal_columns.append(col)
    
    return journal_columns


__all__ = [
    "normalize_journal_name",
    "normalize_journal_columns", 
    "get_journal_columns",
]
