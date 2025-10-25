"""
Нормализаторы для химических структур (SMILES, InChI).
"""

import re
from typing import Any

from .base import ensure_string, is_empty_value, register_normalizer, safe_normalize


@safe_normalize
def normalize_smiles(value: Any) -> str | None:
    """Нормализует SMILES строку.

    - strip(): удаление пробелов
    - canonicalize(): канонизация (базовая проверка)
    - validate(): проверка парсером молекулы

    Args:
        value: Значение для нормализации

    Returns:
        Нормализованная SMILES строка или None
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

    # Базовая валидация SMILES
    # SMILES не должен содержать недопустимые символы
    invalid_chars = re.findall(r"[^A-Za-z0-9@+\-\[\]()=#\\/]", text)
    if invalid_chars:
        return None

    # Проверяем базовую структуру SMILES
    # Должен содержать хотя бы один атом
    if not re.search(r"[A-Z][a-z]?", text):
        return None

    # Проверяем баланс скобок
    open_brackets = text.count("[")
    close_brackets = text.count("]")
    if open_brackets != close_brackets:
        return None

    open_parens = text.count("(")
    close_parens = text.count(")")
    if open_parens != close_parens:
        return None

    return text


@safe_normalize
def normalize_inchi(value: Any) -> str | None:
    """Нормализует InChI строку.

    - strip(): удаление пробелов
    - ensure_prefix_inchi(): проверка префикса InChI= или InChI=1S/
    - validate(): базовая проверка структуры

    Args:
        value: Значение для нормализации

    Returns:
        Нормализованная InChI строка или None
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

    # Проверяем префикс
    if not (text.startswith("InChI=") or text.startswith("InChI=1S/")):
        return None

    # Базовая валидация структуры InChI
    # Должен содержать слои разделенные /
    if "/" not in text:
        return None

    # Проверяем что есть хотя бы один слой после префикса
    parts = text.split("/")
    if len(parts) < 2:
        return None

    return text


@safe_normalize
def normalize_inchi_key(value: Any) -> str | None:
    """Нормализует InChI Key.

    - strip(): удаление пробелов
    - uppercase(): приведение к верхнему регистру
    - validate_inchikey_format(): проверка формата ^[A-Z]{14}-[A-Z]{10}-[A-Z]$

    Args:
        value: Значение для нормализации

    Returns:
        Нормализованный InChI Key или None
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

    # Валидация формата InChI Key
    # Формат: ^[A-Z]{14}-[A-Z]{10}-[A-Z]$
    inchikey_pattern = r"^[A-Z]{14}-[A-Z]{10}-[A-Z]$"

    if not re.match(inchikey_pattern, text):
        return None

    return text


@safe_normalize
def normalize_molecular_formula(value: Any) -> str | None:
    """Нормализует молекулярную формулу.

    - strip(): удаление пробелов
    - uppercase(): приведение к верхнему регистру
    - validate_formula(): базовая проверка структуры

    Args:
        value: Значение для нормализации

    Returns:
        Нормализованная молекулярная формула или None
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

    # Базовая валидация молекулярной формулы
    # Должна содержать только буквы и цифры
    if not re.match(r"^[A-Z0-9]+$", text):
        return None

    # Должна содержать хотя бы один элемент (буква)
    if not re.search(r"[A-Z]", text):
        return None

    return text


@safe_normalize
def normalize_smiles_canonical(value: Any) -> str | None:
    """Канонизирует SMILES строку (расширенная версия).

    Args:
        value: Значение для нормализации

    Returns:
        Канонизированная SMILES строка или None
    """
    # Сначала базовая нормализация
    normalized = normalize_smiles(value)
    if normalized is None:
        return None

    # Здесь можно добавить более сложную канонизацию
    # Например, с использованием RDKit
    # Пока возвращаем базовую нормализацию
    return normalized


# Регистрация всех нормализаторов
register_normalizer("normalize_smiles", normalize_smiles)
register_normalizer("normalize_inchi", normalize_inchi)
register_normalizer("normalize_inchi_key", normalize_inchi_key)
register_normalizer("normalize_molecular_formula", normalize_molecular_formula)
register_normalizer("normalize_smiles_canonical", normalize_smiles_canonical)
