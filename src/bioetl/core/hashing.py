"""Детерминированное хеширование для пайплайнов.

Модуль предоставляет функции для генерации SHA256 хешей с гарантированным
детерминизмом и канонической сериализацией.
"""

import hashlib
import json
from typing import Any

import pandas as pd


def generate_hash_row(row: dict[str, Any]) -> str:
    """Сгенерировать SHA256 хеш от канонической строки row.

    Правила канонизации:
    - JSON с sort_keys=True
    - ISO8601 для datetime
    - %.6f для float (6 знаков после запятой)
    - исключаем None/null из сериализации

    Args:
        row: Словарь с данными строки

    Returns:
        SHA256 хеш в hex формате (64 символа)

    Пример:
        >>> row = {"id": 1, "value": 3.141592653589793, "name": "test"}
        >>> hash_value = generate_hash_row(row)
        >>> len(hash_value)
        64
    """
    # Канонизация: JSON с sort_keys=True
    # Убираем None значения для согласованности
    canonical_dict = {k: _canonicalize_value(v) for k, v in row.items() if v is not None}
    
    json_str = json.dumps(canonical_dict, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(json_str.encode("utf-8")).hexdigest()


def generate_hash_business_key(key_value: str | int) -> str:
    """Сгенерировать SHA256 хеш от бизнес-ключа.

    Используется для создания стабильного идентификатора записи.

    Args:
        key_value: Значение бизнес-ключа (CHEMBL ID, activity_id и т.д.)

    Returns:
        SHA256 хеш в hex формате (64 символа)

    Пример:
        >>> hash_value = generate_hash_business_key("CHEMBL123")
        >>> len(hash_value)
        64
    """
    key_str = str(key_value).encode("utf-8")
    return hashlib.sha256(key_str).hexdigest()


def _canonicalize_value(value: Any) -> Any:
    """Канонизировать значение для сериализации.

    - datetime → ISO8601 string
    - float → %.6f
    - pandas Timestamp → ISO8601 string

    Args:
        value: Значение для канонизации

    Returns:
        Канонизированное значение
    """
    # pandas Timestamp или datetime
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except (AttributeError, TypeError):
            pass
    
    # float → 6 знаков после запятой
    if isinstance(value, float):
        return round(value, 6)
    
    return value

