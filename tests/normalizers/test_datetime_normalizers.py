"""
Тесты для нормализаторов данных даты и времени.
"""

import datetime
from src.library.normalizers.datetime_normalizers import (
    normalize_datetime_iso8601,
    normalize_datetime_validate,
    normalize_datetime_precision,
    normalize_date_only,
)


class TestDatetimeNormalizers:
    """Тесты для нормализаторов данных даты и времени."""
    
    def test_normalize_datetime_iso8601(self):
        """Тест нормализации datetime к ISO 8601."""
        # Тест с datetime объектом
        dt = datetime.datetime(2023, 12, 25, 15, 30, 45)
        result = normalize_datetime_iso8601(dt)
        assert result == "2023-12-25T15:30:45Z"
        
        # Тест с date объектом
        d = datetime.date(2023, 12, 25)
        result = normalize_datetime_iso8601(d)
        assert result == "2023-12-25T00:00:00Z"
        
        # Тест со строкой ISO 8601
        result = normalize_datetime_iso8601("2023-12-25T15:30:45Z")
        assert result == "2023-12-25T15:30:45Z"
        
        # Граничные случаи
        assert normalize_datetime_iso8601(None) is None
        assert normalize_datetime_iso8601("") is None
        assert normalize_datetime_iso8601("invalid") is None
    
    def test_normalize_datetime_validate(self):
        """Тест валидации datetime."""
        # Тест с валидными форматами
        assert normalize_datetime_validate("2023-12-25") == "2023-12-25T00:00:00Z"
        assert normalize_datetime_validate("25/12/2023") == "2023-12-25T00:00:00Z"
        assert normalize_datetime_validate("2023-12-25T15:30:45Z") == "2023-12-25T15:30:45Z"
        
        # Тест с datetime объектом
        dt = datetime.datetime(2023, 12, 25, 15, 30, 45)
        result = normalize_datetime_validate(dt)
        assert result == "2023-12-25T15:30:45Z"
        
        # Граничные случаи
        assert normalize_datetime_validate(None) is None
        assert normalize_datetime_validate("") is None
        assert normalize_datetime_validate("invalid") is None
    
    def test_normalize_datetime_precision(self):
        """Тест нормализации точности datetime."""
        # Тест с различной точностью
        dt_str = "2023-12-25T15:30:45.123456Z"
        
        result = normalize_datetime_precision(dt_str, "days")
        assert result == "2023-12-25T00:00:00Z"
        
        result = normalize_datetime_precision(dt_str, "hours")
        assert result == "2023-12-25T15:00:00Z"
        
        result = normalize_datetime_precision(dt_str, "minutes")
        assert result == "2023-12-25T15:30:00Z"
        
        result = normalize_datetime_precision(dt_str, "seconds")
        assert result == "2023-12-25T15:30:45Z"
        
        # Граничные случаи
        assert normalize_datetime_precision(None, "seconds") is None
        assert normalize_datetime_precision("invalid", "seconds") is None
    
    def test_normalize_date_only(self):
        """Тест нормализации только даты."""
        # Тест с различными форматами даты
        assert normalize_date_only("2023-12-25") == "2023-12-25"
        assert normalize_date_only("25/12/2023") == "2023-12-25"
        assert normalize_date_only("12/25/2023") == "2023-12-25"
        assert normalize_date_only("2023/12/25") == "2023-12-25"
        
        # Тест с datetime объектом
        dt = datetime.datetime(2023, 12, 25, 15, 30, 45)
        result = normalize_date_only(dt)
        assert result == "2023-12-25"
        
        # Тест с date объектом
        d = datetime.date(2023, 12, 25)
        result = normalize_date_only(d)
        assert result == "2023-12-25"
        
        # Граничные случаи
        assert normalize_date_only(None) is None
        assert normalize_date_only("") is None
        assert normalize_date_only("invalid") is None
