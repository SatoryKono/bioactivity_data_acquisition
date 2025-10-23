"""
Тесты для нормализаторов строковых данных.
"""

from src.library.normalizers.string_normalizers import (
    normalize_empty_to_null,
    normalize_string_lower,
    normalize_string_nfc,
    normalize_string_strip,
    normalize_string_titlecase,
    normalize_string_upper,
    normalize_string_whitespace,
)


class TestStringNormalizers:
    """Тесты для нормализаторов строк."""
    
    def test_normalize_string_strip(self):
        """Тест нормализации strip."""
        # Позитивные тесты
        assert normalize_string_strip("  hello  ") == "hello"
        assert normalize_string_strip("test") == "test"
        assert normalize_string_strip("") is None
        assert normalize_string_strip("   ") is None
        
        # Граничные случаи
        assert normalize_string_strip(None) is None
        assert normalize_string_strip(123) == "123"
        assert normalize_string_strip(0) == "0"
    
    def test_normalize_string_upper(self):
        """Тест нормализации uppercase."""
        # Позитивные тесты
        assert normalize_string_upper("hello") == "HELLO"
        assert normalize_string_upper("Test") == "TEST"
        assert normalize_string_upper("123") == "123"
        
        # Граничные случаи
        assert normalize_string_upper("") is None
        assert normalize_string_upper(None) is None
        assert normalize_string_upper("   ") == "   "
    
    def test_normalize_string_lower(self):
        """Тест нормализации lowercase."""
        # Позитивные тесты
        assert normalize_string_lower("HELLO") == "hello"
        assert normalize_string_lower("Test") == "test"
        assert normalize_string_lower("123") == "123"
        
        # Граничные случаи
        assert normalize_string_lower("") is None
        assert normalize_string_lower(None) is None
        assert normalize_string_lower("   ") == "   "
    
    def test_normalize_string_titlecase(self):
        """Тест нормализации titlecase."""
        # Позитивные тесты
        assert normalize_string_titlecase("hello world") == "Hello World"
        assert normalize_string_titlecase("test") == "Test"
        assert normalize_string_titlecase("123") == "123"
        
        # Граничные случаи
        assert normalize_string_titlecase("") is None
        assert normalize_string_titlecase(None) is None
        assert normalize_string_titlecase("   ") == "   "
    
    def test_normalize_string_nfc(self):
        """Тест Unicode NFC нормализации."""
        # Позитивные тесты
        assert normalize_string_nfc("café") == "café"
        assert normalize_string_nfc("test") == "test"
        
        # Граничные случаи
        assert normalize_string_nfc("") is None
        assert normalize_string_nfc(None) is None
    
    def test_normalize_string_whitespace(self):
        """Тест нормализации пробелов."""
        # Позитивные тесты
        assert normalize_string_whitespace("hello   world") == "hello world"
        assert normalize_string_whitespace("test\t\nvalue") == "test value"
        assert normalize_string_whitespace("  hello  world  ") == "hello world"
        
        # Граничные случаи
        assert normalize_string_whitespace("") is None
        assert normalize_string_whitespace(None) is None
        assert normalize_string_whitespace("   ") is None
    
    def test_normalize_empty_to_null(self):
        """Тест преобразования пустых строк в NULL."""
        # Позитивные тесты
        assert normalize_empty_to_null("hello") == "hello"
        assert normalize_empty_to_null("test") == "test"
        
        # Граничные случаи
        assert normalize_empty_to_null("") is None
        assert normalize_empty_to_null("   ") is None
        assert normalize_empty_to_null(None) is None
