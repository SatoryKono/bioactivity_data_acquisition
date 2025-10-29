"""Тесты для NumericNormalizer и BooleanNormalizer."""

import math
import pytest

from bioetl.normalizers.numeric import BooleanNormalizer, NumericNormalizer


class TestNumericNormalizer:
    """Тесты для NumericNormalizer."""

    def setup_method(self):
        """Настройка перед каждым тестом."""
        self.normalizer = NumericNormalizer()

    def test_normalize_int_valid_values(self):
        """Тест нормализации валидных целых чисел."""
        assert self.normalizer.normalize_int(42) == 42
        assert self.normalizer.normalize_int("123") == 123
        assert self.normalizer.normalize_int(0) == 0
        assert self.normalizer.normalize_int(-5) == -5

    def test_normalize_int_invalid_values(self):
        """Тест нормализации невалидных значений."""
        assert self.normalizer.normalize_int(None) is None
        assert self.normalizer.normalize_int("") is None
        assert self.normalizer.normalize_int("abc") is None
        assert self.normalizer.normalize_int(3.14) is None
        assert self.normalizer.normalize_int(math.nan) is None

    def test_normalize_float_valid_values(self):
        """Тест нормализации валидных float значений."""
        assert self.normalizer.normalize_float(3.14) == 3.14
        assert self.normalizer.normalize_float("2.5") == 2.5
        assert self.normalizer.normalize_float(0.0) == 0.0
        assert self.normalizer.normalize_float(-1.5) == -1.5

    def test_normalize_float_invalid_values(self):
        """Тест нормализации невалидных float значений."""
        assert self.normalizer.normalize_float(None) is None
        assert self.normalizer.normalize_float("") is None
        assert self.normalizer.normalize_float("abc") is None
        assert self.normalizer.normalize_float(math.nan) is None

    def test_normalize_bool_with_default(self):
        """Тест нормализации булевых значений с дефолтом."""
        # True значения
        assert self.normalizer.normalize_bool(True) is True
        assert self.normalizer.normalize_bool(1) is True
        assert self.normalizer.normalize_bool("true") is True
        assert self.normalizer.normalize_bool("1") is True
        assert self.normalizer.normalize_bool("yes") is True

        # False значения
        assert self.normalizer.normalize_bool(False) is False
        assert self.normalizer.normalize_bool(0) is False
        assert self.normalizer.normalize_bool("false") is False
        assert self.normalizer.normalize_bool("0") is False
        assert self.normalizer.normalize_bool("no") is False

        # NA значения с дефолтом
        assert self.normalizer.normalize_bool(None, default=True) is True
        assert self.normalizer.normalize_bool("", default=False) is False
        assert self.normalizer.normalize_bool("unknown", default=True) is True

    def test_validate(self):
        """Тест валидации значений."""
        assert self.normalizer.validate(42) is True
        assert self.normalizer.validate("123") is True
        assert self.normalizer.validate(3.14) is True
        assert self.normalizer.validate(None) is True
        assert self.normalizer.validate("abc") is False
        assert self.normalizer.validate(math.nan) is True

    def test_normalize_main(self):
        """Тест основной функции normalize (возвращает float)."""
        assert self.normalizer.normalize(42) == 42.0
        assert self.normalizer.normalize("3.14") == 3.14
        assert self.normalizer.normalize(None) is None
        assert self.normalizer.normalize("abc") is None


class TestBooleanNormalizer:
    """Тесты для BooleanNormalizer."""

    def setup_method(self):
        """Настройка перед каждым тестом."""
        self.normalizer = BooleanNormalizer()

    def test_normalize_valid_values(self):
        """Тест нормализации валидных булевых значений."""
        assert self.normalizer.normalize(True) is True
        assert self.normalizer.normalize(False) is False
        assert self.normalizer.normalize(1) is True
        assert self.normalizer.normalize(0) is False
        assert self.normalizer.normalize("true") is True
        assert self.normalizer.normalize("false") is False
        assert self.normalizer.normalize("1") is True
        assert self.normalizer.normalize("0") is False

    def test_normalize_na_values(self):
        """Тест нормализации NA значений."""
        assert self.normalizer.normalize(None) is None
        assert self.normalizer.normalize("") is None
        assert self.normalizer.normalize("na") is None
        assert self.normalizer.normalize("none") is None
        assert self.normalizer.normalize("null") is None

    def test_normalize_invalid_values(self):
        """Тест нормализации невалидных значений."""
        assert self.normalizer.normalize("unknown") is None
        assert self.normalizer.normalize("xyz") is None
        # Неинтегральные числа приводят к None
        assert self.normalizer.normalize(3.14) is None

    def test_normalize_with_default(self):
        """Тест нормализации с дефолтом."""
        assert self.normalizer.normalize_with_default(None, default=True) is True
        assert self.normalizer.normalize_with_default("", default=False) is False
        assert self.normalizer.normalize_with_default("unknown", default=True) is True
        assert self.normalizer.normalize_with_default("true", default=False) is True

    def test_validate(self):
        """Тест валидации значений."""
        assert self.normalizer.validate(True) is True
        assert self.normalizer.validate(False) is True
        assert self.normalizer.validate(1) is True
        assert self.normalizer.validate(0) is True
        assert self.normalizer.validate("true") is True
        assert self.normalizer.validate("false") is True
        assert self.normalizer.validate(None) is True
        assert self.normalizer.validate("unknown") is False
        # Неинтегральные числа считаются невалидными
        assert self.normalizer.validate(3.14) is False
