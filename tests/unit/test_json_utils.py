"""Тесты для JSON утилит."""

import json
import math

import pytest

from bioetl.utils.json import canonical_json, normalize_json_list


class TestCanonicalJson:
    """Тесты для canonical_json."""

    def test_basic_functionality(self):
        """Тест базовой функциональности."""
        data = {"b": 2, "a": 1, "c": 3}
        result = canonical_json(data)
        assert result is not None
        parsed = json.loads(result)
        assert parsed["a"] == 1
        assert parsed["b"] == 2
        assert parsed["c"] == 3

    def test_nested_structures(self):
        """Тест вложенных структур."""
        data = {
            "nested": {"z": 3, "x": 1, "y": 2},
            "list": [3, 1, 2],
            "mixed": {"a": [2, 1], "b": {"c": 3, "a": 1}}
        }
        result = canonical_json(data)
        assert result is not None
        parsed = json.loads(result)
        assert parsed["nested"]["x"] == 1
        assert parsed["list"] == [3, 1, 2]

    def test_sorting(self):
        """Тест сортировки ключей."""
        data = {"c": 3, "a": 1, "b": 2}
        result = canonical_json(data)
        assert result is not None
        # Ключи должны быть отсортированы
        parsed = json.loads(result)
        keys = list(parsed.keys())
        assert keys == ["a", "b", "c"]

    def test_sort_keys_false(self):
        """Тест с отключенной сортировкой ключей."""
        data = {"c": 3, "a": 1, "b": 2}
        result = canonical_json(data, sort_keys=False)
        assert result == '{"c": 3, "a": 1, "b": 2}'

    def test_ensure_ascii_false(self):
        """Тест с отключенным ensure_ascii."""
        data = {"русский": "текст", "emoji": "🚀"}
        result = canonical_json(data, ensure_ascii=False)
        assert "русский" in result
        assert "🚀" in result

    def test_invalid_values(self):
        """Тест невалидных значений."""
        # Функции, которые не сериализуются в JSON
        def test_func():
            pass

        class TestClass:
            pass

        assert canonical_json(test_func) is None
        assert canonical_json(TestClass()) is None

    def test_deterministic_output(self):
        """Тест детерминированности вывода."""
        data = {"b": 2, "a": 1, "c": 3}
        result1 = canonical_json(data)
        result2 = canonical_json(data)
        assert result1 == result2


class TestNormalizeJsonList:
    """Тесты для normalize_json_list."""

    def test_basic_normalization(self):
        """Тест базовой нормализации."""
        data = [{"name": "test", "value": "42"}]
        canonical, records = normalize_json_list(data)

        assert canonical is not None
        assert len(records) == 1
        assert records[0]["name"] == "test"
        assert records[0]["value"] == 42

    def test_multiple_records(self):
        """Тест множественных записей."""
        data = [
            {"name": "test1", "value": "42"},
            {"name": "test2", "value": "24"}
        ]
        canonical, records = normalize_json_list(data)

        assert canonical is not None
        assert len(records) == 2
        assert records[0]["name"] == "test1"
        assert records[1]["name"] == "test2"

    def test_string_to_number_conversion(self):
        """Тест преобразования строк в числа."""
        data = [{"name": "test", "value": "42", "float_val": "3.14", "invalid": "abc"}]
        canonical, records = normalize_json_list(data)

        assert canonical is not None
        assert len(records) == 1
        # Строки должны быть нормализованы
        assert records[0]["name"] == "test"
        assert records[0]["value"] == 42  # Числовая строка должна стать числом
        assert records[0]["float_val"] == 3.14
        assert records[0]["invalid"] == "abc"  # Нечисловая строка остается строкой

    def test_none_handling(self):
        """Тест обработки None значений."""
        data = [{"name": None, "value": None, "other": None}]
        canonical, records = normalize_json_list(data)

        assert canonical is not None
        assert len(records) == 1
        assert records[0]["name"] is None
        assert records[0]["value"] is None
        assert records[0]["other"] is None

    def test_float_nan_handling(self):
        """Тест обработки NaN значений."""
        import math
        data = [{"value": math.nan, "normal": 42}]
        canonical, records = normalize_json_list(data)

        assert len(records) == 1
        assert records[0]["value"] is None
        assert records[0]["normal"] == 42

    def test_sorting(self):
        """Тест сортировки записей."""
        data = [
            {"value": 3, "name": "c"},
            {"value": 1, "name": "a"},
            {"value": 2, "name": "b"}
        ]

        canonical, records = normalize_json_list(data)
        assert len(records) == 3
        # Записи должны быть отсортированы по name
        assert records[0]["name"] == "a"
        assert records[1]["name"] == "b"
        assert records[2]["name"] == "c"

    def test_custom_sorting(self):
        """Тест пользовательской сортировки."""
        data = [
            {"value": 3, "name": "c"},
            {"value": 1, "name": "a"},
            {"value": 2, "name": "b"}
        ]

        def sort_by_value(record):
            return record.get("value", 0)

        canonical, records = normalize_json_list(data, sort_fn=sort_by_value)
        assert len(records) == 3
        # Записи должны быть отсортированы по value
        assert records[0]["value"] == 1
        assert records[1]["value"] == 2
        assert records[2]["value"] == 3

    def test_empty_string_keys(self):
        """Тест пустых строковых ключей."""
        data = [{"": None, "name": "test"}]
        canonical, records = normalize_json_list(data)

        assert canonical is not None
        assert len(records) == 2
        assert records[0][""] is None  # Пустая строка становится None
        assert records[1]["name"] == "test"

    def test_deterministic_output(self):
        """Тест детерминированности вывода."""
        data = [{"b": 2, "a": 1}, {"d": 4, "c": 3}]

        canonical1, records1 = normalize_json_list(data)
        canonical2, records2 = normalize_json_list(data)

        # Результаты должны быть идентичными
        assert canonical1 == canonical2
        assert records1 == records2

        # Канонический JSON должен быть детерминированным
        parsed1 = json.loads(canonical1)
        parsed2 = json.loads(canonical2)
        assert parsed1 == parsed2
    @pytest.mark.parametrize("raw", [None, "", "   ", "NA", " n/a ", math.nan])
    def test_na_inputs_return_empty_payload(self, raw):
        """NA inputs should short-circuit to empty canonical payload."""

        canonical, records = normalize_json_list(raw)

        assert canonical is None
        assert records == []

