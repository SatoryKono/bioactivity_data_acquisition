"""Тесты для JSON утилит."""

import json

from bioetl.utils.json import canonical_json, normalize_json_list


class TestCanonicalJson:
    """Тесты для canonical_json."""

    def test_simple_values(self):
        """Тест простых значений."""
        assert canonical_json(None) is None
        assert canonical_json("") is None
        assert canonical_json("test") == '"test"'
        assert canonical_json(42) == "42"
        assert canonical_json(3.14) == "3.14"
        assert canonical_json(True) == "true"
        assert canonical_json(False) == "false"

    def test_dict_sorting(self):
        """Тест сортировки ключей в словаре."""
        data = {"c": 3, "a": 1, "b": 2}
        result = canonical_json(data)
        expected = '{"a":1,"b":2,"c":3}'
        assert result == expected

    def test_nested_structures(self):
        """Тест вложенных структур."""
        data = {
            "outer": {
                "inner": [3, 1, 2],
                "other": "test"
            }
        }
        result = canonical_json(data)
        # Проверяем, что результат детерминирован
        parsed = json.loads(result)
        assert parsed["outer"]["inner"] == [3, 1, 2]
        assert parsed["outer"]["other"] == "test"

    def test_list_sorting(self):
        """Тест сортировки списков."""
        data = [{"b": 2, "a": 1}, {"d": 4, "c": 3}]
        result = canonical_json(data)
        # Проверяем, что результат детерминирован
        parsed = json.loads(result)
        assert len(parsed) == 2
        assert parsed[0]["a"] == 1
        assert parsed[0]["b"] == 2

    def test_invalid_values(self):
        """Тест невалидных значений."""
        # Функции, которые не сериализуются в JSON
        def test_func():
            pass

        class TestClass:
            pass

        assert canonical_json(test_func) is None
        assert canonical_json(TestClass()) is None

    def test_sort_keys_false(self):
        """Тест с отключенной сортировкой ключей."""
        data = {"c": 3, "a": 1, "b": 2}
        result = canonical_json(data, sort_keys=False)
        # Результат может быть не отсортирован
        parsed = json.loads(result)
        assert parsed["a"] == 1
        assert parsed["b"] == 2
        assert parsed["c"] == 3


class TestNormalizeJsonList:
    """Тесты для normalize_json_list."""

    def test_empty_values(self):
        """Тест пустых значений."""
        assert normalize_json_list(None) == (None, [])
        assert normalize_json_list("") == (None, [])
        assert normalize_json_list("   ") == (None, [])
        assert normalize_json_list("na") == (None, [])

    def test_string_json(self):
        """Тест парсинга JSON строки."""
        json_str = '[{"name": "test", "value": 42}]'
        canonical, records = normalize_json_list(json_str)
        
        assert canonical is not None
        assert len(records) == 1
        assert records[0]["name"] == "test"
        assert records[0]["value"] == 42

    def test_dict_to_list(self):
        """Тест преобразования словаря в список."""
        data = {"name": "test", "value": 42}
        canonical, records = normalize_json_list(data)
        
        assert canonical is not None
        assert len(records) == 1
        assert records[0]["name"] == "test"
        assert records[0]["value"] == 42

    def test_already_list(self):
        """Тест уже списка."""
        data = [{"name": "test1", "value": 42}, {"name": "test2", "value": 24}]
        canonical, records = normalize_json_list(data)
        
        assert canonical is not None
        assert len(records) == 2
        assert records[0]["name"] == "test1"
        assert records[1]["name"] == "test2"

    def test_string_normalization(self):
        """Тест нормализации строк."""
        data = [{"name": "  test  ", "value": "  42  "}]
        canonical, records = normalize_json_list(data)
        
        assert len(records) == 1
        # Строки должны быть нормализованы
        assert records[0]["name"] == "test"
        assert records[0]["value"] == 42  # Числовая строка должна стать числом

    def test_numeric_parsing(self):
        """Тест парсинга числовых значений."""
        data = [{"value": "42", "float_val": "3.14", "invalid": "abc"}]
        canonical, records = normalize_json_list(data)
        
        assert len(records) == 1
        assert records[0]["value"] == 42
        assert records[0]["float_val"] == 3.14
        assert records[0]["invalid"] == "abc"  # Нечисловая строка остается строкой

    def test_na_values(self):
        """Тест обработки NA значений."""
        data = [{"name": "", "value": "na", "other": None}]
        canonical, records = normalize_json_list(data)
        
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
            {"name": "zebra", "value": 3},
            {"name": "apple", "value": 1},
            {"name": "banana", "value": 2}
        ]
        canonical, records = normalize_json_list(data)
        
        assert len(records) == 3
        # Записи должны быть отсортированы по name
        assert records[0]["name"] == "apple"
        assert records[1]["name"] == "banana"
        assert records[2]["name"] == "zebra"

    def test_custom_sort_function(self):
        """Тест пользовательской функции сортировки."""
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

    def test_invalid_json_string(self):
        """Тест невалидной JSON строки."""
        canonical, records = normalize_json_list('{"invalid": json}')
        assert canonical is None
        assert records == []

    def test_non_dict_entries(self):
        """Тест записей, которые не являются словарями."""
        data = [{"name": "test"}, "invalid", {"name": "test2"}]
        canonical, records = normalize_json_list(data)
        
        assert len(records) == 2  # Только словари должны быть включены
        assert records[0]["name"] == "test"
        assert records[1]["name"] == "test2"

    def test_empty_records(self):
        """Тест пустых записей."""
        data = [{"": ""}, {}, {"name": "test"}]
        canonical, records = normalize_json_list(data)
        
        # Записи с пустыми ключами и пустые записи не исключаются
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
