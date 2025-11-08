# Инвентарь валидаторов и нормализаторов

| Функция | Тип | Использование | Заметки |
| --- | --- | --- | --- |
| `bioetl.core.validators._is_json_mapping_internal` | validator | bioetl.core.validators | — |
| `bioetl.core.validators._is_json_value` | validator | bioetl.core.validators | — |
| `bioetl.core.validators.assert_iterable` | validator | tests.bioetl.core.test_validators | Убедиться, что аргумент итерируем; иначе выбросить :class:`TypeError`. |
| `bioetl.core.validators.assert_json_mapping` | validator | tests.bioetl.core.test_validators | Убедиться, что объект — корректный JSON-подобный словарь. |
| `bioetl.core.validators.assert_list_of` | validator | bioetl.etl.vocab_store<br>tests.bioetl.core.test_validators | Убедиться, что аргумент — список, элементы которого проходят проверку. |
| `bioetl.core.validators.is_iterable` | validator | bioetl.config.loader<br>bioetl.core.validators<br>tests.bioetl.core.test_validators | Проверить, что объект итерируем, опираясь на вызов ``iter(obj)``. |
| `bioetl.core.validators.is_json_mapping` | validator | bioetl.core.validators<br>tests.bioetl.core.test_validators | Проверить, что объект — JSON-подобный словарь с ключами-строками. |
| `bioetl.core.validators.is_list_of` | validator | tests.bioetl.core.test_validators | Проверить, что объект — список, чьи элементы удовлетворяют предикату. |
| `bioetl.schemas.activity.activity_chembl._is_valid_activity_properties` | validator | bioetl.schemas.activity.activity_chembl | Element-wise validator ensuring activity_properties stores normalized JSON arrays. |
| `bioetl.schemas.activity.activity_chembl._is_valid_activity_property_item` | validator | bioetl.schemas.activity.activity_chembl | Return True if the payload item only contains the allowed keys and value types. |
| `bioetl.schemas.load_meta._coerce_timestamp` | normalizer | bioetl.schemas.load_meta | — |
| `bioetl.schemas.load_meta._is_valid_json_string` | validator | bioetl.schemas.load_meta | — |
| `bioetl.schemas.load_meta._validate_json_series` | validator | bioetl.schemas.load_meta | — |
| `bioetl.schemas.load_meta._validate_optional_json_series` | validator | bioetl.schemas.load_meta | — |
