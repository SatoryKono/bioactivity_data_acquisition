# Добавление нового XML источника

## Шаги для интеграции

### 1. Определить namespaces

Добавьте namespaces в `src/library/xml/namespaces.py`:

```python
# Новый источник
NEW_SOURCE_NS = {
    "ns": "http://example.com/namespace",
}
```

### 2. Создать клиент

Создайте новый клиент в `src/library/clients/`:

```python
from library.xml import make_xml_parser, select_one, select_many, text

class NewSourceClient(BaseApiClient):
    def parse_response(self, xml_content: str) -> dict:
        parser = make_xml_parser(recover=True)
        root = etree.fromstring(xml_content.encode('utf-8'), parser)
        
        # Извлечение данных
        data = {}
        self._extract_field1(root, data)
        self._extract_field2(root, data)
        return data
    
    def _extract_field1(self, root, data: dict) -> None:
        elem = select_one(root, './/field1')
        data["field1"] = text(elem, default="unknown")
```

### 3. Добавить XPath в каталог

Обновите `docs/refactor/xpath_catalog.md`:

```markdown
## New Source Fields

### Field1
- **XPath:** `.//field1/text()`
- **Описание:** Описание поля
- **Фолбэк:** `"unknown"`
```

### 4. Создать тесты

Добавьте тесты в `tests/clients/test_new_source.py`:

```python
def test_parse_response():
    client = NewSourceClient(config)
    result = client.parse_response(xml_fixture)
    assert result["field1"] == "expected_value"
```

### 5. Добавить фикстуры

Создайте XML фикстуры в `tests/parsing/fixtures/`:

- `new_source_valid.xml`
- `new_source_broken.xml`
- `new_source_missing_fields.xml`

## Best Practices

1. **Всегда используйте `make_xml_parser()`** для создания парсеров
2. **Используйте graceful degradation** с фолбэк-значениями
3. **Документируйте XPath** в каталоге
4. **Тестируйте сломанный XML** для проверки recover
5. **Логируйте ошибки парсинга** для отладки
