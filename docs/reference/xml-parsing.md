# XML Parsing with lxml

## Создание парсера

```python
from library.xml import make_xml_parser
from lxml import etree

parser = make_xml_parser(recover=True)  # Для "грязного" XML
root = etree.fromstring(xml_text.encode('utf-8'), parser)
```

## XPath запросы

```python
from library.xml import select_one, select_many, text

# Один элемент с фолбэком
doi_elem = select_one(root, './/ArticleId[@IdType="doi"]', default=None)
doi_value = text(doi_elem, default="unknown")

# Множество элементов
abstract_elems = select_many(root, './/AbstractText')
abstract_parts = [text(elem) for elem in abstract_elems]
```

## Namespaces

```python
from library.xml import PUBMED_NS

# С namespaces
elems = select_many(root, './/pubmed:Article', namespaces=PUBMED_NS)
```

## Безопасность

Все парсеры создаются с безопасными настройками по умолчанию:

- `no_network=True` — блокирует сетевые обращения
- `resolve_entities=False` — защита от XXE атак
- `load_dtd=False` — не загружает DTD

## Graceful Degradation

Все функции возвращают фолбэк-значения вместо исключений:

```python
# Если элемент не найден, возвращается default
value = text(select_one(root, './/missing', default="fallback"))
```

## HTML парсинг

```python
from library.xml import make_html_parser, parse_html_document

# Для "грязного" HTML
html_root = parse_html_document(html_content)
```
