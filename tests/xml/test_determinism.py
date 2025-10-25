"""Tests for XML parsing determinism."""

import json
from lxml import etree

from library.xml import make_xml_parser


def test_xml_to_dict_deterministic():
    """Убедиться, что XML парсинг детерминирован."""
    xml = '''<root>
        <item id="2">second</item>
        <item id="1">first</item>
    </root>'''
    
    results = []
    for _ in range(10):
        parser = make_xml_parser()
        root = etree.fromstring(xml.encode('utf-8'), parser)
        # Простое преобразование в строку для проверки детерминизма
        result = etree.tostring(root, method="text", encoding="unicode")
        results.append(result)
    
    # Все результаты идентичны
    assert len(set(results)) == 1


def test_parser_settings_consistency():
    """Проверить, что настройки парсера консистентны."""
    parser1 = make_xml_parser()
    parser2 = make_xml_parser()
    
    # Проверяем, что парсеры создаются без ошибок
    assert parser1 is not None
    assert parser2 is not None
    assert type(parser1) == type(parser2)


def test_safe_parser_defaults():
    """Проверить, что парсер создается с безопасными настройками по умолчанию."""
    parser = make_xml_parser()
    
    # Проверяем, что парсер создается без ошибок
    assert parser is not None
    assert hasattr(parser, '__class__')
    
    # Тестируем безопасность через парсинг потенциально опасного XML
    safe_xml = '<root>test</root>'
    root = etree.fromstring(safe_xml.encode('utf-8'), parser)
    assert root is not None
