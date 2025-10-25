"""Tests for XML selectors with graceful degradation."""

from lxml import etree

from library.xml import attr, select_many, select_one, text


def test_select_one_with_default():
    """Test select_one with existing and missing elements."""
    xml = '<root><item>value</item></root>'
    root = etree.fromstring(xml)
    
    # Существующий элемент
    item_elem = select_one(root, './/item')
    assert item_elem is not None
    assert text(item_elem) == "value"
    
    # Отсутствующий элемент с фолбэком
    missing_elem = select_one(root, './/missing', default=None)
    assert missing_elem is None
    
    # Отсутствующий элемент с кастомным фолбэком
    fallback_elem = select_one(root, './/missing', default="fallback")
    assert fallback_elem == "fallback"


def test_select_many_with_namespaces():
    """Test select_many with namespaces."""
    xml = '''<root xmlns:ns="http://example.com">
        <ns:item>one</ns:item>
        <ns:item>two</ns:item>
    </root>'''
    root = etree.fromstring(xml)
    ns = {"ns": "http://example.com"}
    
    items = select_many(root, './/ns:item', namespaces=ns)
    assert len(items) == 2
    assert [text(i) for i in items] == ["one", "two"]


def test_text_extraction():
    """Test text extraction with various scenarios."""
    xml = '<root><item>  value  </item><empty></empty></root>'
    root = etree.fromstring(xml)
    
    # Нормальный текст с strip
    item_elem = select_one(root, './/item')
    assert text(item_elem) == "value"
    
    # Текст без strip
    assert text(item_elem, strip=False) == "  value  "
    
    # Пустой элемент
    empty_elem = select_one(root, './/empty')
    assert text(empty_elem) == ""
    
    # None элемент
    assert text(None) == ""
    assert text(None, default="fallback") == "fallback"


def test_attr_extraction():
    """Test attribute extraction."""
    xml = '<root><item id="123" class="test">value</item></root>'
    root = etree.fromstring(xml)
    
    item_elem = select_one(root, './/item')
    
    # Существующий атрибут
    assert attr(item_elem, "id") == "123"
    assert attr(item_elem, "class") == "test"
    
    # Отсутствующий атрибут
    assert attr(item_elem, "missing") == ""
    assert attr(item_elem, "missing", default="fallback") == "fallback"
    
    # None элемент
    assert attr(None, "id") == ""
    assert attr(None, "id", default="fallback") == "fallback"


def test_xpath_with_attributes():
    """Test XPath queries with attribute selectors."""
    xml = '''<root>
        <item type="doi">10.1234/example</item>
        <item type="pmid">12345</item>
        <item type="doi">10.5678/another</item>
    </root>'''
    root = etree.fromstring(xml)
    
    # Выбор по атрибуту
    doi_items = select_many(root, './/item[@type="doi"]')
    assert len(doi_items) == 2
    assert [text(item) for item in doi_items] == ["10.1234/example", "10.5678/another"]
    
    # Один элемент по атрибуту
    pmid_item = select_one(root, './/item[@type="pmid"]')
    assert text(pmid_item) == "12345"


def test_graceful_degradation():
    """Test graceful degradation with malformed XML."""
    from library.xml import make_xml_parser
    
    # XML с незакрытыми тегами (recover=True должен справиться)
    malformed_xml = '<root><item>value<item>another</root>'
    parser = make_xml_parser(recover=True)
    root = etree.fromstring(malformed_xml.encode('utf-8'), parser)
    
    # Должен найти элементы несмотря на ошибки
    items = select_many(root, './/item')
    assert len(items) >= 1  # Может найти один или больше в зависимости от recover
