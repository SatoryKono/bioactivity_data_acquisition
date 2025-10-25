"""HTML parsing support for "dirty" HTML content."""

from lxml import etree

from .parser_factory import make_html_parser


def parse_html_document(html_content: str, encoding: str | None = None) -> etree._Element:
    """
    Парсит HTML документ с помощью lxml HTMLParser.

    Args:
        html_content: HTML контент для парсинга
        encoding: Кодировка (опционально)

    Returns:
        Корневой элемент HTML дерева
    """
    parser = make_html_parser(encoding=encoding)
    return etree.HTML(html_content, parser=parser)


def clean_html_text(html_element: etree._Element) -> str:
    """
    Извлекает чистый текст из HTML элемента, удаляя все теги.

    Args:
        html_element: HTML элемент

    Returns:
        Чистый текст без HTML тегов
    """
    return etree.tostring(html_element, method="text", encoding="unicode").strip()
