"""Factory for creating safe XML and HTML parsers with lxml."""

from lxml import etree


def make_xml_parser(
    *,
    recover: bool = True,
    ns_clean: bool = True,
    remove_blank_text: bool = True,
    resolve_entities: bool = False,
    load_dtd: bool = False,
    no_network: bool = True,
    encoding: str | None = None,
    huge_tree: bool = False,
) -> etree.XMLParser:
    """
    Создает безопасный XML парсер с опциями для отказоустойчивости.

    Безопасность:
    - no_network=True: блокирует сетевые обращения
    - resolve_entities=False: защита от XXE
    - load_dtd=False: не загружаем DTD по умолчанию

    Отказоустойчивость:
    - recover=True: пытается парсить "грязный" XML
    - ns_clean=True: чистит избыточные namespace декларации
    """
    return etree.XMLParser(
        recover=recover,
        ns_clean=ns_clean,
        remove_blank_text=remove_blank_text,
        resolve_entities=resolve_entities,
        load_dtd=load_dtd,
        no_network=no_network,
        encoding=encoding,
        huge_tree=huge_tree,
    )


def make_html_parser(*, encoding: str | None = None) -> etree.HTMLParser:
    """HTML парсер для "грязного" контента."""
    return etree.HTMLParser(
        encoding=encoding,
        remove_blank_text=True,
        no_network=True,
    )
