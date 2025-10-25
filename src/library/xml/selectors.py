"""XPath selectors with graceful degradation and fallbacks."""

from typing import Any

from lxml import etree


def select_one(
    root: etree._Element,
    xpath: str,
    *,
    namespaces: dict[str, str] | None = None,
    default: Any = None,
) -> etree._Element | Any:
    """Выбирает один элемент по XPath с фолбэком."""
    result = root.xpath(xpath, namespaces=namespaces)
    if result and isinstance(result, list):
        return result[0]
    return default


def select_many(
    root: etree._Element,
    xpath: str,
    *,
    namespaces: dict[str, str] | None = None,
) -> list[etree._Element]:
    """Выбирает множество элементов по XPath."""
    result = root.xpath(xpath, namespaces=namespaces)
    return result if isinstance(result, list) else []


def text(
    element: etree._Element | None,
    default: str = "",
    strip: bool = True,
) -> str:
    """Извлекает текст элемента с фолбэком."""
    if element is None:
        return default
    text_val = element.text
    if text_val is None:
        return default
    return text_val.strip() if strip else text_val


def attr(
    element: etree._Element | None,
    attr_name: str,
    default: str = "",
) -> str:
    """Извлекает атрибут с фолбэком."""
    if element is None:
        return default
    return element.get(attr_name, default)
