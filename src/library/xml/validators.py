"""Optional XSD/RelaxNG validation utilities."""

from lxml import etree

from .exceptions import XMLValidationError


def validate_with_xmlschema(tree: etree._ElementTree, xsd_path: str) -> bool:
    """
    Валидирует XML дерево против XSD схемы.

    Args:
        tree: XML дерево для валидации
        xsd_path: Путь к XSD файлу

    Returns:
        True если валидация прошла успешно

    Raises:
        XMLValidationError: Если валидация не прошла
    """
    try:
        schema = etree.XMLSchema(file=xsd_path)
        return schema.validate(tree)
    except etree.XMLSchemaError as e:
        raise XMLValidationError(f"XSD validation failed: {e}") from e
    except Exception as e:
        raise XMLValidationError(f"Validation error: {e}") from e


def validate_with_relaxng(tree: etree._ElementTree, rng_path: str) -> bool:
    """
    Валидирует XML дерево против RelaxNG схемы.

    Args:
        tree: XML дерево для валидации
        rng_path: Путь к RelaxNG файлу

    Returns:
        True если валидация прошла успешно

    Raises:
        XMLValidationError: Если валидация не прошла
    """
    try:
        schema = etree.RelaxNG(file=rng_path)
        return schema.validate(tree)
    except etree.RelaxNGError as e:
        raise XMLValidationError(f"RelaxNG validation failed: {e}") from e
    except Exception as e:
        raise XMLValidationError(f"Validation error: {e}") from e
