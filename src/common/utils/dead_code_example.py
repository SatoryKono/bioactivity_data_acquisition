"""Учебные примеры для скрипта очистки мёртвого кода."""


def maybe_unused():
    """Функция-устаревший кандидат B."""
    return "legacy"


def legacy_stub():
    """Функция-устаревший кандидат C."""
    return "legacy placeholder"
