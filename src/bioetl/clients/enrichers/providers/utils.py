from __future__ import annotations

import warnings


def warn_on_provider_module_move(current_module: str) -> None:
    """Выводит предупреждение о переносе модуля в пространство providers.

    Старые импорты вида ``bioetl.clients.enrichers.<module>`` продолжают
    работать, но сигнализируются как устаревшие.
    """

    if current_module.startswith("bioetl.clients.enrichers.") and ".providers." not in current_module:
        module = current_module.split(".")[-1]
        warnings.warn(
            (
                f"Модуль 'bioetl.clients.enrichers.{module}' перемещён в "
                f"'bioetl.clients.enrichers.providers.{module}'"
            ),
            DeprecationWarning,
            stacklevel=2,
        )


__all__ = ["warn_on_provider_module_move"]
