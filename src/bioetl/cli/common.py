"""Совместимые вспомогательные функции общего CLI-раннера."""

from __future__ import annotations

from collections.abc import Callable

from bioetl.cli.runner import run as _run
from bioetl.cli.runner import runner_factory as _runner_factory

__all__ = ["run", "runner_factory"]


def run(fn: Callable[[], int | None], *, setup_logging: bool = True) -> int:
    """Запустить CLI-команду через общий раннер.

    Делегирует выполнение в :func:`bioetl.cli.runner.run`, сохраняя исторический
    контракт импорта ``bioetl.cli.common.run``.
    """

    return _run(fn, setup_logging=setup_logging)


def runner_factory(
    fn: Callable[[], int | None], *, setup_logging: bool = True
) -> Callable[[], None]:
    """Создать обёртку для запуска CLI-команды.

    Делегирует в :func:`bioetl.cli.runner.runner_factory` и возвращает функцию,
    которая вызывает ``SystemExit`` при ненулевом коде выхода команды.
    """

    return _runner_factory(fn, setup_logging=setup_logging)


