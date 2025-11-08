"""Общий раннер для Typer-команд BioETL."""

from __future__ import annotations

from collections.abc import Callable
from typing import Final

import typer

from bioetl.core.logger import LoggerConfig, UnifiedLogger

__all__ = ["run", "runner_factory"]

_DEFAULT_LOG_LEVEL: Final[str] = "INFO"
_INTERRUPTED_EXIT_CODE: Final[int] = 130
_USAGE_ERROR_EXIT_CODE: Final[int] = 2
_RUNTIME_ERROR_EXIT_CODE: Final[int] = 1


def _ensure_logging() -> None:
    """Инициализировать структурное логирование с настройками по умолчанию."""

    UnifiedLogger.configure(LoggerConfig(level=_DEFAULT_LOG_LEVEL))
    UnifiedLogger.bind(
        component="cli",
        pipeline="cli",
        stage="entrypoint",
        dataset="n/a",
        run_id="cli",
        trace_id="cli",
        span_id="cli",
    )


def run(fn: Callable[[], int | None], *, setup_logging: bool = True) -> int:
    """Выполнить CLI-функцию с унифицированной обработкой исключений.

    Parameters
    ----------
    fn:
        Вызываемый объект, реализующий команду. Может вернуть код выхода
        или None (будет интерпретирован как успех).
    setup_logging:
        Если истина, выполняет настройку ``UnifiedLogger`` перед запуском.

    Returns
    -------
    int
        Код выхода, согласованный с ``docs/cli/02-cli-exit_codes.md``.
    """

    if setup_logging:
        _ensure_logging()

    log = UnifiedLogger.get(__name__)

    try:
        result = fn()
        exit_code = 0 if result is None else int(result)
        log.info("cli_runner_completed", exit_code=exit_code)
        return exit_code

    except typer.Exit as exc:
        exit_code_attr = getattr(exc, "exit_code", None)
        exit_code = int(exit_code_attr) if exit_code_attr is not None else 0
        log.info("cli_runner_typer_exit", exit_code=exit_code)
        return exit_code

    except KeyboardInterrupt:
        log.warning("cli_runner_interrupted")
        return _INTERRUPTED_EXIT_CODE

    except ValueError as exc:
        log.error("cli_runner_usage_error", error=str(exc), exc_info=True)
        return _USAGE_ERROR_EXIT_CODE

    except RuntimeError as exc:
        log.error("cli_runner_runtime_error", error=str(exc), exc_info=True)
        return _RUNTIME_ERROR_EXIT_CODE

    except SystemExit as exc:
        exit_code = exc.code if isinstance(exc.code, int) else 0
        log.info("cli_runner_system_exit", exit_code=exit_code)
        return exit_code

    except Exception as exc:  # pragma: no cover - страховка на крайние случаи
        log.error("cli_runner_unhandled_exception", error=str(exc), exc_info=True)
        return _RUNTIME_ERROR_EXIT_CODE

    finally:
        UnifiedLogger.reset()


def runner_factory(
    fn: Callable[[], int | None], *, setup_logging: bool = True
) -> Callable[[], None]:
    """Создать обёртку для запуска CLI-функции через общий раннер.

    Parameters
    ----------
    fn:
        Вызываемый объект CLI, обычно ``typer.Typer`` или функция-обработчик.
    setup_logging:
        Если истина, включает настройку логирования перед запуском.

    Returns
    -------
    Callable[[], None]
        Функция без аргументов, завершающая процесс при ненулевом коде выхода.
    """

    def _runner() -> None:
        exit_code = run(fn, setup_logging=setup_logging)
        if exit_code != 0:
            raise SystemExit(exit_code)

    return _runner

