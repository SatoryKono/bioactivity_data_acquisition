"""Базовые компоненты для построения CLI-слоя."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, ClassVar, NoReturn, TypeAlias

import typer
from structlog.stdlib import BoundLogger

from bioetl.cli.tools._typer import TyperApp
from bioetl.cli.tools._typer import run_app as typer_run_app
from bioetl.core.logger import UnifiedLogger

CommandCallable: TypeAlias = Callable[..., None]


@dataclass(frozen=True)
class CliEntrypoint:
    """Унифицированный запуск Typer-приложения."""

    app: TyperApp

    def run(self) -> None:
        """Запустить приложение."""

        typer_run_app(self.app)

    @staticmethod
    def run_app(app: TyperApp) -> None:
        """Запустить произвольное Typer-приложение без создания объекта."""

        typer_run_app(app)


class CliCommandBase:
    """Базовый класс команд CLI с единым контуром обработки ошибок."""

    exit_code_error: ClassVar[int] = 1

    def __init__(self, *, logger: BoundLogger | None = None) -> None:
        self._logger = logger or UnifiedLogger.get(__name__)

    @property
    def logger(self) -> BoundLogger:
        """Вернуть привязанный логгер команды."""

        return self._logger

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        self.invoke(*args, **kwargs)

    def invoke(self, *args: Any, **kwargs: Any) -> None:
        """Выполнить команду с единым обработчиком ошибок."""

        try:
            self.handle(*args, **kwargs)
        except typer.Exit:
            raise
        except Exception as exc:  # noqa: BLE001 - управляем обработкой ниже
            self.handle_exception(exc)

    def handle(self, *args: Any, **kwargs: Any) -> None:
        """Реализация команды. Должна быть переопределена."""

        raise NotImplementedError

    def handle_exception(self, exc: Exception) -> NoReturn:
        """Обработать необработанное исключение и завершить процесс."""

        self.emit_error("E001", f"Unhandled CLI exception: {exc}")
        self.exit(self.exit_code_error)  # pragma: no cover - завершение процесса

    @staticmethod
    def emit_error(code: str, message: str) -> None:
        """Вывести сообщение об ошибке в единообразном формате."""

        typer.echo(f"[bioetl-cli] ERROR {code}: {message}", err=True)

    @staticmethod
    def exit(code: int) -> NoReturn:
        """Завершить выполнение команды с переданным кодом."""

        raise typer.Exit(code=code)

    @classmethod
    def build(cls, *init_args: Any, **init_kwargs: Any) -> CommandCallable:
        """Создать адаптер, совместимый с Typer."""

        def _command(*args: Any, **kwargs: Any) -> None:
            runner = cls(*init_args, **init_kwargs)
            runner.invoke(*args, **kwargs)

        return _command

