"""CLI command ``bioetl-determinism-check``."""

from __future__ import annotations

from typing import Any

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.cli.tools._logic import cli_determinism_check as cli_determinism_check_impl
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_CONFIG, CLI_ERROR_INTERNAL

_LOGIC_EXPORTS = getattr(cli_determinism_check_impl, "__all__", [])
globals().update(
    {symbol: getattr(cli_determinism_check_impl, symbol) for symbol in _LOGIC_EXPORTS}
)
__all__ = [*_LOGIC_EXPORTS, "app", "cli_main", "run"]  # pyright: ignore[reportUnsupportedDunderAll]

typer: Any = get_typer()


def cli_main(
    pipeline: list[str] | None = typer.Option(
        None,
        "--pipeline",
        "-p",
        help="Pipeline to verify (can be repeated). Defaults to activity_chembl and assay_chembl.",
    ),
) -> None:
    """Run determinism checks for the selected pipelines."""

    targets = tuple(pipeline) if pipeline else None
    results: dict[str, cli_determinism_check_impl.DeterminismRunResult]

    try:
        results = cli_determinism_check_impl.run_determinism_check(pipelines=targets)
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Determinism check failed: {exc}",
            context={
                "command": "bioetl-determinism-check",
                "exception_type": exc.__class__.__name__,
                "pipelines": targets,
            },
        )
        CliCommandBase.exit(1, cause=exc)

    if not results:
        CliCommandBase.emit_error(
            template=CLI_ERROR_CONFIG,
            message="No pipelines found for determinism check",
            context={
                "command": "bioetl-determinism-check",
                "pipelines": targets,
            },
        )
        CliCommandBase.exit(2)

    non_deterministic = [
        name for name, item in results.items() if not item.deterministic
    ]
    first_result = next(iter(results.values()))

    if non_deterministic:
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=(
                "Non-deterministic pipelines detected: "
                f"{', '.join(non_deterministic)}. "
                f"Report: {first_result.report_path.resolve()}"
            ),
            context={
                "command": "bioetl-determinism-check",
                "pipelines": tuple(non_deterministic),
                "report_path": str(first_result.report_path.resolve()),
            },
        )
        CliCommandBase.exit(3)

    typer.echo(
        "All inspected pipelines are deterministic. "
        f"Report: {first_result.report_path.resolve()}"
    )
    CliCommandBase.exit(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-determinism-check",
    help_text="Execute two runs and compare their logs",
    main_fn=cli_main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()