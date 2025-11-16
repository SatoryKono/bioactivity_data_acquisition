"""CLI command ``bioetl-vocab-audit``."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from bioetl.devtools.typer_helpers import TyperApp, get_typer, register_tool_app
from bioetl.devtools import cli_vocab_audit as cli_vocab_audit_impl
from bioetl.clients.client_exceptions import HTTPError, Timeout
from bioetl.core.http.api_client import CircuitBreakerOpenError
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_EXTERNAL_API, CLI_ERROR_INTERNAL
from bioetl.core.runtime.errors import BioETLError

_LOGIC_EXPORTS = getattr(cli_vocab_audit_impl, "__all__", [])
globals().update({symbol: getattr(cli_vocab_audit_impl, symbol) for symbol in _LOGIC_EXPORTS})
VocabAuditResult = getattr(cli_vocab_audit_impl, "VocabAuditResult")
audit_vocabularies = getattr(cli_vocab_audit_impl, "audit_vocabularies")
__all__ = [
    * _LOGIC_EXPORTS,
    "VocabAuditResult",
    "audit_vocabularies",
    "app",
    "cli_main",
    "run",
]  # pyright: ignore[reportUnsupportedDunderAll]

if TYPE_CHECKING:
    from bioetl.devtools.cli_vocab_audit import VocabAuditResult as VocabAuditResultType
else:
    VocabAuditResultType = Any

typer: Any = get_typer()
DEFAULT_OUTPUT = cli_vocab_audit_impl.DEFAULT_OUTPUT
DEFAULT_META = cli_vocab_audit_impl.DEFAULT_META


def cli_main(
    store: Path | None = typer.Option(
        None,
        "--store",
        help="Directory or YAML file containing ChEMBL vocabularies.",
        exists=True,
        file_okay=True,
        dir_okay=True,
        readable=True,
    ),
    output: Path = typer.Option(
        DEFAULT_OUTPUT,
        "--output",
        help="Path to the CSV report with vocabulary audit results.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
    meta: Path = typer.Option(
        DEFAULT_META,
        "--meta",
        help="Path to the audit meta.yaml file.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
    pages: int = typer.Option(
        10,
        "--pages",
        help="Number of ChEMBL API pages to sample values from.",
    ),
    page_size: int = typer.Option(
        1000,
        "--page-size",
        help="Page size to use for ChEMBL API pagination.",
    ),
) -> None:
    """Run the vocabulary audit pipeline."""

    result: VocabAuditResultType

    try:
        result = audit_vocabularies(
            store=store.resolve() if store else None,
            output=output,
            meta=meta,
            pages=pages,
            page_size=page_size,
        )
    except typer.Exit:
        raise
    except (BioETLError, CircuitBreakerOpenError, HTTPError, Timeout) as exc:
        CliCommandBase.emit_error(
            template=CLI_ERROR_EXTERNAL_API,
            message=f"Vocabulary audit failed due to external API error: {exc}",
            context={
                "command": "bioetl-vocab-audit",
                "exception_type": exc.__class__.__name__,
                "store": str(store.resolve()) if store else None,
                "output": str(output.resolve()) if hasattr(output, "resolve") else str(output),
                "meta": str(meta.resolve()) if hasattr(meta, "resolve") else str(meta),
                "pages": pages,
                "page_size": page_size,
            },
            exit_code=1,
            cause=exc,
        )
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Vocabulary audit failed: {exc}",
            context={
                "command": "bioetl-vocab-audit",
                "exception_type": exc.__class__.__name__,
                "store": str(store.resolve()) if store else None,
                "output": str(output.resolve()) if hasattr(output, "resolve") else str(output),
                "meta": str(meta.resolve()) if hasattr(meta, "resolve") else str(meta),
                "pages": pages,
                "page_size": page_size,
            },
            cause=exc,
        )

    typer.echo(
        "Vocabulary audit completed: "
        f"{len(result.rows)} records, CSV {result.output.resolve()}, meta {result.meta.resolve()}"
    )
    CliCommandBase.exit(0)


app: TyperApp
run: Callable[[], None]
app, run = register_tool_app(
    name="bioetl-vocab-audit",
    help_text="Audit ChEMBL vocabularies and generate a report",
    main_fn=cli_main,
)


if __name__ == "__main__":
    run()
