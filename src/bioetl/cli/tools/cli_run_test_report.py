"""CLI command ``bioetl-run-test-report``."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping

from bioetl.cli._io import atomic_write_yaml
from bioetl.cli.cli_entrypoint import TyperApp, get_typer, register_tool_app
from bioetl.cli.tools._logic import cli_run_test_report as cli_run_test_report_impl
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL

TEST_REPORTS_ROOT = cli_run_test_report_impl.TEST_REPORTS_ROOT
_blake2_digest = cli_run_test_report_impl._blake2_digest
_load_pytest_summary = cli_run_test_report_impl._load_pytest_summary
_compute_config_hash = cli_run_test_report_impl._compute_config_hash
_compute_pipeline_version = cli_run_test_report_impl._compute_pipeline_version
_read_git_commit = cli_run_test_report_impl._read_git_commit
UnifiedLogger = cli_run_test_report_impl.UnifiedLogger


def _write_yaml_atomic(path: Path, payload: Mapping[str, object]) -> None:
    """Compatibility wrapper delegating to the shared atomic YAML helper."""

    atomic_write_yaml(payload, path)
resolve_artifact_paths = cli_run_test_report_impl.resolve_artifact_paths
build_timestamp_directory_name = cli_run_test_report_impl.build_timestamp_directory_name
datetime = cli_run_test_report_impl.datetime
timezone = cli_run_test_report_impl.timezone
uuid4 = cli_run_test_report_impl.uuid4
subprocess = cli_run_test_report_impl.subprocess
REPO_ROOT = cli_run_test_report_impl.REPO_ROOT


def generate_test_report(output_root: Path | None = None) -> int:
    """Wrapper forwarding dependencies to the logic implementation."""

    return cli_run_test_report_impl.generate_test_report(
        output_root=output_root,
        subprocess_module=subprocess,
        logger_cls=UnifiedLogger,
        datetime_module=datetime,
        uuid4_fn=uuid4,
        resolve_artifacts_fn=resolve_artifact_paths,
        repo_root=REPO_ROOT,
        pipeline_version_fn=_compute_pipeline_version,
        git_commit_fn=_read_git_commit,
        config_hash_fn=_compute_config_hash,
    )

__all__ = (
    "TEST_REPORTS_ROOT",
    "generate_test_report",
    "_blake2_digest",
    "_load_pytest_summary",
    "_compute_config_hash",
    "_compute_pipeline_version",
    "_read_git_commit",
    "_write_yaml_atomic",
    "UnifiedLogger",
    "resolve_artifact_paths",
    "build_timestamp_directory_name",
    "datetime",
    "timezone",
    "uuid4",
    "subprocess",
    "REPO_ROOT",
    "app",
    "cli_main",
    "run",
)

typer: Any = get_typer()


def cli_main(
    output_root: Path = typer.Option(
        TEST_REPORTS_ROOT,
        "--output-root",
        help="Directory where pytest and coverage artifacts will be stored.",
        exists=False,
        file_okay=False,
        dir_okay=True,
        writable=True,
    ),
) -> None:
    """Run pytest and build the combined report."""

    output_root_path = output_root.resolve()
    exit_code: int
    try:
        exit_code = generate_test_report(output_root=output_root_path)
    except typer.Exit:
        raise
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Test report generation failed: {exc}",
            context={
                "command": "bioetl-run-test-report",
                "output_root": str(output_root_path),
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

    if exit_code == 0:
        typer.echo("Test report generated successfully")
        CliCommandBase.exit(0)

    typer.secho(
        f"pytest exited with code {exit_code}",
        err=True,
        fg=typer.colors.RED,
    )
    CliCommandBase.emit_error(
        template=CLI_ERROR_INTERNAL,
        message=f"pytest exited with code {exit_code}",
        context={
            "command": "bioetl-run-test-report",
            "output_root": str(output_root_path),
            "pytest_exit_code": exit_code,
        },
        exit_code=exit_code,
    )


app: TyperApp
run: Callable[[], None]
app, run = register_tool_app(
    name="bioetl-run-test-report",
    help_text="Generate pytest and coverage reports with metadata",
    main_fn=cli_main,
)


if __name__ == "__main__":
    run()
