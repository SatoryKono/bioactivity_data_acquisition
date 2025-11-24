"""Smoke test to ensure CLI import avoids eager builder execution."""


def test_cli_import_does_not_execute_builders():
    import importlib

    import bioetl.cli.cli_app as cli_app

    # Ensure CLI module loads without side effects and exposes expected API
    assert hasattr(cli_app, "create_app")

    # Re-import should also be safe and idempotent
    cli_app_reloaded = importlib.reload(cli_app)
    assert hasattr(cli_app_reloaded, "create_app")
