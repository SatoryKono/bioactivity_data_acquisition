# Local Test Utilities

This directory contains helper scripts for running the project's automated test
suite on a developer workstation. The shell and PowerShell entry points provide
consistent invocation flags so that linting, type checking, and the full pytest
suite run in a deterministic order.

* `run_tests.sh` – Unix entry point that orchestrates Ruff, mypy, and pytest.
* `run_tests.ps1` – PowerShell equivalent for Windows developers.

Both scripts expect to be executed from the repository root:

```bash
./scripts/local/run_tests.sh
```

On Windows PowerShell:

```powershell
./scripts/local/run_tests.ps1
```

The scripts honour the project's virtual environment if one is active. See
`README.md` in the repository root for further onboarding instructions.
