Quick, repo-specific guidance for AI coding agents working on BioETL

Keep this short — focus on actionable patterns, commands and gotchas that
help an agent be productive right away.

- Project surface and big-picture
  - Core code lives under `src/bioetl/`. Primary subsystems: `pipelines/`,
    `clients/`, `config/`, `schemas/`, and `cli/`.
  - Pipelines are composition-based: see `src/bioetl/pipelines/` and
    `src/bioetl/chembl/common/descriptor.py`. New pipelines subclass
    `PipelineBase` / `UnifiedPipelineBase` and implement `build_descriptor`
    and transformation hooks (`pre_transform`, `transform`, `post_transform`).
  - Stage orchestration is implemented in `src/bioetl/pipelines/base.py`
    (look for `PipelineStagesProtocol`, `StageFactory` and `PipelineStageCommand`).

- Tests, tools and common developer flows
  - Install dev deps and project in editable mode: `pip install -e .[dev]`.
  - Recommended execution without relying on PowerShell activation on Windows:
    use the virtualenv python directly: `.\\.venv\\Scripts\\python -m pytest -q`.
  - Default pytest discovery and options are in `pyproject.toml` (see
    `[tool.pytest.ini_options]`) — tests run from `tests/bioetl` and
    `tests/golden`. Many CI flags (coverage, markers) are preset there.
  - If collection fails with "import file mismatch" check for duplicate
    basenames (e.g. multiple `test_common.py` under different test packages)
    or stale `.pyc`/`__pycache__` files. Running tests per-folder is a good
    diagnostic: `python -m pytest tests/bioetl -q` and `python -m pytest tests/schemas -q`.

- Conventions and patterns an agent should follow
  - Tests and source are added under `tests/bioetl/...` and `src/bioetl/...`.
  - Configs are typed YAML profiles under `configs/` — use `bioetl config inspect`
    to materialize the merged payload before coding against a setting.
  - CLI entrypoint: `bioetl` script (`[project.scripts]` in `pyproject.toml`) —
    prefer `python -m bioetl.cli.cli_app <command>` when experimenting.
  - Style & static checks are enforced: `ruff`, `black`, `isort`, `mypy`, `pylint`.
    Respect line-length = 100 and `src`/`tests` layout defined in `pyproject.toml`.

- Integration points & external dependencies
  - External APIs: ChEMBL, PubMed, Crossref, Semantic Scholar, IUPHAR —
    clients live in `src/bioetl/clients/`. Network calls are typically wrapped
    with retries (`backoff`) and tested with `pytest-httpserver`.
  - Secrets are never stored in repo; tests and CI expect Vault or env-vars
    (see `README.md` and `configs/templates/.env.key.template`).

- Useful file references (examples)
  - Pipeline contract & stage factory: `src/bioetl/pipelines/base.py`
  - ChEMBL pipeline architecture docs: `docs/pipelines/chembl/00-architecture.md`
  - Tests configuration & discovery: `pyproject.toml` and `pytest.ini`
  - CLI implementation and entrypoints: `src/bioetl/cli/cli_app.py`

- Quick troubleshooting recipes for agents
  - "Import file mismatch" during test collection: search for duplicate
    test filenames (e.g. `file_search "**/test_common.py"`) and rename to
    unique basenames; remove `__pycache__` and rerun tests.
  - PowerShell refuses to run `Activate.ps1`: do not change system policies;
    instead call the venv interpreter directly as shown above.
  - To run a single test function quickly:
    `python -m pytest tests/bioetl/path/to/test_file.py::test_name -q`.

If any section is unclear or you'd like the checklist extended (CI steps,
ADR links, or common refactors), say which area and I'll expand examples.
