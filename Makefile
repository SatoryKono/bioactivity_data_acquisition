.PHONY: dicts.aggregate format lint qa typecheck test lint.ruff deadcode

dicts.aggregate:
	python scripts/build_vocab_store.py --output artifacts/chembl_dictionaries.yaml

format:
	ruff check --fix src tests scripts
	isort --settings-path pyproject.toml src tests scripts
	black --config pyproject.toml src tests scripts

lint: lint.ruff
	isort --settings-path pyproject.toml --check-only src tests scripts
	black --config pyproject.toml --check src tests scripts

lint.ruff:
	ruff check src tests scripts

typecheck:
	mypy --config-file pyproject.toml src/bioetl src/scripts

test:
	pytest --maxfail=1 --disable-warnings -vv tests

deadcode:
	vulture src tests scripts --min-confidence 80

qa: lint typecheck test
