# Unified Makefile для bioactivity-data-acquisition
# Единый интерфейс для всех пайплайнов и операций

.PHONY: help setup-api-keys clean-backups test test-unit test-integration test-e2e run-dev install-dev
.PHONY: pipeline pipeline-test pipeline-clean health analyze-iuphar
.PHONY: fmt lint type-check qa
.PHONY: docs-serve docs-build docs-lint docs-deploy
.PHONY: clean

# Переменные по умолчанию
PYTHON := python
CONFIG_DIR := configs
DATA_DIR := data
INPUT_DIR := $(DATA_DIR)/input
OUTPUT_DIR := $(DATA_DIR)/output
DATE_TAG := $(shell date +%Y%m%d)

# Цвета для вывода
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
BLUE := \033[0;34m
NC := \033[0m # No Color

# Показать справку
help:
	@echo "$(BLUE)Bioactivity Data Acquisition - Unified Interface$(NC)"
	@echo ""
	@echo "$(GREEN)Pipeline Commands:$(NC)"
	@echo "  make pipeline ENTITY=<documents|targets|assays|activities|testitems> [CONFIG=...] [INPUT=...] [FLAGS=\"...\"]"
	@echo "  make pipeline-test TYPE=<...> [MARKERS=\"slow\"]"
	@echo "  make pipeline-clean TYPE=<...>"
	@echo ""
	@echo "$(GREEN)Health & Monitoring:$(NC)"
	@echo "  make health CONFIG=..."
	@echo ""
	@echo "$(GREEN)Analysis:$(NC)"
	@echo "  make analyze-iuphar TARGET_CSV=... [IUPHAR_DICT=...] [SAMPLE_SIZE=...] [TARGET_ID=...] [OUTPUT_FORMAT=...] [VERBOSE=true]"
	@echo ""
	@echo "$(GREEN)Code Quality:$(NC)"
	@echo "  make fmt          - Format code"
	@echo "  make lint         - Lint code"
	@echo "  make type-check   - Type checking"
	@echo "  make qa           - Full quality check"
	@echo ""
	@echo "$(GREEN)Documentation:$(NC)"
	@echo "  make docs-serve   - Serve documentation locally"
	@echo "  make docs-build   - Build documentation"
	@echo "  make docs-lint    - Lint documentation"
	@echo "  make docs-deploy  - Deploy documentation"
	@echo ""
	@echo "$(GREEN)Testing:$(NC)"
	@echo "  make test         - Run all tests"
	@echo "  make test-unit    - Run unit tests"
	@echo "  make test-integration - Run integration tests"
	@echo "  make test-e2e     - Run e2e tests"
	@echo ""
	@echo "$(GREEN)Utilities:$(NC)"
	@echo "  make clean        - Clean temporary files"
	@echo "  make install-dev  - Install in development mode"
	@echo ""
	@echo "$(GREEN)Examples:$(NC)"
	@echo "  make pipeline ENTITY=documents CONFIG=configs/config_documents_full.yaml"
	@echo "  make pipeline ENTITY=targets INPUT=data/input/target.csv CONFIG=configs/config_target_full.yaml"
	@echo "  make pipeline ENTITY=activities CONFIG=configs/config_activity_full.yaml"
	@echo "  make test-unit"
	@echo "  make health CONFIG=configs/config_documents_full.yaml"
	@echo "  make analyze-iuphar TARGET_CSV=data/output/target/target_20251021.csv VERBOSE=true"

# =============================================================================
# UNIFIED PIPELINE COMMAND
# =============================================================================

# Универсальная команда для запуска любых пайплайнов
pipeline:
	@if [ -z "$(ENTITY)" ]; then \
		echo "$(RED)Error: ENTITY is required. Use: make pipeline ENTITY=<documents|targets|assays|activities|testitems> [CONFIG=...]$(NC)"; \
		exit 1; \
	fi
	@echo "$(BLUE)Running $(ENTITY) pipeline...$(NC)"
	@$(MAKE) pipeline-$(ENTITY) CONFIG=$(CONFIG) INPUT=$(INPUT) FLAGS="$(FLAGS)"

# =============================================================================
# PIPELINE IMPLEMENTATIONS
# =============================================================================

# Documents pipeline
pipeline-documents:
	@echo "$(BLUE)Running documents pipeline...$(NC)"
	@mkdir -p $(OUTPUT_DIR)/documents
	@$(PYTHON) -m library.cli get-document-data \
		--config $(or $(CONFIG),$(CONFIG_DIR)/config_documents_full.yaml) \
		$(if $(INPUT),--input $(INPUT),) \
		$(if $(FLAGS),$(FLAGS),) \
		--log-level INFO
	@echo "$(GREEN)Documents pipeline completed!$(NC)"

# Targets pipeline
pipeline-targets:
	@echo "$(BLUE)Running targets pipeline...$(NC)"
	@mkdir -p $(OUTPUT_DIR)/target
	@$(PYTHON) -m library.cli get-target-data \
		--config $(or $(CONFIG),$(CONFIG_DIR)/config_target_full.yaml) \
		--input $(or $(INPUT),$(INPUT_DIR)/target.csv) \
		$(if $(FLAGS),$(FLAGS),) \
		--log-level INFO
	@echo "$(GREEN)Targets pipeline completed!$(NC)"

# Assays pipeline
pipeline-assays:
	@echo "$(BLUE)Running assays pipeline...$(NC)"
	@mkdir -p $(OUTPUT_DIR)/assay
	@$(PYTHON) -m library.cli get-assay-data \
		--config $(or $(CONFIG),$(CONFIG_DIR)/config_assay_full.yaml) \
		--input $(or $(INPUT),$(INPUT_DIR)/assay.csv) \
		$(if $(FLAGS),$(FLAGS),) \
		--log-level INFO
	@echo "$(GREEN)Assays pipeline completed!$(NC)"

# Activities pipeline
pipeline-activities:
	@echo "$(BLUE)Running activities pipeline...$(NC)"
	@mkdir -p $(OUTPUT_DIR)/activity
	@$(PYTHON) -m library.cli get-activity-data \
		--config $(or $(CONFIG),$(CONFIG_DIR)/config_activity_full.yaml) \
		--input $(or $(INPUT),$(INPUT_DIR)/activity.csv) \
		$(if $(FLAGS),$(FLAGS),) \
		--log-level INFO
	@echo "$(GREEN)Activities pipeline completed!$(NC)"

# Testitems pipeline
pipeline-testitems:
	@echo "$(BLUE)Running testitems pipeline...$(NC)"
	@mkdir -p $(OUTPUT_DIR)/testitem
	@$(PYTHON) -m library.cli testitem-run \
		--config $(or $(CONFIG),$(CONFIG_DIR)/config_testitem_full.yaml) \
		--input $(or $(INPUT),$(INPUT_DIR)/testitem.csv) \
		$(if $(FLAGS),$(FLAGS),) \
		--verbose
	@echo "$(GREEN)Testitems pipeline completed!$(NC)"


# =============================================================================
# PIPELINE TESTING
# =============================================================================

# Универсальная команда для тестирования пайплайнов
pipeline-test:
	@if [ -z "$(TYPE)" ]; then \
		echo "$(RED)Error: TYPE is required. Use: make pipeline-test TYPE=<documents|targets|assays|activities|testitems>$(NC)"; \
		exit 1; \
	fi
	@echo "$(BLUE)Running $(TYPE) pipeline tests...$(NC)"
	@$(MAKE) pipeline-test-$(TYPE) MARKERS="$(MARKERS)"

# Documents pipeline tests
pipeline-test-documents:
	@echo "$(BLUE)Running documents pipeline tests...$(NC)"
	@pytest tests/test_document_*.py $(if $(MARKERS),-m "$(MARKERS)",) -v
	@echo "$(GREEN)Documents pipeline tests completed!$(NC)"

# Targets pipeline tests
pipeline-test-targets:
	@echo "$(BLUE)Running targets pipeline tests...$(NC)"
	@pytest tests/test_target_*.py $(if $(MARKERS),-m "$(MARKERS)",) -v
	@echo "$(GREEN)Targets pipeline tests completed!$(NC)"

# Assays pipeline tests
pipeline-test-assays:
	@echo "$(BLUE)Running assays pipeline tests...$(NC)"
	@pytest tests/test_assay_*.py $(if $(MARKERS),-m "$(MARKERS)",) -v
	@echo "$(GREEN)Assays pipeline tests completed!$(NC)"

# Activities pipeline tests
pipeline-test-activities:
	@echo "$(BLUE)Running activities pipeline tests...$(NC)"
	@pytest tests/test_activity_*.py $(if $(MARKERS),-m "$(MARKERS)",) -v
	@echo "$(GREEN)Activities pipeline tests completed!$(NC)"

# Testitems pipeline tests
pipeline-test-testitems:
	@echo "$(BLUE)Running testitems pipeline tests...$(NC)"
	@pytest tests/test_testitem_*.py $(if $(MARKERS),-m "$(MARKERS)",) -v
	@echo "$(GREEN)Testitems pipeline tests completed!$(NC)"

# =============================================================================
# PIPELINE CLEANUP
# =============================================================================

# Универсальная команда для очистки пайплайнов
pipeline-clean:
	@if [ -z "$(TYPE)" ]; then \
		echo "$(RED)Error: TYPE is required. Use: make pipeline-clean TYPE=<documents|targets|assays|activities|testitems>$(NC)"; \
		exit 1; \
	fi
	@echo "$(BLUE)Cleaning $(TYPE) pipeline outputs...$(NC)"
	@$(MAKE) pipeline-clean-$(TYPE)

# Documents pipeline cleanup
pipeline-clean-documents:
	@echo "$(YELLOW)Cleaning documents pipeline outputs...$(NC)"
	@rm -rf $(OUTPUT_DIR)/documents/*
	@rm -rf $(OUTPUT_DIR)/_documents/*
	@echo "$(GREEN)Documents pipeline outputs cleaned!$(NC)"

# Targets pipeline cleanup
pipeline-clean-targets:
	@echo "$(YELLOW)Cleaning targets pipeline outputs...$(NC)"
	@rm -rf $(OUTPUT_DIR)/target/*
	@rm -rf $(OUTPUT_DIR)/target_correlation_report_*
	@echo "$(GREEN)Targets pipeline outputs cleaned!$(NC)"

# Assays pipeline cleanup
pipeline-clean-assays:
	@echo "$(YELLOW)Cleaning assays pipeline outputs...$(NC)"
	@rm -rf $(OUTPUT_DIR)/assay/*
	@echo "$(GREEN)Assays pipeline outputs cleaned!$(NC)"

# Activities pipeline cleanup
pipeline-clean-activities:
	@echo "$(YELLOW)Cleaning activities pipeline outputs...$(NC)"
	@rm -rf $(OUTPUT_DIR)/activity/*
	@echo "$(GREEN)Activities pipeline outputs cleaned!$(NC)"

# Testitems pipeline cleanup
pipeline-clean-testitems:
	@echo "$(YELLOW)Cleaning testitems pipeline outputs...$(NC)"
	@rm -rf $(OUTPUT_DIR)/testitem/*
	@echo "$(GREEN)Testitems pipeline outputs cleaned!$(NC)"

# =============================================================================
# HEALTH & MONITORING
# =============================================================================

# Health check command
health:
	@if [ -z "$(CONFIG)" ]; then \
		echo "$(RED)Error: CONFIG is required. Use: make health CONFIG=path/to/config.yaml$(NC)"; \
		exit 1; \
	fi
	@echo "$(BLUE)Checking API health...$(NC)"
	@$(PYTHON) -m library.cli health --config $(CONFIG)
	@echo "$(GREEN)Health check completed!$(NC)"

# Analysis commands
analyze-iuphar:
	@if [ -z "$(TARGET_CSV)" ]; then \
		echo "$(RED)Error: TARGET_CSV is required. Use: make analyze-iuphar TARGET_CSV=path/to/target.csv$(NC)"; \
		exit 1; \
	fi
	@echo "$(BLUE)Analyzing IUPHAR mapping...$(NC)"
	@$(PYTHON) -m library.cli analyze-iuphar-mapping \
		--target-csv $(TARGET_CSV) \
		$(if $(IUPHAR_DICT),--iuphar-dict $(IUPHAR_DICT),) \
		$(if $(SAMPLE_SIZE),--sample-size $(SAMPLE_SIZE),) \
		$(if $(TARGET_ID),--target-id $(TARGET_ID),) \
		$(if $(OUTPUT_FORMAT),--format $(OUTPUT_FORMAT),) \
		$(if $(VERBOSE),--verbose,)
	@echo "$(GREEN)IUPHAR analysis completed!$(NC)"

# =============================================================================
# CODE QUALITY
# =============================================================================

# Format code
fmt:
	@echo "$(BLUE)Formatting code...$(NC)"
	@black src/ tests/
	@ruff check src/ tests/ --fix
	@echo "$(GREEN)Code formatting completed!$(NC)"

# Lint code
lint:
	@echo "$(BLUE)Linting code...$(NC)"
	@ruff check src/ tests/
	@echo "$(GREEN)Code linting completed!$(NC)"

# Type checking
type-check:
	@echo "$(BLUE)Type checking...$(NC)"
	@mypy src/
	@echo "$(GREEN)Type checking completed!$(NC)"

# Full quality check
qa: fmt lint type-check tidy-root
	@echo "$(GREEN)Full quality check with root cleanup completed!$(NC)"

# =============================================================================
# DOCUMENTATION
# =============================================================================

# Serve documentation locally
docs-serve:
	@echo "$(BLUE)Serving documentation locally...$(NC)"
	@mkdocs serve --config-file $(CONFIG_DIR)/mkdocs.yml

# Build documentation
docs-build:
	@echo "$(BLUE)Building documentation...$(NC)"
	@mkdocs build --config-file $(CONFIG_DIR)/mkdocs.yml --strict
	@echo "$(GREEN)Documentation build completed!$(NC)"

# Lint documentation
docs-lint:
	@echo "$(BLUE)Linting documentation...$(NC)"
	@markdownlint docs/ --config .markdownlint.json
	@pymarkdown scan docs/
	@echo "$(GREEN)Documentation linting completed!$(NC)"

# Deploy documentation
docs-deploy:
	@echo "$(BLUE)Deploying documentation...$(NC)"
	@mkdocs gh-deploy --config-file $(CONFIG_DIR)/mkdocs.yml --force
	@echo "$(GREEN)Documentation deployment completed!$(NC)"

# =============================================================================
# ROOT CLEANUP & QUALITY
# =============================================================================

# Check root directory cleanliness
tidy-root:
	@echo "$(BLUE)Running root cleanup checks...$(NC)"
	@mkdir -p metadata/reports
	@echo "$(YELLOW)1. Checking root directory structure...$(NC)"
	@python -c "import sys; from pathlib import Path; allowed_files={'README.md','Makefile','pyproject.toml','mkdocs.yml','Dockerfile','docker-compose.yml','pyrightconfig.json','.pre-commit-config.yaml','.gitignore','.markdownlint.json','.python-version','LICENSE','CHANGELOG.md','.bandit','.banditignore','.coverage','.dockerignore','.env.example','.gitattributes','.markdown-link-check.json','.pre-commit-hooks.yaml','.pymarkdown.json','.safety_policy.yaml'}; allowed_dirs={'src','tests','configs','scripts','docs','data','metadata','logs','.git','.github','.vscode','.idea','venv','.venv','env','node_modules','__pycache__','build','dist','site','.pytest_cache','.mypy_cache','.ruff_cache','.cursor'}; root=Path('.'); violations=[]; [violations.append(f'Unwanted: {item.name}') for item in root.iterdir() if (item.is_file() and item.name not in allowed_files) or (item.is_dir() and item.name not in allowed_dirs and not item.name.startswith('.'))]; sys.exit(1) if violations and any(print(v) for v in violations) else print('✅ Root is clean')"
	@echo "$(YELLOW)2. Running Vulture (dead code detection)...$(NC)"
	@mkdir -p metadata/reports/analysis
	@vulture src tests --min-confidence 80 --exclude=*/__pycache__/*,*/site/*,*/.venv/* > metadata/reports/analysis/vulture_report.txt 2>&1 || (cat metadata/reports/analysis/vulture_report.txt && echo "⚠️  Dead code detected")
	@echo "$(YELLOW)3. Running jscpd (code duplication detection)...$(NC)"
	@npx jscpd --min-tokens 50 --threshold 3 --ignore "**/node_modules/**,**/.venv/**,**/site/**,**/build/**,**/__pycache__/**,**/data/**" --pattern "**/*.{py,yaml,yml,md}" --reporters console,markdown --output metadata/reports/analysis || echo "⚠️  Code duplication detected - check metadata/reports/analysis/jscpd-report.md"
	@echo "$(YELLOW)4. Running Deptry (dependency management)...$(NC)"
	@deptry . --ignore DEP002,DEP003 > metadata/reports/analysis/deptry_report.txt 2>&1 || (cat metadata/reports/analysis/deptry_report.txt && echo "⚠️  Dependency issues detected")
	@echo "$(GREEN)Root cleanup checks completed! Reports saved to metadata/reports/$(NC)"

# =============================================================================
# UTILITIES
# =============================================================================


# Clean backup files
clean-backups:
	@echo "$(BLUE)Cleaning backup files...$(NC)"
ifeq ($(OS),Windows_NT)
	@powershell -Command "Remove-Item 'data\output\full\*.backup' -Force -ErrorAction SilentlyContinue"
else
	@find data/output/full -name "*.backup" -delete 2>/dev/null || true
endif
	@echo "$(GREEN)Backup files cleaned!$(NC)"

# Run tests
test:
	@echo "$(BLUE)Running all tests...$(NC)"
	@pytest tests/ -v
	@echo "$(GREEN)Tests completed!$(NC)"

# Run unit tests
test-unit:
	@echo "$(BLUE)Running unit tests...$(NC)"
	@pytest tests/unit/ -v -m unit
	@echo "$(GREEN)Unit tests completed!$(NC)"

# Run integration tests
test-integration:
	@echo "$(BLUE)Running integration tests...$(NC)"
	@pytest tests/integration/ -v -m integration
	@echo "$(GREEN)Integration tests completed!$(NC)"

# Run e2e tests
test-e2e:
	@echo "$(BLUE)Running e2e tests...$(NC)"
	@pytest tests/e2e/ -v -m e2e
	@echo "$(GREEN)E2E tests completed!$(NC)"

# Run with test data
run-dev:
	@echo "$(BLUE)Running with test data (3 records)...$(NC)"
	@$(PYTHON) -m library.cli get-document-data --config $(CONFIG_DIR)/config_documents_full.yaml --limit 3
	@echo "$(GREEN)Test run completed!$(NC)"

# Run with full data
run-full:
	@echo "$(BLUE)Running with full data (100 records)...$(NC)"
	@$(PYTHON) -m library.cli get-document-data --config $(CONFIG_DIR)/config_documents_full.yaml --limit 100
	@echo "$(GREEN)Full run completed!$(NC)"

# Install in development mode
install-dev:
	@echo "$(BLUE)Installing in development mode...$(NC)"
	@pip install -e .[dev]
	@echo "$(GREEN)Development installation completed!$(NC)"

# Clean all temporary files
clean: clean-backups
	@echo "$(BLUE)Cleaning temporary files...$(NC)"
ifeq ($(OS),Windows_NT)
	@powershell -Command "Remove-Item '__pycache__' -Recurse -Force -ErrorAction SilentlyContinue"
	@powershell -Command "Remove-Item 'src\**\__pycache__' -Recurse -Force -ErrorAction SilentlyContinue"
	@powershell -Command "Remove-Item 'tests\**\__pycache__' -Recurse -Force -ErrorAction SilentlyContinue"
	@powershell -Command "Remove-Item '.pytest_cache' -Recurse -Force -ErrorAction SilentlyContinue"
	@powershell -Command "Remove-Item '.mypy_cache' -Recurse -Force -ErrorAction SilentlyContinue"
else
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
endif
	@echo "$(GREEN)Temporary files cleaned!$(NC)"

# Quick start
quick-start: run-dev
	@echo "$(GREEN)Quick start completed!$(NC)"

# Full setup
full-setup: install-dev clean-backups run-dev
	@echo "$(GREEN)Full setup completed!$(NC)"
