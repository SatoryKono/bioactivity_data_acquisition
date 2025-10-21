# Unified Makefile

Документация по единому Makefile для всех операций в проекте bioactivity-data-acquisition.

## Философия использования Make

В проекте Make используется для:

- **Унификации интерфейса**: Единый интерфейс для всех пайплайнов
- **Параметризации**: Гибкие команды с параметрами
- **Автоматизации**: Упрощение повторяющихся операций
- **Документации**: Справка по доступным командам
- **Кроссплатформенности**: Работа на Windows, Linux, macOS

## Единый интерфейс

### Основные команды

```bash
# Показать справку
make help

# Универсальные команды пайплайнов
make pipeline TYPE=<documents|targets|assays|activities|testitems> INPUT=... CONFIG=... [FLAGS="..."]
make pipeline-test TYPE=<...> [MARKERS="slow"]
make pipeline-clean TYPE=<...>

# Health & Monitoring
make health CONFIG=...

# Code Quality
make fmt          # Format code
make lint         # Lint code
make type-check   # Type checking
make qa           # Full quality check

# Documentation
make docs-serve   # Serve documentation locally
make docs-build   # Build documentation
make docs-lint    # Lint documentation
make docs-deploy  # Deploy documentation

# Utilities
make setup-api-keys  # Setup API keys
make clean          # Clean temporary files
make test           # Run tests
make install-dev    # Install in development mode
```

## Pipeline Commands

### Универсальный запуск пайплайнов

```bash
# Documents pipeline
make pipeline TYPE=documents CONFIG=configs/config_documents_full.yaml

# Targets pipeline
make pipeline TYPE=targets INPUT=data/input/target.csv CONFIG=configs/config_target_full.yaml

# Assays pipeline
make pipeline TYPE=assays INPUT=data/input/assay.csv CONFIG=configs/config_assay_full.yaml

# Activities pipeline
make pipeline TYPE=activities INPUT=data/input/activity.csv CONFIG=configs/config_activity_full.yaml

# Testitems pipeline
make pipeline TYPE=testitems INPUT=data/input/testitem.csv CONFIG=configs/config_testitem_full.yaml
```

### Тестирование пайплайнов

```bash
# Test all pipeline types
make pipeline-test TYPE=documents
make pipeline-test TYPE=targets
make pipeline-test TYPE=assays
make pipeline-test TYPE=activities
make pipeline-test TYPE=testitems

# Test with specific markers
make pipeline-test TYPE=documents MARKERS="slow"
make pipeline-test TYPE=targets MARKERS="integration"
```

### Очистка пайплайнов

```bash
# Clean pipeline outputs
make pipeline-clean TYPE=documents
make pipeline-clean TYPE=targets
make pipeline-clean TYPE=assays
make pipeline-clean TYPE=activities
make pipeline-clean TYPE=testitems
```

## Health & Monitoring

### Health checks

```bash
# Check API health
make health CONFIG=configs/config_documents_full.yaml
make health CONFIG=configs/config_target_full.yaml
make health CONFIG=configs/config_assay_full.yaml
```

## Code Quality

### Formatting and linting

```bash
# Format code
make fmt

# Lint code
make lint

# Type checking
make type-check

# Full quality check
make qa
```

## Documentation

### Documentation operations

```bash
# Serve documentation locally
make docs-serve

# Build documentation
make docs-build

# Lint documentation
make docs-lint

# Deploy documentation
make docs-deploy
```

## Utilities

### Setup and maintenance

```bash
# Setup API keys
make setup-api-keys

# Clean temporary files
make clean

# Run tests
make test

# Install in development mode
make install-dev
```

### Quick start commands

```bash
# Quick start
make quick-start

# Full setup
make full-setup
```

## Examples

### Basic usage

```bash
# 1. Setup
make setup-api-keys
make install-dev

# 2. Run documents pipeline
make pipeline TYPE=documents CONFIG=configs/config_documents_full.yaml

# 3. Test
make pipeline-test TYPE=documents

# 4. Clean
make pipeline-clean TYPE=documents
```

### Advanced usage

```bash
# Custom input and config
make pipeline TYPE=targets \
  INPUT=data/input/custom_targets.csv \
  CONFIG=configs/custom_config.yaml \
  FLAGS="--limit 100 --timeout-sec 120"

# Test with specific markers
make pipeline-test TYPE=assays MARKERS="slow integration"

# Health check
make health CONFIG=configs/config_documents_full.yaml
```

### Development workflow

```bash
# 1. Code quality
make qa

# 2. Test
make test

# 3. Run pipeline
make pipeline TYPE=documents

# 4. Clean
make clean
```

## Migration from old Makefiles

### Old commands → New commands

| Old Command | New Command |
|-------------|-------------|
| `make -f Makefile.assay assay-example` | `make pipeline TYPE=assays` |
| `make -f Makefile.assay test-assay` | `make pipeline-test TYPE=assays` |
| `make -f Makefile.assay clean-assay` | `make pipeline-clean TYPE=assays` |
| `make -f Makefile.target target-example` | `make pipeline TYPE=targets` |
| `make -f Makefile.target test-target` | `make pipeline-test TYPE=targets` |
| `make -f Makefile.target clean-target` | `make pipeline-clean TYPE=targets` |

### Benefits of unified interface

- **Consistency**: Same syntax for all pipelines
- **Simplicity**: One Makefile to learn
- **Flexibility**: Parameterized commands
- **Maintainability**: Single source of truth
- **Extensibility**: Easy to add new pipeline types

## Troubleshooting

### Common issues

1. **Missing TYPE parameter**:
   ```bash
   # Error: TYPE is required
   make pipeline
   
   # Solution: Specify TYPE
   make pipeline TYPE=documents
   ```

2. **Missing CONFIG parameter**:
   ```bash
   # Error: CONFIG is required for health
   make health
   
   # Solution: Specify CONFIG
   make health CONFIG=configs/config_documents_full.yaml
   ```

3. **Invalid TYPE**:
   ```bash
   # Error: Invalid TYPE
   make pipeline TYPE=invalid
   
   # Solution: Use valid TYPE
   make pipeline TYPE=documents
   ```

### Getting help

```bash
# Show all available commands
make help

# Show specific pipeline help
make pipeline TYPE=documents  # Will show error with valid types
```

## Best practices

1. **Always specify TYPE**: Required for pipeline commands
2. **Use CONFIG parameter**: For custom configurations
3. **Use INPUT parameter**: For custom input files
4. **Use FLAGS parameter**: For additional CLI options
5. **Test before running**: Use `pipeline-test` before `pipeline`
6. **Clean after testing**: Use `pipeline-clean` to remove outputs

## Future extensions

The unified Makefile is designed to be easily extensible:

- Add new pipeline types by adding new `pipeline-<type>` targets
- Add new test types by adding new `pipeline-test-<type>` targets
- Add new cleanup types by adding new `pipeline-clean-<type>` targets
- Add new utility commands as needed