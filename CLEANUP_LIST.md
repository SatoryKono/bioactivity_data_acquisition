# План очистки корня репозитория

Этот документ описывает план очистки корневого каталога репозитория и перемещения
артефактов в соответствующие места согласно целевой структуре проекта.

## Принципы очистки

1. **Артефакты сборки** → `/artifacts/` или `.gitignore`
2. **Логи** → `/logs/` (в `.gitignore`)
3. **Временные файлы** → `.gitignore` или удаление
4. **Скрипты запуска** → `/scripts/` (тонкие обертки над `bioetl.cli`)
5. **Документация** → `/docs/` с соответствующей структурой
6. **Конфигурация MkDocs** → `docs/mkdocs.yml`

## Категория 1: Скрипты запуска (переместить в `/scripts/`)

### Из `tools/`
- ✅ `tools/run_activity.py` → `scripts/run_activity.py`
- ✅ `tools/run_assay.py` → `scripts/run_assay.py`
- ✅ `tools/run_document.py` → `scripts/run_document.py`
- ✅ `tools/run_target.py` → `scripts/run_target.py`
- ✅ `tools/run_testitem.py` → `scripts/run_testitem.py`

### Из `src/scripts/`
- ✅ `src/scripts/run_chembl_activity.py` → `scripts/run_chembl_activity.py`
- ✅ `src/scripts/run_chembl_assay.py` → `scripts/run_chembl_assay.py`
- ✅ `src/scripts/run_chembl_document.py` → `scripts/run_chembl_document.py`
- ✅ `src/scripts/run_chembl_target.py` → `scripts/run_chembl_target.py`
- ✅ `src/scripts/run_chembl_testitem.py` → `scripts/run_chembl_testitem.py`
- ✅ `src/scripts/run_crossref.py` → `scripts/run_crossref.py`
- ✅ `src/scripts/run_openalex.py` → `scripts/run_openalex.py`
- ✅ `src/scripts/run_pubmed.py` → `scripts/run_pubmed.py`
- ✅ `src/scripts/run_semantic_scholar.py` → `scripts/run_semantic_scholar.py`
- ✅ `src/scripts/run_pubchem.py` → `scripts/run_pubchem.py`
- ✅ `src/scripts/run_uniprot.py` → `scripts/run_uniprot.py`
- ✅ `src/scripts/run_gtp_iuphar.py` → `scripts/run_gtp_iuphar.py`
- ✅ `src/scripts/validate_columns.py` → `scripts/validate_columns.py`
- ✅ `src/scripts/run_inventory.py` → `scripts/run_inventory.py`
- ✅ `src/scripts/generate_pipeline_metrics.py` → `scripts/generate_pipeline_metrics.py`
- ✅ `src/scripts/run_fix_markdown.py` → `scripts/run_fix_markdown.py`

### Из `tools/scripts/`
- ✅ `tools/scripts/run_crossref.py` → `scripts/run_crossref.py` (дубликат, удалить после перемещения)
- ✅ `tools/scripts/run_openalex.py` → `scripts/run_openalex.py` (дубликат, удалить после перемещения)
- ✅ `tools/scripts/run_pubmed.py` → `scripts/run_pubmed.py` (дубликат, удалить после перемещения)
- ✅ `tools/scripts/run_semantic_scholar.py` → `scripts/run_semantic_scholar.py` (дубликат, удалить после перемещения)
- ✅ `tools/scripts/run_pubchem.py` → `scripts/run_pubchem.py` (дубликат, удалить после перемещения)

**Примечание**: После перемещения проверить, что скрипты являются тонкими обертками над `bioetl.cli.main`.

## Категория 2: Артефакты сборки (переместить в `/artifacts/`)

### Из `tools/artifacts/`
- ✅ `tools/artifacts/` → `/artifacts/`
  - `tools/artifacts/baselines/` → `artifacts/baselines/`
  - Остальное содержимое `tools/artifacts/` → `artifacts/`

**Примечание**: После перемещения добавить `/artifacts/` в `.gitignore` (кроме `baselines/` если они нужны в git).

## Категория 3: Логи (переместить в `/logs/`)

### Из `data/logs/`
- ✅ `data/logs/` → `/logs/`

**Примечание**: Убедиться, что `/logs/` в `.gitignore` (кроме `.gitkeep`).

## Категория 4: Конфигурация MkDocs (переместить в `docs/`)

- ✅ `tools/mkdocs.yml` → `docs/mkdocs.yml`

**Примечание**: Обновить пути в `mkdocs.yml` если необходимо (site_dir и т.д.).

## Категория 5: Документация (переместить в `docs/`)

### Из `tools/`
- ✅ `tools/DEPRECATIONS.md` → `docs/DEPRECATIONS.md`
- ✅ `tools/PUBLIC_API.md` → `docs/PUBLIC_API.md`
- ✅ `tools/TARGET_TREE.md` → `docs/TARGET_TREE.md`

### Из `docs/`
- ✅ `docs/PROMPTS/*.md` → Объединить с `docs/cli/prompts/*.md`
  - Проверить дубликаты
  - Оставить уникальные файлы, удалить дубликаты

**Примечание**: Обновить ссылки в документации после перемещения.

## Категория 6: Временные и служебные файлы

### Удалить или переместить в `.gitignore`

#### Временные файлы (уже удалены)
- ✅ `md_scan*.txt` - удалены
- ✅ `md_report.txt` - удалены
- ✅ `coverage.xml` - удален

#### Кэш и артефакты сборки
- ✅ `tools/htmlcov/` → уже в `.gitignore`
- ✅ `tools/gold/` → проверить необходимость
- ✅ `tools/silver/` → проверить необходимость
- ✅ `tools/build/` → уже в `.gitignore`
- ✅ `tools/var/` → проверить необходимость

#### Type stubs
- ✅ `tools/typecheck/stubs/` → оставить как есть (нужны для mypy)

## Категория 7: Служебные файлы в корне

### Оставить в корне (нужны для работы проекта)
- ✅ `pyproject.toml` - конфигурация пакета
- ✅ `README.md` - главная документация
- ✅ `.gitignore` - игнорируемые файлы
- ✅ `.pre-commit-config.yaml` - если есть
- ✅ `.github/workflows/` - если есть

### Переместить из корня
- ✅ Проверить наличие временных файлов в корне
- ✅ Проверить наличие артефактов сборки в корне

## Категория 8: Структура каталогов

### Создать новые каталоги
- ✅ `/artifacts/` - для артефактов сборки
  - Добавить `.gitkeep` если нужно
- ✅ `/logs/` - для логов
  - Добавить `.gitkeep`
  - Добавить в `.gitignore`: `logs/*.log`

### Обновить `.gitignore`

Добавить следующие паттерны:

```gitignore
# Артефакты сборки
/artifacts/*
!/artifacts/.gitkeep
!/artifacts/baselines/

# Логи
/logs/*
!/logs/.gitkeep

# Данные (если еще не добавлено)
/data/cache/
/data/output/
/data/logs/
```

## Порядок выполнения миграции

### Этап 1: Подготовка
1. Создать новые каталоги `/artifacts/`, `/logs/`
2. Добавить `.gitkeep` файлы
3. Обновить `.gitignore`

### Этап 2: Перемещение скриптов
1. Переместить все `run_*.py` из `tools/` → `scripts/`
2. Переместить все `run_*.py` из `src/scripts/` → `scripts/`
3. Проверить дубликаты в `tools/scripts/`
4. Удалить дубликаты после перемещения
5. Обновить импорты в скриптах если необходимо

### Этап 3: Перемещение артефактов
1. Переместить `tools/artifacts/` → `/artifacts/`
2. Проверить, что пути обновлены в CI/CD

### Этап 4: Перемещение логов
1. Переместить `data/logs/` → `/logs/`
2. Обновить пути в конфигурации если необходимо

### Этап 5: Перемещение конфигурации
1. Переместить `tools/mkdocs.yml` → `docs/mkdocs.yml`
2. Проверить и обновить пути в `mkdocs.yml`

### Этап 6: Перемещение документации
1. Переместить `tools/DEPRECATIONS.md` → `docs/DEPRECATIONS.md`
2. Переместить `tools/PUBLIC_API.md` → `docs/PUBLIC_API.md`
3. Переместить `tools/TARGET_TREE.md` → `docs/TARGET_TREE.md`
4. Объединить `docs/PROMPTS/` с `docs/cli/prompts/`
5. Обновить ссылки в документации

### Этап 7: Очистка
1. Удалить пустые каталоги
2. Проверить все ссылки на перемещенные файлы
3. Обновить CI/CD конфигурацию если необходимо

## Проверка после миграции

```bash
# Проверка структуры
ls -la scripts/
ls -la artifacts/
ls -la logs/
ls -la docs/mkdocs.yml

# Проверка что старые пути не используются
rg -n "tools/run_" --exclude-dir=.git
rg -n "src/scripts/run_" --exclude-dir=.git
rg -n "tools/artifacts" --exclude-dir=.git
rg -n "data/logs" --exclude-dir=.git

# Проверка ссылок в документации
rg -n "tools/DEPRECATIONS" docs/
rg -n "tools/PUBLIC_API" docs/
rg -n "tools/TARGET_TREE" docs/
```

## Риски и меры снижения

### Риск 1: Нарушение работы CI/CD
- **Мера**: Обновить пути в `.github/workflows/` и других CI конфигурациях

### Риск 2: Нарушение ссылок в документации
- **Мера**: Проверить все ссылки после перемещения, использовать `git mv` для сохранения истории

### Риск 3: Потеря доступа к логам/артефактам
- **Мера**: Убедиться, что новые каталоги созданы и доступны для записи

### Риск 4: Дублирование скриптов
- **Мера**: Проверить дубликаты перед перемещением, оставить один вариант

## Результат

После выполнения миграции:

- ✅ Корень репозитория содержит только необходимые файлы (`pyproject.toml`, `README.md`, `.gitignore`, etc.)
- ✅ Скрипты запуска находятся в `/scripts/` и являются тонкими обертками
- ✅ Артефакты сборки находятся в `/artifacts/` (в `.gitignore`)
- ✅ Логи находятся в `/logs/` (в `.gitignore`)
- ✅ Документация организована в `/docs/` с правильной структурой
- ✅ Конфигурация MkDocs находится в `docs/mkdocs.yml`
