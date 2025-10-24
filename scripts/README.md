# Скрипты для настройки и запуска

Этот каталог содержит скрипты для автоматизации настройки и запуска bioactivity-data-acquisition.

## 🚀 Автоматическая установка API ключей

**Важно:** Начиная с версии 0.1.0, API ключи устанавливаются автоматически при запуске `bioactivity-data-acquisition`!

- ✅ **Semantic Scholar API ключ** устанавливается автоматически
- ✅ **Не требует ручной настройки** для базового использования
- ✅ **Можно переопределить** через переменные окружения

## 📋 Миграция скриптов (v0.1.0+)

**Удалённые скрипты и их замены (обновлено в PR #1):**

| Удалённый скрипт | Замена | Описание |
|------------------|--------|----------|
| `setup_api_keys.*` | Автоматическая установка | API ключи устанавливаются при запуске CLI |
| `get_activity_data.py` | `library.cli get-activity-data` | Извлечение данных активностей (УДАЛЕН) |
| `api_health_check.py` | `library.cli health` | Проверка здоровья API |
| `monitor_api.py` | `library.cli health` | Мониторинг API |
| `check_api_limits.py` | `library.cli health` | Проверка лимитов API |
| `quick_api_check.py` | `library.cli health --json` | Быстрая проверка API |
| `monitor_semantic_scholar.py` | `library.cli health` | Мониторинг Semantic Scholar |
| `check_semantic_scholar_status.py` | `library.cli health` | Статус Semantic Scholar |
| `get_semantic_scholar_api_key.py` | Автоматическая установка | Ключ устанавливается автоматически |
| `monitor_pubmed.py` | `library.cli health` | Мониторинг PubMed |
| `get_pubmed_api_key.py` | Автоматическая установка | Ключ устанавливается автоматически |

## 🔑 Установка API ключей

**API ключи устанавливаются автоматически при запуске CLI!**

Для переопределения используйте переменные окружения:

```bash
# Установить собственные ключи
export SEMANTIC_SCHOLAR_API_KEY="your_key"
export CHEMBL_API_TOKEN="your_token"
export CROSSREF_API_KEY="your_key"
export PUBMED_API_KEY="your_key"

# Запустить CLI (ключи будут использованы автоматически)
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml
```

## 🚀 Быстрый старт

### 1. Запустить тест (API ключи устанавливаются автоматически)

```bash
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --limit 3
```

### 2. Проверить здоровье API

```bash
bioactivity-data-acquisition health --config configs/config_documents_full.yaml
```

### 3. Проверить результат

```bash
# Проверить, что API ключ работает
grep "Using Semantic Scholar with API key" logs/app.log
```

## 🛠️ Makefile команды (Linux/macOS)

Если у вас установлен `make`:

```bash
# Очистить backup файлы
make clean-backups

# Запустить тест
make run-dev

# Запустить с полными данными
make run-full

# Проверить здоровье API
make health CONFIG=configs/config_documents_full.yaml

# Полная настройка и запуск
make full-setup

# Показать все команды
make help
```

## 📋 Доступные API ключи

| API | Переменная окружения | Описание |
|-----|---------------------|----------|
| Semantic Scholar | `SEMANTIC_SCHOLAR_API_KEY` | Предустановлен, для получения метаданных статей |
| ChEMBL | `CHEMBL_API_TOKEN` | Для доступа к биоактивностным данным |
| Crossref | `CROSSREF_API_KEY` | Для DOI метаданных |
| OpenAlex | `OPENALEX_API_KEY` | Для академических метаданных |
| PubMed | `PUBMED_API_KEY` | Для биомедицинских публикаций |

## 🔧 Устранение проблем

### Проблема: "Using conservative rate limiting for semantic_scholar (no API key)"

**Решение:** API ключи устанавливаются автоматически. Если проблема сохраняется, проверьте переменные окружения:

```bash
# Проверить установленные ключи
echo $SEMANTIC_SCHOLAR_API_KEY

# Установить ключ вручную
export SEMANTIC_SCHOLAR_API_KEY="your_key"
```

### Проблема: "FileExistsError: Cannot create a file when that file already exists"

**Решение:** Очистите backup файлы:

```bash
# Windows
Remove-Item "data\output\full\*.backup" -Force

# Linux/macOS
rm data/output/full/*.backup

# Или используйте Makefile
make clean-backups
```

### Проблема: Ошибки API или сети

**Решение:** Проверьте здоровье API:

```bash
# Проверить статус всех API
bioactivity-data-acquisition health --config configs/config_documents_full.yaml

# Проверить конкретный API в JSON формате
bioactivity-data-acquisition health --config configs/config_documents_full.yaml --json
```

## 🧹 Stage 11: Финальная валидация и создание PR

### Автоматизированная валидация репозитория

После завершения очистки репозитория используйте эти скрипты для финальной валидации:

```bash
# Полный workflow: валидация + создание PR
python scripts/stage11_complete.py

# Только валидация (без создания PR)
python scripts/stage11_complete.py --skip-pr

# Подробный вывод
python scripts/stage11_complete.py --verbose

# Показать что будет сделано (без выполнения)
python scripts/stage11_complete.py --dry-run
```

### Отдельные скрипты валидации

```bash
# Финальная валидация (тесты, линтинг, pre-commit, docs)
python scripts/final_validation.py

# Создание PR после валидации
python scripts/create_cleanup_pr.py

# Создание PR с указанием ветки
python scripts/create_cleanup_pr.py --branch feature/cleanup-validation
```

### Что проверяется

- ✅ **Тесты**: `make test` - все unit и integration тесты
- ✅ **Линтинг**: `make lint` - проверка качества кода
- ✅ **Типы**: `make type-check` - проверка типов mypy
- ✅ **Pre-commit**: `pre-commit run --all-files` - все хуки
- ✅ **Документация**: `mkdocs build --strict` - сборка docs
- ✅ **Git статус**: проверка чистоты рабочей директории
- ✅ **Здоровье репозитория**: размер, структура файлов

## 📁 Структура файлов

```text
scripts/
├── final_validation.py       # Финальная валидация репозитория
├── create_cleanup_pr.py      # Создание PR после очистки
├── stage11_complete.py       # Главный скрипт Stage 11
├── analyze_fixed_results.py  # Анализ результатов исправлений
├── check_field_fill.py       # Проверка заполнения полей
├── check_version_consistency.py # Проверка версий
├── cleanup_logs.py           # Очистка логов
├── generate_cleanup_manifest.py # Генерация манифеста очистки
├── replace_print_with_logger.py # Замена print на logger
├── run_mypy.py              # Запуск mypy
└── README.md                # Этот файл
```

## 🎯 Примеры использования

### Автоматизация в CI/CD

```bash
# Запустить тест (ключи устанавливаются автоматически)
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --limit 5

# Проверить здоровье API
bioactivity-data-acquisition health --config configs/config_documents_full.yaml
```

### Разработка

```bash
# Запустить тесты с ограничением
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --limit 3

# Проверить здоровье API в JSON формате
bioactivity-data-acquisition health --config configs/config_documents_full.yaml --json
```

### Продакшн

```bash
# Установить все ключи через переменные окружения
export CHEMBL_API_TOKEN="prod_token"
export CROSSREF_API_KEY="prod_key"
export PUBMED_API_KEY="prod_key"

# Запустить полную обработку
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --limit 1000
```
