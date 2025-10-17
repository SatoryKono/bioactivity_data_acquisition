# Скрипты для настройки и запуска

Этот каталог содержит скрипты для автоматизации настройки и запуска bioactivity-data-acquisition.

## 🚀 Автоматическая установка API ключей

**Важно:** Начиная с версии 0.1.0, API ключи устанавливаются автоматически при запуске `bioactivity-data-acquisition`!

- ✅ **Semantic Scholar API ключ** устанавливается автоматически
- ✅ **Не требует ручной настройки** для базового использования
- ✅ **Можно переопределить** через переменные окружения

## 🔑 Установка API ключей

### Python (кроссплатформенный)
```bash
# Установить только Semantic Scholar ключ (по умолчанию)
python scripts/setup_api_keys.py

# Установить все ключи
python scripts/setup_api_keys.py --chembl "your_token" --crossref "your_key"

# Установить постоянно
python scripts/setup_api_keys.py --persistent

# Показать справку
python scripts/setup_api_keys.py --help
```

### PowerShell (Windows)
```powershell
# Установить только Semantic Scholar ключ (по умолчанию)
.\scripts\setup_api_keys.ps1

# Установить все ключи
.\scripts\setup_api_keys.ps1 -ChemblToken "your_token" -CrossrefKey "your_key"

# Установить постоянно
.\scripts\setup_api_keys.ps1 -Persistent

# Показать справку
.\scripts\setup_api_keys.ps1 -ShowHelp
```

### Bash (Linux/macOS)
```bash
# Установить только Semantic Scholar ключ (по умолчанию)
./scripts/setup_api_keys.sh

# Установить все ключи
./scripts/setup_api_keys.sh -c "your_token" -r "your_key"

# Установить постоянно
./scripts/setup_api_keys.sh --persistent

# Показать справку
./scripts/setup_api_keys.sh --help
```

## 🚀 Быстрый старт

### 1. Установить API ключи
```bash
python scripts/setup_api_keys.py
```

### 2. Запустить тест
```bash
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --limit 3
```

### 3. Проверить результат
```bash
# Проверить, что API ключ работает
grep "Using Semantic Scholar with API key" logs/app.log
```

## 🛠️ Makefile команды (Linux/macOS)

Если у вас установлен `make`:

```bash
# Установить API ключи
make setup-api-keys

# Очистить backup файлы
make clean-backups

# Запустить тест
make run-dev

# Запустить с полными данными
make run-full

# Проверить здоровье API
make health-check

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
**Решение:** Запустите скрипт установки API ключей:
```bash
python scripts/setup_api_keys.py
```

### Проблема: "FileExistsError: Cannot create a file when that file already exists"
**Решение:** Очистите backup файлы:
```bash
# Windows
Remove-Item "data\output\full\*.backup" -Force

# Linux/macOS
rm data/output/full/*.backup
```

### Проблема: Ошибки кодировки в Windows
**Решение:** Используйте Python скрипт вместо PowerShell:
```bash
python scripts/setup_api_keys.py
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

```
scripts/
├── setup_api_keys.py         # Python скрипт (кроссплатформенный)
├── setup_api_keys.ps1        # PowerShell скрипт (Windows)
├── setup_api_keys.sh         # Bash скрипт (Linux/macOS)
├── final_validation.py       # Финальная валидация репозитория
├── create_cleanup_pr.py      # Создание PR после очистки
├── stage11_complete.py       # Главный скрипт Stage 11
└── README.md                 # Этот файл
```

## 🎯 Примеры использования

### Автоматизация в CI/CD
```bash
# Установить ключи и запустить тест
python scripts/setup_api_keys.py && \
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --limit 5
```

### Разработка
```bash
# Установить ключи для разработки
python scripts/setup_api_keys.py --persistent

# Запустить тесты
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --limit 3
```

### Продакшн
```bash
# Установить все ключи постоянно
python scripts/setup_api_keys.py --persistent \
  --chembl "prod_token" \
  --crossref "prod_key" \
  --pubmed "prod_key"

# Запустить полную обработку
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --limit 1000
```
