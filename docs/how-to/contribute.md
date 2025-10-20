# Руководство по контрибьюшену

## Процесс

1. Форк/ветка от `main`
2. Изменения с тестами и обновлением документации
3. Локальные проверки: pytest, mypy, ruff, black, pre-commit
4. PR с чек-листом и ссылками на задачи

## Стиль коммитов (Conventional Commits)

Примеры:

- `feat(cli): add --limit option to get-document-data`
- `fix(semanticscholar): handle 429 with Retry-After`
- `docs(ops): add monitoring schedule examples`

## Ветвление и ревью

- Ветки формата `feat/…`, `fix/…`, `docs/…`
- Минимум один апрув
- CI зелёный: тесты ≥ 90%, линтеры чистые

## Чек-лист PR

| Пункт | Статус |
|---|---|
| Тесты проходят, покрытие ≥ 90% | [ ] |
| Линтеры чисты (mypy, ruff, black) | [ ] |
| Docs обновлены (при необходимости) | [ ] |
| CHANGELOG обновлён | [ ] |

## Локальный предпросмотр документации

```bash
pip install -r configs/requirements.txt
mkdocs serve
```

## Политика артефактов и LFS

### Запрещённые к коммиту файлы

❌ **НИКОГДА не коммитьте следующие типы файлов:**

#### Кэши и временные файлы

- `__pycache__/` - Скомпилированные Python файлы
- `.mypy_cache/` - Кэш mypy
- `.pytest_cache/` - Кэш pytest
- `temp_*.txt` - Временные файлы разработки
- `CLI_INTEGRATION_SNIPPET.py` - Временные фрагменты кода

#### Логи и отчёты

- `logs/` - Все лог файлы (кроме `.gitkeep`)
- `*.log` - Лог файлы в любом месте
- `reports/*.csv` - Сгенерированные отчёты (кроме `config_audit.csv`)
- `reports/*.json` - JSON отчёты

#### Тестовые выходные данные

- `tests/test_outputs/*` - Результаты тестов (кроме `.gitkeep`)
- `site/` - Сгенерированная документация MkDocs

#### IDE и OS файлы

- `.vscode/` - Настройки VS Code (кроме разрешённых)
- `.idea/` - Настройки JetBrains IDEs
- `.DS_Store` - macOS системные файлы
- `Thumbs.db` - Windows системные файлы
- `.cursor/plans/` - Планы Cursor IDE

### Git LFS для больших файлов

✅ **Используйте Git LFS для файлов >500KB:**

#### Автоматически отслеживаемые форматы

- **Данные**: `*.parquet`, `*.pkl`, `*.h5`, `*.hdf5`
- **Excel**: `*.xlsm`, `*.xlsx`, `*.xls`
- **Изображения**: `*.png`, `*.jpg`, `*.jpeg`, `*.gif`, `*.bmp`, `*.tiff`, `*.svg`
- **Видео**: `*.mp4`, `*.avi`, `*.mov`, `*.mkv`
- **Аудио**: `*.mp3`, `*.wav`, `*.flac`
- **Архивы**: `*.zip`, `*.tar.gz`, `*.7z`, `*.rar`
- **Базы данных**: `*.db`, `*.sqlite`, `*.sqlite3`
- **Модели**: `*.h5`, `*.hdf5`, `*.pkl`, `*.joblib`, `*.model`
- **Документы**: `*.pdf`
- **Большие текстовые файлы**: `*.json`, `*.csv`, `*.tsv`, `*.xml`, `*.log`

#### Настройка Git LFS

```bash
# Установка Git LFS (выполнить один раз)
git lfs install

# Проверка статуса
git lfs status
git lfs ls-files
```

#### Ограничения

- **Максимальный размер файла**: 2GB (GitHub)
- **Бесплатная квота**: 1GB
- **Производительность**: LFS файлы загружаются по требованию

### Обработка сгенерированных выходных данных

#### Рекомендуемые практики

1. **Используйте .gitignore** для исключения сгенерированных файлов
2. **Создавайте .gitkeep** для пустых директорий, которые должны существовать
3. **Документируйте** какие файлы генерируются автоматически
4. **Используйте CI артефакты** для публикации отчётов

#### Примеры правильной структуры

```text
logs/
├── .gitkeep          # ✅ Разрешено
└── app.log           # ❌ Запрещено (генерируется)

reports/
├── .gitkeep          # ✅ Разрешено
├── config_audit.csv  # ✅ Разрешено (исключение)
└── coverage.json     # ❌ Запрещено (генерируется)

tests/test_outputs/
├── .gitkeep          # ✅ Разрешено
└── test_results.csv  # ❌ Запрещено (генерируется)
```

## Pre-commit хуки

### Установка и настройка

```bash
# Установка pre-commit
pip install pre-commit

# Установка хуков в репозиторий
pre-commit install

# Ручной запуск всех хуков
pre-commit run --all-files
```

### Активные проверки

1. **Блокировка больших файлов** (>500KB)
2. **Блокировка артефактов** в `logs/`, `reports/`, `tests/test_outputs/`
3. **Проверка секретов** - поиск хардкодированных API ключей и паролей
4. **Блокировка print statements** в библиотечном коде
5. **Форматирование кода** (black, ruff)
6. **Проверка типов** (mypy)
7. **Линтинг Markdown** (markdownlint)

### Обход проверок (не рекомендуется)

```bash
# Временный обход (только для экстренных случаев)
git commit --no-verify -m "Emergency commit"
```

## CI артефакты

### Публикуемые артефакты

- **Отчёты покрытия** - HTML и JSON отчёты о покрытии кода тестами
- **Test outputs** - Результаты тестов и бенчмарков
- **Security отчёты** - Результаты проверки безопасности
- **Документация** - Предварительный просмотр документации для PR

### Доступ к артефактам

1. Перейдите в **Actions** → ваш workflow
2. Выберите нужный билд
3. Прокрутите вниз до секции **Artifacts**
4. Скачайте нужный артефакт

## Устранение неполадок

### Проблемы с Git LFS

```bash
# Переустановка Git LFS
git lfs uninstall
git lfs install

# Очистка кэша LFS
git lfs prune

# Принудительная загрузка всех LFS файлов
git lfs pull --all

# Проверка целостности
git lfs fsck
```

### Проблемы с pre-commit

```bash
# Обновление хуков
pre-commit autoupdate

# Очистка кэша
pre-commit clean

# Запуск конкретного хука
pre-commit run <hook-id>

# Пропуск хуков для текущего коммита
git commit --no-verify
```

### Проблемы с большими файлами

```bash
# Проверка размера файлов в репозитории
git rev-list --objects --all | git cat-file --batch-check='%(objecttype) %(objectname) %(objectsize) %(rest)' | awk '/^blob/ {print substr($0,6)}' | sort --numeric-sort --key=2 | tail -10

# Удаление файла из истории (ОСТОРОЖНО!)
git filter-branch --force --index-filter 'git rm --cached --ignore-unmatch path/to/large/file' --prune-empty --tag-name-filter cat -- --all
```

### Восстановление после ошибок

```bash
# Отмена последнего коммита (сохраняя изменения)
git reset --soft HEAD~1

# Отмена последнего коммита (удаляя изменения)
git reset --hard HEAD~1

# Очистка рабочей директории
git clean -fd
```

## Дополнительные ресурсы

- Pre-commit документация: [pre-commit.com](https://pre-commit.com/)
- Git LFS документация: [git-lfs.github.io](https://git-lfs.github.io/)
