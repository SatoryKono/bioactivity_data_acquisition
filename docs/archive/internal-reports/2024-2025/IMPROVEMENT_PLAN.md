# План улучшений: bioactivity_data_acquisition

**Репозиторий:** SatoryKono/bioactivity_data_acquisition  
**Дата:** 2025-10-17  
**Количество улучшений:** 20  
**Сортировка:** Приоритет (P1 → P3), затем трудозатраты (S → L)

---

## Улучшение 1: Создать .env.example с документацией секретов

**ID:** `imp-001`  
**Тип:** `devx`  
**Приоритет:** P1  
**Трудозатраты:** S (Small, 1–2 часа)

### Описание

Создать файл `.env.example` в корне репозитория с шаблоном всех необходимых переменных окружения, включая комментарии и placeholder-значения.

### Как сделать

1. Собрать все упоминания секретов из `README.md`, `src/library/config.py`, `configs/config*.yaml`.
2. Создать `.env.example` с секциями (API Keys, Configuration Overrides, Telemetry).
3. Для каждой переменной добавить:
   - Комментарий с описанием назначения
   - Placeholder-значение (e.g., `CHEMBL_API_TOKEN=your_token_here`)
   - Указание обязательности (required/optional)
4. Добавить ссылку на `.env.example` в README.md секцию Quick Start.
5. Добавить `.env` в `.gitignore` (если ещё не добавлено).
6. Обновить `docs/configuration.md` с примером из `.env.example`.

### Критерии приёмки

- [ ] Файл `.env.example` существует в корне репозитория.
- [ ] Содержит все секреты, упомянутые в README и конфигах (минимум: CHEMBL_API_TOKEN, PUBMED_API_KEY, SEMANTIC_SCHOLAR_API_KEY, CROSSREF_API_KEY, JAEGER_ENDPOINT).
- [ ] Каждая переменная имеет комментарий и placeholder.
- [ ] `.env` добавлен в `.gitignore`.
- [ ] README.md ссылается на `.env.example` в секции "Настройка переменных окружения".

### Связанные файлы

- `README.md:56-63, 169-186`
- `src/library/config.py:17-33` (validate_secrets)
- Новый файл: `.env.example`
- `.gitignore`

---

## Улучшение 2: Удалить deprecated модуль logger.py

**ID:** `imp-002`  
**Тип:** `arch`  
**Приоритет:** P1  
**Трудозатраты:** S (1 час)

### Описание

Удалить `src/library/logger.py`, так как он помечен как deprecated, а функциональность перенесена в `logging_setup.py`.

### Как сделать

1. Выполнить grep-поиск импортов из `library.logger` во всех файлах проекта:
   ```bash
   rg "from library.logger import|import library.logger"
   ```
2. Заменить найденные импорты на `from library.logging_setup import`.
3. Удалить файл `src/library/logger.py`.
4. Обновить `__all__` в `src/library/__init__.py`, если там экспортировался старый модуль.
5. Запустить тесты: `pytest -v`.
6. Проверить mypy: `mypy src/`.

### Критерии приёмки

- [ ] `src/library/logger.py` удалён.
- [ ] Нет импортов из `library.logger` в кодовой базе (grep возвращает 0 результатов).
- [ ] Все тесты проходят.
- [ ] MyPy не выдаёт ошибок.
- [ ] Pre-commit hooks проходят.

### Связанные файлы

- `src/library/logger.py` (удалить)
- `src/library/__init__.py` (возможно обновить)
- Все файлы с импортами `library.logger` (grep результаты)

---

## Улучшение 3: Создать конфигурационные файлы безопасности

**ID:** `imp-003`  
**Тип:** `security`  
**Приоритет:** P1  
**Трудозатраты:** S (1–2 часа)

### Описание

Создать отсутствующие файлы `.bandit`, `.banditignore`, `.safety_policy.yaml`, на которые ссылаются Dockerfile и Makefile.

### Как сделать

1. Создать `.bandit` с базовой конфигурацией:
   ```yaml
   skips: ['B101']  # assert_used (разрешён в тестах)
   exclude_dirs: ['/tests/', '/venv/', '/.venv/']
   ```
2. Создать `.banditignore` (если нужны исключения для специфичных файлов).
3. Создать `.safety_policy.yaml` с примером:
   ```yaml
   security:
     ignore-vulnerabilities:
       # Список CVE для игнорирования с обоснованием
   ```
4. Обновить `Dockerfile:100-102` и `Makefile:54,169`, чтобы корректно ссылаться на эти файлы.
5. Запустить локально:
   ```bash
   bandit -r src/ -c .bandit -ll
   safety check --policy-file .safety_policy.yaml
   ```
6. Убедиться, что CI проходит.

### Критерии приёмки

- [ ] Файлы `.bandit`, `.banditignore`, `.safety_policy.yaml` существуют.
- [ ] `bandit -r src/ -c .bandit -ll` выполняется успешно.
- [ ] `safety check --policy-file .safety_policy.yaml` выполняется успешно.
- [ ] CI workflow проходит без ошибок.
- [ ] Dockerfile и Makefile копируют/используют эти файлы корректно.

### Связанные файлы

- Новые файлы: `.bandit`, `.banditignore`, `.safety_policy.yaml`
- `Dockerfile:100-102`
- `Makefile:54-55, 169-176`
- `.github/workflows/ci.yaml:38-46`

---

## Улучшение 4: Сократить длину строки до 120 символов

**ID:** `imp-004`  
**Тип:** `lint`  
**Приоритет:** P1  
**Трудозатраты:** M (Medium, 3–4 часа)

### Описание

Уменьшить максимальную длину строки с 180 до 120 символов (стандарт Python community) для улучшения читаемости.

### Как сделать

1. Обновить `configs/pyproject.toml`:
   ```toml
   [tool.black]
   line-length = 120
   
   [tool.ruff]
   line-length = 120
   ```
2. Запустить автоформатирование:
   ```bash
   black .
   ruff check --fix .
   ```
3. Вручную отредактировать строки, которые не поддаются автоформатированию (длинные URL, docstrings).
4. Обновить `.pre-commit-config.yaml`, если там захардкожены параметры.
5. Запустить тесты и проверку типов.
6. Закоммитить изменения с сообщением "style: reduce line length to 120".

### Критерии приёмки

- [ ] `configs/pyproject.toml` обновлён: `line-length = 120`.
- [ ] Все файлы отформатированы с новым лимитом.
- [ ] `black --check .` и `ruff check .` проходят без ошибок.
- [ ] Тесты проходят.
- [ ] Pre-commit hooks проходят.

### Связанные файлы

- `configs/pyproject.toml:81-82, 84`
- Все `.py` файлы в `src/` и `tests/`

---

## Улучшение 5: Добавить CODEOWNERS файл

**ID:** `imp-005`  
**Тип:** `devx`  
**Приоритет:** P2  
**Трудозатраты:** S (30 минут)

### Описание

Создать файл `.github/CODEOWNERS` для автоматического назначения ревьюеров на PR.

### Как сделать

1. Создать `.github/CODEOWNERS` со структурой:
   ```
   # Global owners
   * @SatoryKono
   
   # Configuration files
   configs/ @SatoryKono
   
   # Documentation
   docs/ @SatoryKono
   
   # CI/CD
   .github/workflows/ @SatoryKono
   
   # Security
   SECURITY.md @SatoryKono
   ```
2. Указать реальных владельцев для каждой секции (если команда больше одного человека).
3. Добавить в `docs/contributing.md` описание процесса код-ревью с упоминанием CODEOWNERS.
4. Протестировать: создать PR и проверить, что владельцы автоматически назначены.

### Критерии приёмки

- [ ] Файл `.github/CODEOWNERS` существует.
- [ ] Указаны владельцы для основных директорий (src/, tests/, docs/, configs/).
- [ ] При создании PR автоматически запрашивается ревью от указанных владельцев.
- [ ] `docs/contributing.md` содержит упоминание CODEOWNERS.

### Связанные файлы

- Новый файл: `.github/CODEOWNERS`
- `docs/contributing.md`

---

## Улучшение 6: Добавить lock-файлы зависимостей

**ID:** `imp-006`  
**Тип:** `devx`  
**Приоритет:** P2  
**Трудозатраты:** M (2–3 часа)

### Описание

Внедрить pip-tools (или poetry) для генерации lock-файлов зависимостей, обеспечивая детерминизм установки.

### Как сделать

1. Установить pip-tools: `pip install pip-tools`.
2. Создать `requirements.in` из dependencies в `pyproject.toml`.
3. Создать `requirements-dev.in` из optional-dependencies[dev].
4. Сгенерировать lock-файлы:
   ```bash
   pip-compile requirements.in -o requirements.txt
   pip-compile requirements-dev.in -o requirements-dev.txt
   ```
5. Обновить CI workflow для использования lock-файлов:
   ```yaml
   pip install -r requirements.txt -r requirements-dev.txt
   ```
6. Обновить `Dockerfile` для установки из lock-файлов.
7. Добавить в `Makefile` цели `deps-compile`, `deps-sync`, `deps-upgrade`.
8. Обновить `docs/development.md` с инструкциями по управлению зависимостями.

### Критерии приёмки

- [ ] `requirements.txt` и `requirements-dev.txt` содержат закреплённые версии всех зависимостей.
- [ ] CI использует lock-файлы для установки зависимостей.
- [ ] `Dockerfile` установка использует lock-файлы.
- [ ] `Makefile` содержит цели для компиляции/синхронизации зависимостей.
- [ ] `docs/development.md` документирует процесс управления зависимостями.

### Связанные файлы

- Новые файлы: `requirements.in`, `requirements-dev.in`, `requirements.txt`, `requirements-dev.txt`
- `.github/workflows/ci.yaml:25-27`
- `Dockerfile:28-29`
- `Makefile` (новые цели)
- `docs/development.md`

---

## Улучшение 7: Добавить кэширование зависимостей в CI

**ID:** `imp-007`  
**Тип:** `ci`  
**Приоритет:** P2  
**Трудозатраты:** S (1 час)

### Описание

Настроить `actions/cache` для кэширования pip зависимостей в GitHub Actions, ускоряя CI на 50–70%.

### Как сделать

1. Обновить `.github/workflows/ci.yaml`, добавить шаг после `setup-python`:
   ```yaml
   - name: Cache pip dependencies
     uses: actions/cache@v3
     with:
       path: ~/.cache/pip
       key: ${{ runner.os }}-pip-${{ hashFiles('requirements*.txt') }}
       restore-keys: |
         ${{ runner.os }}-pip-
   ```
2. Убедиться, что кэш инвалидируется при изменении `requirements.txt`.
3. Запустить CI несколько раз и сравнить время выполнения (до/после).
4. Добавить кэширование для pre-commit: `~/.cache/pre-commit`.
5. Документировать в `docs/ci.md`.

### Критерии приёмки

- [ ] CI workflow использует `actions/cache` для pip зависимостей.
- [ ] Кэш инвалидируется при изменении `requirements.txt`.
- [ ] Время выполнения CI сокращено на ≥30% (измерить на нескольких запусках).
- [ ] Кэш для pre-commit также настроен.
- [ ] `docs/ci.md` документирует кэширование.

### Связанные файлы

- `.github/workflows/ci.yaml`
- `docs/ci.md`

---

## Улучшение 8: Переместить CONTRIBUTING.md в корень

**ID:** `imp-008`  
**Тип:** `docs`  
**Приоритет:** P2  
**Трудозатраты:** S (15 минут)

### Описание

Создать `CONTRIBUTING.md` в корне репозитория (или симлинк на `docs/contributing.md`), так как GitHub ищет его именно там для отображения в UI.

### Как сделать

1. Скопировать `docs/contributing.md` в `CONTRIBUTING.md`:
   ```bash
   cp docs/contributing.md CONTRIBUTING.md
   ```
2. Или создать симлинк (если GitHub поддерживает):
   ```bash
   ln -s docs/contributing.md CONTRIBUTING.md
   ```
3. Добавить в конце `CONTRIBUTING.md` ссылку на полную документацию: "Дополнительная информация: [docs/contributing.md](docs/contributing.md)".
4. Обновить `README.md`, добавить секцию "Как контрибутить" со ссылкой на `CONTRIBUTING.md`.
5. Проверить, что GitHub UI показывает кнопку "Contributing guidelines" на странице репозитория.

### Критерии приёмки

- [ ] Файл `CONTRIBUTING.md` существует в корне репозитория.
- [ ] GitHub UI показывает "Contributing guidelines" на странице репо.
- [ ] `README.md` ссылается на `CONTRIBUTING.md`.
- [ ] Содержимое актуально и совпадает с `docs/contributing.md`.

### Связанные файлы

- Новый файл: `CONTRIBUTING.md`
- `docs/contributing.md`
- `README.md`

---

## Улучшение 9: Создать ER-диаграмму для схем данных

**ID:** `imp-009`  
**Тип:** `docs`  
**Приоритет:** P2  
**Трудозатраты:** M (3–4 часа)

### Описание

Создать ER-диаграмму в Mermaid формате, визуализирующую Pandera схемы и связи между таблицами (documents, activities, assays, targets).

### Как сделать

1. Изучить Pandera схемы в `src/library/schemas/`.
2. Идентифицировать ключевые таблицы и их колонки.
3. Определить связи (foreign keys, например, `document_chembl_id` как связь).
4. Создать файл `docs/data_schemas_erd.md` с Mermaid ER-диаграммой:
   ```mermaid
   erDiagram
       DOCUMENTS ||--o{ ACTIVITIES : has
       DOCUMENTS {
           string document_chembl_id PK
           string doi
           string title
       }
       ACTIVITIES {
           int activity_id PK
           string document_chembl_id FK
           float standard_value
       }
   ```
5. Добавить ссылку на диаграмму в `docs/data_schemas.md` и `README.md`.
6. Обновить при изменении схем (добавить в чек-лист релиза).

### Критерии приёмки

- [ ] Файл `docs/data_schemas_erd.md` существует с Mermaid ER-диаграммой.
- [ ] Диаграмма включает все основные таблицы (documents, activities, assays, targets, testitem, tissue, cell).
- [ ] Связи между таблицами корректно отображены (FK).
- [ ] `docs/data_schemas.md` ссылается на ER-диаграмму.
- [ ] `README.md` содержит краткое упоминание ER-диаграммы.

### Связанные файлы

- Новый файл: `docs/data_schemas_erd.md`
- `docs/data_schemas.md`
- `README.md`
- `src/library/schemas/*.py` (источник данных)

---

## Улучшение 10: Добавить матрицу OS в CI

**ID:** `imp-010`  
**Тип:** `ci`  
**Приоритет:** P2  
**Трудозатраты:** S (1 час)

### Описание

Расширить CI матрицу для тестирования на Windows и macOS помимо Ubuntu.

### Как сделать

1. Обновить `.github/workflows/ci.yaml`:
   ```yaml
   strategy:
     matrix:
       os: [ubuntu-latest, windows-latest, macos-latest]
       python-version: ['3.10', '3.11', '3.12']
   ```
2. Заменить `runs-on: ubuntu-latest` на `runs-on: ${{ matrix.os }}`.
3. Добавить условия для OS-специфичных команд (e.g., path separators).
4. Запустить CI и проверить, что все OS проходят тесты.
5. Если Windows/macOS падают, добавить `allow-failure` или фикс.
6. Документировать в `docs/ci.md`.

### Критерии приёмки

- [ ] CI matrix включает `os: [ubuntu-latest, windows-latest, macos-latest]`.
- [ ] Все комбинации OS × Python проходят (или помечены как allowed-failure с TODO).
- [ ] Время выполнения CI увеличилось незначительно (матрица параллельна).
- [ ] `docs/ci.md` документирует поддержку нескольких OS.

### Связанные файлы

- `.github/workflows/ci.yaml:11-14`
- `docs/ci.md`

---

## Улучшение 11: Настроить Dependabot

**ID:** `imp-011`  
**Тип:** `security`  
**Приоритет:** P2  
**Трудозатраты:** S (30 минут)

### Описание

Настроить Dependabot для автоматических обновлений зависимостей и GitHub Actions.

### Как сделать

1. Создать `.github/dependabot.yml`:
   ```yaml
   version: 2
   updates:
     - package-ecosystem: pip
       directory: "/"
       schedule:
         interval: weekly
       open-pull-requests-limit: 10
       reviewers:
         - SatoryKono
     - package-ecosystem: github-actions
       directory: "/"
       schedule:
         interval: monthly
   ```
2. Включить Dependabot в настройках репозитория на GitHub.
3. Дождаться первого PR от Dependabot и проверить.
4. Настроить автоматический мерж для minor/patch обновлений (опционально).
5. Документировать в `SECURITY.md` и `docs/contributing.md`.

### Критерии приёмки

- [ ] Файл `.github/dependabot.yml` существует.
- [ ] Dependabot создаёт PR для обновления зависимостей (проверить через неделю).
- [ ] Reviewers назначены автоматически.
- [ ] `SECURITY.md` и `docs/contributing.md` упоминают Dependabot.

### Связанные файлы

- Новый файл: `.github/dependabot.yml`
- `SECURITY.md`
- `docs/contributing.md`

---

## Улучшение 12: Добавить sequence diagrams для критичных флоу

**ID:** `imp-012`  
**Тип:** `docs`  
**Приоритет:** P2  
**Трудозатраты:** M (4–5 часов)

### Описание

Создать sequence diagrams в Mermaid для критичных сценариев: полный ETL pipeline, обработка ретраев, rate limiting.

### Как сделать

1. Идентифицировать топ-3 критичных сценария:
   - Полный ETL pipeline с API запросами
   - Обработка HTTP ошибок с backoff
   - Rate limiting и circuit breaker
2. Создать файлы в `docs/diagrams/`:
   - `etl_pipeline_sequence.md`
   - `http_retry_sequence.md`
   - `rate_limiting_sequence.md`
3. Для каждого файла создать Mermaid sequenceDiagram:
   ```mermaid
   sequenceDiagram
       CLI->>Config: load(config.yaml)
       Config->>CLI: Config object
       CLI->>ETL: run_pipeline(config)
       ETL->>APIClient: fetch_data()
       ...
   ```
4. Добавить ссылки на диаграммы в `docs/architecture.md` и `README.md`.
5. Обновить при изменении логики (добавить в чек-лист релиза).

### Критерии приёмки

- [ ] Созданы 3 sequence диаграммы в `docs/diagrams/`.
- [ ] Диаграммы читаются и отражают реальный код.
- [ ] `docs/architecture.md` ссылается на sequence diagrams.
- [ ] `README.md` содержит раздел "Диаграммы" со ссылками.

### Связанные файлы

- Новые файлы: `docs/diagrams/*.md` (3 файла)
- `docs/architecture.md`
- `README.md`

---

## Улучшение 13: Разделить tools/ на скрипты и утилиты

**ID:** `imp-013`  
**Тип:** `arch`  
**Приоритет:** P2  
**Trудозатраты:** M (3 часа)

### Описание

Рефакторинг `src/library/tools/`: CLI-утилиты перенести в `scripts/`, библиотечные модули оставить в `tools/`.

### Как сделать

1. Аудит `src/library/tools/` (17 файлов): определить, какие файлы — CLI скрипты (имеют `if __name__ == '__main__'`), а какие — библиотечные утилиты.
2. CLI скрипты (e.g., `get_pubmed_api_key.py`, `quick_api_check.py`) переместить в `scripts/`.
3. Обновить импорты в скриптах на абсолютные пути (`from library.config import ...`).
4. Обновить `README.md` и `docs/operations.md`, заменив пути к скриптам.
5. Обновить `Makefile`, если там ссылаются на эти скрипты.
6. Запустить тесты: `pytest -v`.
7. Проверить линтеры: `ruff check .` и `mypy src/`.

### Критерии приёмки

- [ ] CLI скрипты перенесены из `src/library/tools/` в `scripts/`.
- [ ] Библиотечные утилиты остались в `src/library/tools/`.
- [ ] Все импорты обновлены и корректны.
- [ ] Тесты проходят.
- [ ] Документация обновлена с новыми путями.
- [ ] MyPy и ruff проходят без ошибок.

### Связанные файлы

- `src/library/tools/` (17 файлов)
- `scripts/`
- `README.md`
- `docs/operations.md`
- `Makefile`

---

## Улучшение 14: Добавить requests-cache для HTTP кэширования

**ID:** `imp-014`  
**Тип:** `perf`  
**Приоритет:** P2  
**Трудозатраты:** M (4 часа)

### Описание

Интегрировать `requests-cache` для кэширования HTTP ответов от API, снижая нагрузку на внешние сервисы и ускоряя повторные запуски.

### Как сделать

1. Добавить зависимость в `pyproject.toml`:
   ```toml
   dependencies = [
       ...
       "requests-cache>=1.0",
   ]
   ```
2. Обновить `src/library/clients/session.py` (или base.py):
   ```python
   import requests_cache
   
   def get_cached_session(backend='sqlite', expire_after=3600):
       return requests_cache.CachedSession(
           'bioactivity_cache',
           backend=backend,
           expire_after=expire_after,
       )
   ```
3. Добавить конфигурацию в `config.yaml`:
   ```yaml
   http:
     global:
       cache:
         enabled: true
         backend: sqlite
         expire_after: 3600  # seconds
   ```
4. Обновить `Config` pydantic model с полями для cache.
5. Обновить инициализацию сессии в `BaseApiClient`.
6. Добавить CLI флаг `--no-cache` для отключения кэша.
7. Документировать в `docs/configuration.md` и `README.md`.
8. Добавить тесты для кэша в `tests/test_clients.py`.

### Критерии приёмки

- [ ] Зависимость `requests-cache` добавлена.
- [ ] HTTP кэш настраивается через конфигурацию.
- [ ] CLI флаг `--no-cache` работает.
- [ ] Тесты для кэша написаны и проходят.
- [ ] Документация обновлена.
- [ ] Повторные запуски пайплайна с кэшем ≥2x быстрее (измерить бенчмарком).

### Связанные файлы

- `configs/pyproject.toml` (dependencies)
- `src/library/clients/session.py` или `base.py`
- `src/library/config.py` (добавить cache config)
- `src/library/cli/__init__.py` (CLI флаг)
- `tests/test_clients.py`
- `docs/configuration.md`
- `README.md`

---

## Улучшение 15: Настроить автоматический cleanup старых логов

**ID:** `imp-015`  
**Тип:** `devx`  
**Приоритет:** P3  
**Трудозатраты:** S (1 час)

### Описание

Автоматизировать удаление старых логов (>14 дней) при старте пайплайна, используя существующую функцию `cleanup_old_logs`.

### Как сделать

1. Обновить `src/library/logging_setup.py`, добавить вызов `cleanup_old_logs()` в `configure_logging()`:
   ```python
   if logging_config and logging_config.get("file", {}).get("cleanup_on_start", False):
       cleanup_old_logs(
           older_than_days=logging_config.get("file", {}).get("retention_days", 14),
           logs_dir=Path(logging_config.get("file", {}).get("path", "logs/app.log")).parent
       )
   ```
2. Обновить `configs/logging.yaml`:
   ```yaml
   file:
     cleanup_on_start: true
     retention_days: 14
   ```
3. Добавить CLI флаг `--cleanup-logs` для принудительного cleanup.
4. Документировать в `docs/operations.md`.
5. Добавить тест для `cleanup_old_logs` в `tests/test_utils_logging.py`.

### Критерии приёмки

- [ ] Функция `cleanup_old_logs` вызывается при старте, если `cleanup_on_start=true`.
- [ ] Старые логи (>14 дней) удаляются успешно.
- [ ] CLI флаг `--cleanup-logs` работает.
- [ ] Тест для cleanup написан и проходит.
- [ ] `docs/operations.md` документирует механизм cleanup.

### Связанные файлы

- `src/library/logging_setup.py:279-303`
- `configs/logging.yaml`
- `src/library/cli/__init__.py` (CLI флаг)
- `tests/test_utils_logging.py`
- `docs/operations.md`

---

## Улучшение 16: Добавить CODE_OF_CONDUCT.md

**ID:** `imp-016`  
**Тип:** `docs`  
**Приоритет:** P3  
**Трудозатраты:** S (15 минут)

### Описание

Создать `CODE_OF_CONDUCT.md` в корне репозитория, используя шаблон Contributor Covenant.

### Как сделать

1. Скачать шаблон Contributor Covenant (версия 2.1): https://www.contributor-covenant.org/version/2/1/code_of_conduct/
2. Скопировать текст в `CODE_OF_CONDUCT.md`.
3. Заменить placeholder `[INSERT CONTACT METHOD]` на email: `security@example.com` (или реальный контакт).
4. Добавить ссылку на `CODE_OF_CONDUCT.md` в `README.md` (секция "Лицензия и вклад").
5. Обновить `CONTRIBUTING.md`, добавив раздел "Кодекс поведения".
6. Проверить, что GitHub UI показывает "Code of conduct" на странице репо.

### Критерии приёмки

- [ ] Файл `CODE_OF_CONDUCT.md` существует в корне.
- [ ] GitHub UI показывает "Code of conduct".
- [ ] `README.md` и `CONTRIBUTING.md` ссылаются на `CODE_OF_CONDUCT.md`.
- [ ] Контактная информация для жалоб указана корректно.

### Связанные файлы

- Новый файл: `CODE_OF_CONDUCT.md`
- `README.md`
- `CONTRIBUTING.md`

---

## Улучшение 17: Добавить .editorconfig

**ID:** `imp-017`  
**Тип:** `devx`  
**Приоритет:** P3  
**Трудозатраты:** S (15 минут)

### Описание

Создать `.editorconfig` для унификации настроек редактора (отступы, кодировка, перевод строки).

### Как сделать

1. Создать `.editorconfig`:
   ```ini
   root = true
   
   [*]
   charset = utf-8
   end_of_line = lf
   insert_final_newline = true
   trim_trailing_whitespace = true
   
   [*.py]
   indent_style = space
   indent_size = 4
   
   [*.{yaml,yml}]
   indent_style = space
   indent_size = 2
   
   [Makefile]
   indent_style = tab
   ```
2. Проверить, что VSCode/PyCharm автоматически подхватывают настройки (установить EditorConfig plugin, если нужно).
3. Добавить упоминание в `docs/development.md`.
4. Закоммитить.

### Критерии приёмки

- [ ] Файл `.editorconfig` существует.
- [ ] Редакторы (VSCode, PyCharm) подхватывают настройки (проверить вручную).
- [ ] `docs/development.md` упоминает `.editorconfig`.

### Связанные файлы

- Новый файл: `.editorconfig`
- `docs/development.md`

---

## Улучшение 18: Добавить профилирование в бенчмарки

**ID:** `imp-018`  
**Тип:** `perf`  
**Приоритет:** P3  
**Трудозатраты:** M (3 часа)

### Описание

Интегрировать `py-spy` или `cProfile` для профилирования производительности в бенчмарках, выявляя узкие места.

### Как сделать

1. Добавить зависимость:
   ```toml
   [project.optional-dependencies]
   dev = [
       ...
       "py-spy>=0.3",
   ]
   ```
2. Создать `tests/benchmarks/test_profiling.py`:
   ```python
   import pytest
   from library.etl.run import run_pipeline
   
   @pytest.mark.benchmark
   def test_profile_pipeline(benchmark, sample_config):
       result = benchmark.pedantic(
           run_pipeline,
           args=(sample_config,),
           rounds=1,
       )
   ```
3. Добавить Makefile цель:
   ```makefile
   profile-pipeline:
       py-spy record -o profile.svg -- python -m library.cli pipeline --config configs/config.yaml
   ```
4. Запустить профилирование:
   ```bash
   make profile-pipeline
   ```
5. Проанализировать `profile.svg` (flamegraph) и выявить топ-3 slowest функций.
6. Документировать в `docs/development.md`.

### Критерии приёмки

- [ ] Зависимость `py-spy` добавлена в `[dev]`.
- [ ] Тесты профилирования написаны в `tests/benchmarks/test_profiling.py`.
- [ ] Makefile цель `profile-pipeline` работает.
- [ ] Сгенерирован flamegraph `profile.svg`.
- [ ] `docs/development.md` документирует профилирование.

### Связанные файлы

- `configs/pyproject.toml` (dev dependencies)
- Новый файл: `tests/benchmarks/test_profiling.py`
- `Makefile` (новая цель)
- `docs/development.md`

---

## Улучшение 19: Добавить Docker build в CI

**ID:** `imp-019`  
**Тип:** `ci`  
**Приоритет:** P3  
**Трудозатраты:** M (2 часа)

### Описание

Добавить job в CI для сборки Docker образов и проверки их работоспособности.

### Как сделать

1. Создать новый job в `.github/workflows/ci.yaml`:
   ```yaml
   docker-build:
     runs-on: ubuntu-latest
     steps:
       - uses: actions/checkout@v4
       - name: Set up Docker Buildx
         uses: docker/setup-buildx-action@v2
       - name: Build development image
         run: docker build --target development -t bioactivity-etl:dev .
       - name: Build production image
         run: docker build --target production -t bioactivity-etl:prod .
       - name: Test production image
         run: docker run --rm bioactivity-etl:prod --version
   ```
2. Добавить кэширование Docker слоёв:
   ```yaml
   - name: Cache Docker layers
     uses: actions/cache@v3
     with:
       path: /tmp/.buildx-cache
       key: ${{ runner.os }}-buildx-${{ github.sha }}
       restore-keys: |
         ${{ runner.os }}-buildx-
   ```
3. Опционально: публиковать образы в GitHub Container Registry при push в main.
4. Документировать в `docs/ci.md`.

### Критерии приёмки

- [ ] CI job `docker-build` успешно собирает образы.
- [ ] Тест `--version` проходит в production образе.
- [ ] Docker слои кэшируются между запусками.
- [ ] Время сборки образов приемлемо (≤5 минут).
- [ ] `docs/ci.md` документирует Docker build в CI.

### Связанные файлы

- `.github/workflows/ci.yaml` (новый job)
- `Dockerfile`
- `docs/ci.md`

---

## Улучшение 20: Разрешить TODO в fetch_publications.py

**ID:** `imp-020`  
**Тип:** `arch`  
**Приоритет:** P3  
**Трудозатраты:** L (Large, 6–8 часов)

### Описание

Реализовать метод `fetch_publications` для клиентов, устранив TODO комментарий в `src/scripts/fetch_publications.py:158`.

### Как сделать

1. Изучить TODO: `src/scripts/fetch_publications.py:158`.
2. Проанализировать, какие клиенты должны поддерживать `fetch_publications` (PubMed, Crossref, Semantic Scholar, OpenAlex).
3. Реализовать метод `fetch_publications(query: str, limit: int) -> List[Dict]` в каждом клиенте.
4. Обновить `BaseApiClient` с абстрактным методом (или default implementation).
5. Обновить `src/scripts/fetch_publications.py`, заменив TODO на реальный вызов.
6. Добавить тесты в `tests/test_clients.py` для нового метода.
7. Обновить документацию API клиентов в `docs/api/index.md`.
8. Убедиться, что интеграционные тесты проходят.

### Критерии приёмки

- [ ] Метод `fetch_publications` реализован в 4+ клиентах.
- [ ] TODO комментарий удалён из `fetch_publications.py`.
- [ ] Скрипт `fetch_publications.py` работает и возвращает результаты.
- [ ] Тесты для метода написаны и проходят.
- [ ] `docs/api/index.md` документирует новый метод.

### Связанные файлы

- `src/scripts/fetch_publications.py:158`
- `src/library/clients/base.py`
- `src/library/clients/pubmed.py`
- `src/library/clients/crossref.py`
- `src/library/clients/semantic_scholar.py`
- `src/library/clients/openalex.py`
- `tests/test_clients.py`
- `docs/api/index.md`

---

## Краткая дорожная карта по батч-PR

Рекомендуется объединять улучшения в логические батчи для упрощения ревью и снижения риска конфликтов.

### Батч 1: Критичные DevX улучшения (P1)
**Цель:** Улучшить onboarding и стабильность  
**Улучшения:** #1 (env.example), #2 (deprecated logger), #3 (security configs)  
**Трудозатраты:** S+S+S = 3–5 часов  
**Зависимости:** Нет  
**PR название:** `chore: critical devx improvements (env.example, cleanup deprecated code, security configs)`

### Батч 2: Линтинг и стандарты кода (P1)
**Цель:** Унифицировать стиль, улучшить читаемость  
**Улучшения:** #4 (line length 120)  
**Трудозатраты:** M = 3–4 часа  
**Зависимости:** Батч 1 (избежать конфликтов форматирования)  
**PR название:** `style: reduce line length to 120`

### Батч 3: CI/CD оптимизации (P2)
**Цель:** Ускорить CI, расширить покрытие  
**Улучшения:** #7 (cache), #10 (OS matrix), #11 (Dependabot), #19 (Docker build)  
**Трудозатраты:** S+S+S+M = 4–6 часов  
**Зависимости:** Батч 2 (stable codebase)  
**PR название:** `ci: optimize builds (caching, OS matrix, Docker, Dependabot)`

### Батч 4: Управление зависимостями и производительность (P2)
**Цель:** Детерминизм зависимостей, кэширование HTTP  
**Улучшения:** #6 (lock-files), #14 (requests-cache)  
**Трудозатраты:** M+M = 5–7 часов  
**Зависимости:** Батч 3 (CI stable)  
**PR название:** `feat: add dependency locking and HTTP caching`

### Батч 5: Документация и диаграммы (P2)
**Цель:** Улучшить визуализацию и UX документации  
**Улучшения:** #5 (CODEOWNERS), #8 (CONTRIBUTING в корень), #9 (ER-диаграммы), #12 (sequence diagrams)  
**Трудозатраты:** S+S+M+M = 8–10 часов  
**Зависимости:** Нет (параллельно с батчами 3–4)  
**PR название:** `docs: add diagrams, CODEOWNERS, and improve structure`

### Батч 6: Архитектурный рефакторинг и дополнительные фичи (P2–P3)
**Цель:** Чистка архитектуры, реализация TODO  
**Улучшения:** #13 (разделить tools/), #15 (cleanup logs), #16 (CODE_OF_CONDUCT), #17 (.editorconfig), #18 (profiling), #20 (fetch_publications TODO)  
**Трудозатраты:** M+S+S+S+M+L = 15–20 часов  
**Зависимости:** Все предыдущие батчи (финальный polish)  
**PR название:** `refactor: architecture cleanup and feature completions`

---

## Примечания

- **Порядок выполнения:** Строго по приоритетам P1 → P2 → P3, затем по трудозатратам S → M → L внутри приоритета.
- **Тестирование:** После каждого улучшения запускать полный CI: `pytest`, `mypy`, `ruff`, `black`.
- **Документация:** Обновлять `docs/changelog.md` после каждого батч-PR.
- **Версионирование:** После батчей 1–3 (критичные) можно выпустить минорную версию 0.2.0.

---

**Подготовлено:** AI Code Analyst  
**Дата:** 2025-10-17  
**Общая оценка трудозатрат:** ~60–75 часов для всех 20 улучшений
