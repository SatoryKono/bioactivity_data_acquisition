# План улучшений качества кода: 20 приоритетных задач

## Сортировка: по приоритету (P1→P2→P3), затем по трудозатратам (S→M→L)

---

## P1: Критические улучшения (7 задач)

### 1. Добавить .env.example с документированными переменными окружения

**Тип:** `docs` | **Приоритет:** `P1` | **Трудозатраты:** `S`

**Описание:**
1. Создать `.env.example` в корне репозитория со всеми необходимыми env vars
2. Скопировать список из `README.md:163-175` и добавить placeholder значения
3. Добавить комментарии о том, где получить API ключи
4. Обновить `.gitignore`, чтобы исключить `.env`, но не `.env.example`
5. Добавить ссылку на `.env.example` в Getting Started секции README

**Критерии приёмки:**
- [ ] Файл `.env.example` существует в корне
- [ ] Содержит все env vars из `README.md:163-175`
- [ ] Каждая переменная имеет placeholder значение и комментарий
- [ ] `.gitignore` корректно настроен
- [ ] README ссылается на `.env.example`

**Связанные файлы:**
- `README.md:163-175`
- `.gitignore:4`
- Новый файл: `.env.example`

---

### 2. Настроить Dependabot для автоматических обновлений зависимостей

**Тип:** `ci` | **Приоритет:** `P1` | **Трудозатраты:** `S`

**Описание:**
1. Создать `.github/dependabot.yml` с конфигурацией для pip ecosystem
2. Настроить еженедельные обновления для production dependencies
3. Настроить ежемесячные обновления для dev dependencies
4. Добавить auto-merge для patch версий через GitHub Actions
5. Настроить группировку обновлений по категориям (security, minor, major)

**Критерии приёмки:**
- [ ] Файл `.github/dependabot.yml` создан
- [ ] Dependabot создаёт PR для обновлений
- [ ] Security updates имеют высокий приоритет
- [ ] Настроена группировка по типам обновлений
- [ ] Auto-merge работает для patch версий

**Связанные файлы:**
- Новый файл: `.github/dependabot.yml`
- `pyproject.toml:12-48` (зависимости)

---

### 3. Добавить upper bounds на критические зависимости

**Тип:** `deps` | **Приоритет:** `P1` | **Трудозатраты:** `S`

**Описание:**
1. Добавить upper bounds для pandas, pydantic, typer с учётом мажорных версий
2. Использовать формат `package>=X.Y,<(X+1).0` для избежания breaking changes
3. Протестировать установку с новыми constraints
4. Добавить комментарии о причинах ограничений
5. Обновить CI для проверки совместимости с минимальными и максимальными версиями

**Критерии приёмки:**
- [ ] Upper bounds добавлены для pandas, pydantic, typer, requests, structlog
- [ ] CI проходит с текущими зависимостями
- [ ] Документированы причины выбора границ
- [ ] Тесты проходят на минимальной и максимальной версиях
- [ ] README обновлён с информацией о версионных ограничениях

**Связанные файлы:**
- `pyproject.toml:12-32`
- `.github/workflows/ci.yaml`

---

### 4. Создать CODEOWNERS файл для автоматического review assignment

**Тип:** `devx` | **Приоритет:** `P1` | **Трудозатраты:** `S`

**Описание:**
1. Создать `.github/CODEOWNERS` с ответственными за разные части кода
2. Назначить владельцев для `src/library/clients/`, `src/library/etl/`, `tests/`
3. Установить глобального владельца для критических файлов (pyproject.toml, CI configs)
4. Добавить команду/группу для docs review
5. Документировать процесс в `CONTRIBUTING.md`

**Критерии приёмки:**
- [ ] Файл `.github/CODEOWNERS` создан
- [ ] Указаны владельцы для всех критических директорий
- [ ] GitHub автоматически запрашивает review от владельцев
- [ ] `CONTRIBUTING.md` обновлён с информацией о CODEOWNERS
- [ ] Тестовый PR показывает корректное назначение reviewers

**Связанные файлы:**
- Новый файл: `.github/CODEOWNERS`
- `docs/contributing.md`

---

### 5. Добавить SECURITY.md с процедурой раскрытия уязвимостей

**Тип:** `security` | **Приоритет:** `P1` | **Трудозатраты:** `S`

**Описание:**
1. Создать `SECURITY.md` в корне с шаблоном GitHub security policy
2. Указать поддерживаемые версии проекта
3. Описать процесс приватного раскрытия уязвимостей
4. Добавить контактную информацию (email/security issue)
5. Указать SLA на ответ и патчи

**Критерии приёмки:**
- [ ] Файл `SECURITY.md` создан в корне
- [ ] Содержит секцию "Supported Versions"
- [ ] Описан процесс раскрытия уязвимостей
- [ ] Указаны контакты для security reports
- [ ] Документирован SLA на ответ (например, 72 часа)

**Связанные файлы:**
- Новый файл: `SECURITY.md`
- `.safety_policy.yaml`
- `.bandit`

---

### 6. Усилить Pandera schemas: перейти на strict mode для production

**Тип:** `data-quality` | **Приоритет:** `P1` | **Трудозатраты:** `M`

**Описание:**
1. Установить `strict = True` в `RawBioactivitySchema` и `NormalizedBioactivitySchema`
2. Пересмотреть все `nullable=True` поля — оставить только действительно опциональные
3. Добавить Pandera checks для допустимых значений (например, `activity_value > 0`)
4. Создать отдельные schemas для разных источников данных (ChEMBL, Crossref, etc.)
5. Добавить тесты для валидации с invalid data

**Критерии приёмки:**
- [ ] `strict = True` для всех production schemas
- [ ] Документированы причины для каждого `nullable=True`
- [ ] Добавлены Pandera checks для бизнес-правил
- [ ] Созданы source-specific schemas
- [ ] Тесты покрывают позитивные и негативные сценарии валидации

**Связанные файлы:**
- `src/library/schemas/input_schema.py:39-42`
- `src/library/schemas/output_schema.py:29-31`
- `tests/test_validation.py`

---

### 7. Добавить кэширование и артефакты в CI workflow

**Тип:** `ci` | **Приоритет:** `P1` | **Трудозатраты:** `M`

**Описание:**
1. Добавить `actions/cache@v3` для pip cache в `.github/workflows/ci.yaml`
2. Кэшировать `.mypy_cache/`, `.ruff_cache/`, `.pytest_cache/`
3. Настроить upload артефактов для coverage reports, benchmark results
4. Добавить badge в README для CI status и coverage
5. Оптимизировать матрицу: fail-fast для быстрой обратной связи

**Критерии приёмки:**
- [ ] Pip cache настроен и работает
- [ ] Время выполнения CI сокращено минимум на 30%
- [ ] Coverage reports загружаются как артефакты
- [ ] README содержит CI status badge
- [ ] Fail-fast настроен корректно

**Связанные файлы:**
- `.github/workflows/ci.yaml`
- `README.md` (для badges)

---

## P2: Важные улучшения (8 задач)

### 8. Создать диаграммы архитектуры в Mermaid

**Тип:** `docs` | **Приоритет:** `P2` | **Трудозатраты:** `M`

**Описание:**
1. Создать `docs/diagrams/architecture.mmd` с high-level архитектурой системы
2. Создать `docs/diagrams/etl_flow.mmd` с детальным потоком ETL процесса
3. Создать `docs/diagrams/client_hierarchy.mmd` для HTTP clients структуры
4. Встроить диаграммы в `docs/architecture.md` через Mermaid blocks
5. Добавить легенду с объяснением нотации

**Критерии приёмки:**
- [ ] 3 Mermaid диаграммы созданы в `docs/diagrams/`
- [ ] Диаграммы отображаются в MkDocs сайте
- [ ] `docs/architecture.md` содержит встроенные диаграммы
- [ ] Добавлена легенда/описание для каждой диаграммы
- [ ] Диаграммы синхронизированы с кодом (проверено ревьюером)

**Связанные файлы:**
- `docs/architecture.md`
- Новые файлы: `docs/diagrams/*.mmd`
- `mkdocs.yml:33` (mermaid2 plugin)

---

### 9. Добавить объяснение для исключённого из coverage файла

**Тип:** `tests` | **Приоритет:** `P2` | **Трудозатраты:** `S`

**Описание:**
1. Изучить причину исключения `src/library/io_/normalize.py` из coverage
2. Добавить inline комментарий в `pyproject.toml` с объяснением
3. Если файл deprecated — переместить в `src/library/io_/_legacy/`
4. Если тестируется в integration tests — документировать это
5. Рассмотреть возможность повышения coverage для этого файла

**Критерии приёмки:**
- [ ] Комментарий с объяснением добавлен в `pyproject.toml:76`
- [ ] Если deprecated — файл перемещён в `_legacy/`
- [ ] Если integration-only — это документировано
- [ ] Issue создан для будущего удаления/рефакторинга (если применимо)

**Связанные файлы:**
- `pyproject.toml:74-77`
- `src/library/io_/normalize.py`

---

### 10. Добавить PR и issue templates

**Тип:** `devx` | **Приоритет:** `P2` | **Трудозатраты:** `S`

**Описание:**
1. Создать `.github/pull_request_template.md` с чек-листом из CONTRIBUTING
2. Создать `.github/ISSUE_TEMPLATE/bug_report.yml` для structured bug reports
3. Создать `.github/ISSUE_TEMPLATE/feature_request.yml` для feature proposals
4. Создать `.github/ISSUE_TEMPLATE/config.yml` для custom issue routing
5. Добавить ссылки на templates в `CONTRIBUTING.md`

**Критерии приёмки:**
- [ ] PR template создан и содержит checklist
- [ ] 2 issue templates (bug, feature) созданы в YAML формате
- [ ] Config файл настраивает routing issues
- [ ] Templates валидируются GitHub (проверить в test issue)
- [ ] `CONTRIBUTING.md` ссылается на templates

**Связанные файлы:**
- Новые файлы: `.github/pull_request_template.md`, `.github/ISSUE_TEMPLATE/*.yml`
- `docs/contributing.md`

---

### 11. Рефакторинг tools/ директории: сократить фрагментацию

**Тип:** `arch` | **Приоритет:** `P2` | **Трудозатраты:** `M`

**Описание:**
1. Сгруппировать 17 файлов в `tools/` по назначению: monitoring, validation, formatting
2. Создать subdirectories: `tools/monitoring/`, `tools/validation/`, `tools/formatters/`
3. Объединить схожие скрипты (например, 3 monitor_*.py → один с strategy pattern)
4. Переместить `api_health_check.py` в `clients/` (более подходящее место)
5. Добавить README в `tools/` с описанием каждой категории

**Критерии приёмки:**
- [ ] Файлы сгруппированы в 3-4 subdirectories
- [ ] Монолитные скрипты рефакторены в модули
- [ ] `tools/README.md` документирует структуру
- [ ] Все импорты обновлены и тесты проходят
- [ ] Уменьшено количество top-level файлов минимум на 50%

**Связанные файлы:**
- `src/library/tools/` (17 файлов)
- Тесты, использующие эти модули

---

### 12. Добавить property-based тестирование с Hypothesis

**Тип:** `tests` | **Приоритет:** `P2` | **Трудозатраты:** `M`

**Описание:**
1. Добавить `hypothesis>=6.0` в dev dependencies
2. Создать `tests/property/test_transforms.py` для unit conversions
3. Создать `tests/property/test_normalization.py` для data transformations
4. Добавить strategies для генерации валидных DataFrame inputs
5. Интегрировать Hypothesis с Pandera для schema-based generation

**Критерии приёмки:**
- [ ] Hypothesis добавлен в `pyproject.toml`
- [ ] 2 модуля property tests созданы
- [ ] Минимум 5 property-based тестов написаны
- [ ] CI запускает property tests с достаточным количеством examples (1000+)
- [ ] Документация обновлена с примерами property tests

**Связанные файлы:**
- `pyproject.toml:35-48` (dev dependencies)
- Новые файлы: `tests/property/*.py`
- `docs/quality.md`

---

### 13. Внедрить atomic writes для CSV экспорта

**Тип:** `data-quality` | **Приоритет:** `P2` | **Трудозатраты:** `M`

**Описание:**
1. Реализовать atomic write pattern: запись в `.tmp` → rename
2. Обновить `write_deterministic_csv()` для использования atomic writes
3. Добавить file locking через `fcntl` (Unix) / `msvcrt` (Windows)
4. Обработать исключения с cleanup temp files
5. Добавить тесты для concurrent writes и crash recovery

**Критерии приёмки:**
- [ ] Atomic write реализован через temp file + rename
- [ ] File locking работает на Linux и Windows
- [ ] Temp files очищаются при ошибках
- [ ] Тесты покрывают concurrent writes
- [ ] Документация обновлена с описанием atomic behavior

**Связанные файлы:**
- `src/library/etl/load.py`
- `src/library/io_/read_write.py`
- Новые тесты: `tests/test_atomic_io.py`

---

### 14. Настроить мутационное тестирование (mutmut/cosmic-ray)

**Тип:** `tests` | **Приоритет:** `P2` | **Трудозатраты:** `M`

**Описание:**
1. Добавить `mutmut` в dev dependencies
2. Создать `.mutmut.toml` для конфигурации
3. Запустить мутационное тестирование на критических модулях (`config.py`, `base.py`)
4. Повысить покрытие для низкоскоринговых мутантов
5. Добавить mutation testing в CI как optional check

**Критерии приёмки:**
- [ ] `mutmut` установлен и настроен
- [ ] Mutation score ≥80% для критических модулей
- [ ] CI запускает mutation tests (с allow_failure)
- [ ] Документированы найденные недостатки в тестах
- [ ] Создан baseline для отслеживания прогресса

**Связанные файлы:**
- `pyproject.toml` (dev deps)
- Новый файл: `.mutmut.toml`
- `.github/workflows/ci.yaml`

---

### 15. Добавить HTTP response caching для идемпотентных запросов

**Тип:** `perf` | **Приоритет:** `P2` | **Трудозатраты:** `L`

**Описание:**
1. Интегрировать `requests-cache` для GET запросов
2. Настроить SQLite backend для кэша с TTL по источникам
3. Добавить cache headers в конфигурацию (`Cache-Control`, `ETag`)
4. Реализовать cache invalidation logic для force refresh
5. Добавить CLI флаг `--no-cache` для обхода кэша

**Критерии приёмки:**
- [ ] `requests-cache` интегрирован в `BaseApiClient`
- [ ] Кэш настроен с reasonable TTL (1 час для metadata, 24 часа для documents)
- [ ] Cache hit/miss логируется
- [ ] CLI флаг `--no-cache` работает
- [ ] Benchmark показывает 50%+ ускорение для повторных запросов

**Связанные файлы:**
- `src/library/clients/base.py`
- `src/library/clients/session.py`
- `src/library/cli/__init__.py`

---

## P3: Желательные улучшения (5 задач)

### 16. Добавить Ruff docstring правила (D-категория)

**Тип:** `lint` | **Приоритет:** `P3` | **Трудозатраты:** `M`

**Описание:**
1. Добавить `"D"` в `ruff.lint.select` для docstring linting
2. Настроить per-file ignores для tests (D100, D101, D102)
3. Выбрать docstring convention (Google/NumPy) — уже используется Google в mkdocstrings
4. Добавить missing docstrings для публичных функций/классов
5. Обновить pre-commit hook для проверки docstrings

**Критерии приёмки:**
- [ ] `"D"` добавлен в `pyproject.toml:87`
- [ ] Выбрана и документирована Google convention
- [ ] Все публичные API имеют docstrings
- [ ] Pre-commit проверяет docstrings
- [ ] CI проходит без D-ошибок

**Связанные файлы:**
- `pyproject.toml:83-95`
- `.pre-commit-config.yaml`
- `src/library/` (все модули)

---

### 17. Снизить line-length со 180 до 120 символов

**Тип:** `lint` | **Приоритет:** `P3` | **Трудозатраты:** `M`

**Описание:**
1. Изменить `line-length` в `pyproject.toml` для black и ruff на 120
2. Запустить `black .` для автоформатирования
3. Вручную исправить случаи, где автоформат нарушает читаемость
4. Обновить pre-commit hooks
5. Добавить ADR (Architecture Decision Record) с обоснованием выбора 120

**Критерии приёмки:**
- [ ] `line-length = 120` в `pyproject.toml`
- [ ] Весь код переформатирован
- [ ] Читаемость не ухудшилась (ревью)
- [ ] ADR документирует решение
- [ ] CI проходит с новыми правилами

**Связанные файлы:**
- `pyproject.toml:81`, `pyproject.toml:84`
- Весь код в `src/library/`

---

### 18. Добавить профилирование производительности (py-spy)

**Тип:** `perf` | **Приоритет:** `P3` | **Трудозатраты:** `S`

**Описание:**
1. Добавить `py-spy` в dev dependencies
2. Создать `scripts/profile_pipeline.py` для профилирования полного ETL цикла
3. Генерировать flamegraphs в `reports/profiling/`
4. Добавить Makefile target `make profile`
5. Документировать процесс профилирования в `docs/development.md`

**Критерии приёмки:**
- [ ] `py-spy` установлен
- [ ] Скрипт профилирования работает
- [ ] Flamegraphs генерируются
- [ ] Makefile target `profile` добавлен
- [ ] Документация обновлена

**Связанные файлы:**
- `pyproject.toml` (dev deps)
- Новый файл: `scripts/profile_pipeline.py`
- `Makefile`

---

### 19. Добавить второй static analyzer (Pyright) для cross-validation

**Тип:** `lint` | **Приоритет:** `P3` | **Трудозатраты:** `M`

**Описание:**
1. Добавить `pyright` в dev dependencies
2. Создать `pyrightconfig.json` с strict настройками
3. Синхронизировать настройки с `mypy` (target Python version, strictness)
4. Добавить CI step для pyright проверки
5. Исправить различия между mypy и pyright результатами

**Критерии приёмки:**
- [ ] `pyright` установлен и настроен
- [ ] `pyrightconfig.json` создан
- [ ] CI запускает pyright (после mypy)
- [ ] Оба анализатора проходят без ошибок
- [ ] Документирована причина использования двух analyzers

**Связанные файлы:**
- `pyproject.toml` (dev deps)
- Новый файл: `pyrightconfig.json`
- `.github/workflows/ci.yaml`

---

### 20. Создать ADR (Architecture Decision Records) директорию

**Тип:** `docs` | **Приоритет:** `P3` | **Трудозатраты:** `M`

**Описание:**
1. Создать `docs/adr/` для Architecture Decision Records
2. Создать шаблон ADR в `docs/adr/template.md`
3. Написать первые 3 ADR для критических решений:
   - ADR-001: Выбор Pandera вместо Pydantic для data validation
   - ADR-002: Использование structlog для structured logging
   - ADR-003: Детерминированный вывод через sort + column_order
4. Добавить ADR index в MkDocs navigation
5. Документировать процесс создания ADR в `CONTRIBUTING.md`

**Критерии приёмки:**
- [ ] Директория `docs/adr/` создана
- [ ] Шаблон ADR существует
- [ ] 3 ADR написаны и ревьюированы
- [ ] ADR индексируются в MkDocs
- [ ] `CONTRIBUTING.md` описывает процесс ADR

**Связанные файлы:**
- Новые файлы: `docs/adr/*.md`
- `mkdocs.yml` (navigation)
- `docs/contributing.md`

---

## Дорожная карта: батч-PR с зависимостями

### Batch 1: Foundational DevOps (P1, независимые)
**Задачи:** #1, #2, #4, #5  
**Зависимости:** нет  
**Ожидаемое время:** 1-2 дня  
**Описание:** Базовая инфраструктура для безопасности и автоматизации

### Batch 2: CI/CD Optimization (P1, зависит от Batch 1)
**Задачи:** #3, #7  
**Зависимости:** Batch 1 (особенно #2 для dependabot)  
**Ожидаемое время:** 2-3 дня  
**Описание:** Улучшение CI pipeline и управление зависимостями

### Batch 3: Data Quality Enhancement (P1, независимый)
**Задачи:** #6, #13  
**Зависимости:** нет  
**Ожидаемое время:** 3-4 дня  
**Описание:** Усиление валидации данных и надёжности I/O

### Batch 4: Documentation & Architecture (P2, зависит от Batch 3)
**Задачи:** #8, #9, #10, #20  
**Зависимости:** Batch 3 (для актуальной архитектуры)  
**Ожидаемое время:** 3-5 дней  
**Описание:** Визуализация и документирование архитектурных решений

### Batch 5: Code Quality & Testing (P2, независимый)
**Задачи:** #11, #12, #14  
**Зависимости:** нет  
**Ожидаемое время:** 5-7 дней  
**Описание:** Рефакторинг и расширенное тестирование

### Batch 6: Performance & Polish (P2-P3, зависит от всех предыдущих)
**Задачи:** #15, #16, #17, #18, #19  
**Зависимости:** Batch 1-5  
**Ожидаемое время:** 4-6 дней  
**Описание:** Оптимизации производительности и финальный polish

---

## Итоговая оценка

**Общее время реализации:** 18-27 рабочих дней (3.5-5.5 недель)  
**Критический путь:** Batch 1 → Batch 2 → Batch 4 → Batch 6  
**Быстрые победы (Quick Wins):** #1, #2, #4, #5, #9, #10 (можно сделать за 1 неделю)

**Рекомендуемый порядок выполнения:**
1. Начать с Batch 1 (foundational) — обязательно для безопасности
2. Параллельно Batch 3 (data quality) — high impact на надёжность
3. Batch 2 после завершения Batch 1 — для автоматизации CI
4. Batch 4 и 5 можно выполнять параллельно разными людьми
5. Batch 6 в конце, когда всё стабильно работает

