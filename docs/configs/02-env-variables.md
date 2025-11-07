# 02. Переменные окружения и pydantic-settings

> **Статус**: реализовано. Поддерживается `pydantic-settings>=2.0`.

## 1. Принцип работы

1. YAML профили и конфигмердж формируют базовую карту.
2. CLI `--set key=value` накладывает точечные переопределения.
3. «Короткие» переменные окружения (`PUBMED_TOOL`, `SEMANTIC_SCHOLAR_API_KEY` и т.д.) загружаются через `EnvironmentSettings` и конвертируются в вложенные ключи `BIOETL__...`.
4. Переменные окружения с префиксами `BIOETL__` или `BIOACTIVITY__` остаются источником последней мили и имеют высший приоритет.

Модуль `src/bioetl/config/environment.py` реализует связку:

- `load_environment_settings()` — читает `.env` (если присутствует) и валидирует значения.
- `apply_runtime_overrides()` — синхронизирует «короткие» переменные с вложенными `BIOETL__SOURCES__...` ключами, не перетирая ручные overrides.

Шаблоны `.env` расположены в `configs/templates/` и документируют минимальный набор переменных для локальной разработки (`.env.local.template`), общего dev-окружения (`.env.dev.template`) и CI (`.env.ci.template`).
Ключи автоматически приводятся к нижнему регистру, поэтому `BIOETL__SOURCES__PUBMED__HTTP__IDENTIFY__TOOL` превращается в `sources.pubmed.http.identify.tool`.

## 2. Формат переменных

```text
BIOETL__SECTION__SUBSECTION__KEY=value
```

- Префикс (`BIOETL__`, `BIOACTIVITY__`) обязательный.
- Двойное подчеркивание отделяет уровень вложенности.
- Значения приводятся через `yaml.safe_load`, поэтому можно передавать числа, булевы и списки.

### Примеры

```bash
export BIOETL__PIPELINE__NAME=activity_override
export BIOETL__IO__OUTPUT__PARTITION_BY='["year"]'
export BIOACTIVITY__RUNTIME__PARALLELISM=8
export BIOETL__DETERMINISM__HASHING__ALGORITHM=blake2b
```

## 3. Типичные кейсы

- Быстрое изменение путей: `BIOETL__IO__OUTPUT__PATH=/tmp/out.parquet`.
- Переопределение таймаутов без правки YAML: `BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=120`.
- Инжекция секретов и токенов: `BIOETL__SOURCES__CHEMBL__PARAMETERS__api_key=...` (значение не попадает в репозиторий).

## 4. Ограничения

- Пустой сегмент (`BIOETL__`) игнорируется.
- Неизвестные ключи запрещены за счёт `extra="forbid"` в Pydantic-моделях.
- Значения, не распознаваемые `yaml.safe_load`, остаются строкой.

## 5. Рекомендации

1. Скопируйте `configs/templates/.env.local.template` в `.env` и заполните значения из Vault (или договорённого источника).
2. Для продакшна храните секреты в менеджере секретов и прокидывайте через переменные окружения (GitHub Actions, Argo, Airflow и т.п.).
3. После обновления `.env` выполните `export $(grep -v '^#' .env | xargs)` (или аналог для вашей оболочки), затем `pip install -e .[dev]` и `pytest`.
4. Проверьте настройки через `bioetl config inspect` (см. CLI документацию), чтобы убедиться, что значения применились.

Эти рекомендации помогают сохранить детерминизм и повторяемость окружения.

## 6. Карта коротких переменных

| Переменная | Вложенный ключ `BIOETL__...` | Назначение |
| --- | --- | --- |
| `PUBMED_TOOL` | `SOURCES__PUBMED__HTTP__IDENTIFY__TOOL` | Идентификатор клиента для NCBI |
| `PUBMED_EMAIL` | `SOURCES__PUBMED__HTTP__IDENTIFY__EMAIL` | Контактный email для NCBI |
| `PUBMED_API_KEY` | `SOURCES__PUBMED__HTTP__IDENTIFY__API_KEY` | Расширенный лимит NCBI |
| `CROSSREF_MAILTO` | `SOURCES__CROSSREF__IDENTIFY__MAILTO` | Полит pool Crossref |
| `SEMANTIC_SCHOLAR_API_KEY` | `SOURCES__SEMANTIC_SCHOLAR__HTTP__HEADERS__X-API-KEY` | Аутентификация Semantic Scholar |
| `IUPHAR_API_KEY` | `SOURCES__IUPHAR__HTTP__HEADERS__X-API-KEY` | Доступ к IUPHAR API |
