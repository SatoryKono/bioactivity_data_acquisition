# NAMING_POLICY

## Папки верхнего уровня
Обязательная структура `src/<package>/` с разделами:
- `api/`, `cli/`, `clients/`, `core/`, `pipelines/`, `schemas/`, `utils/`, `tests/`, `config/`, `docs/`.

Внутри `pipelines/`: трехмерная иерархия `<provider>/<entity>/<stage>.py`, где:
- provider: `chembl`, `pubchem`, `uniprot`, `iuphar`, `pubmed`, `openalex`, `crossref`, `semanticscholar`.
- entity: `assay`, `activity`, `target`, `document`, `testitem`.
- stage: `extract`, `transform`, `validate`, `write`, `run`.

Тесты зеркалируют дерево: `tests/<package>/.../test_<module>.py`.

## Политика имен файлов
Формат: `<layer>_<domain>_<action>[_<provider>].py`, где:
- layer ∈ {api, cli, client, service, repo, schema, model, utils, pipeline}
- domain — ключевая сущность (единственное число, snake_case)
- action — `extract|transform|validate|write|run|sync|normalize|resolve|map`
- provider — опционально, в нижнем регистре

Запрещено: пробелы, дефисы, CamelCase.

Документы-руководства: `NN-<topic>.md` (например, `09-document-chembl-extraction.md`).

## Политика имен объектов
Классы — PascalCase; суффиксы: Client, Service, Repository, Validator, Normalizer, Writer.
Базы/интерфейсы — *Base, *ABC. Исключения — *Error. Data-типы — *Model, *Record, *Config.

Функции/методы — глагольные имена: `load_config`, `create_session`, `normalize_row`, `validate_payload`.
Предикаты: `is_`, `has_`, `should_`. Фабрики: `build_`, `create_`, `make_`. I/O: `load_`, `save_`. Приватные: `_name`.

## Регулярные выражения
```
PACKAGE/MODULE: ^[a-z][a-z0-9_]*$
TEST MODULE:    ^test_[a-z0-9_]+\.py$
CLASS:          ^[A-Z][A-Za-z0-9]*$
FUNCTION:       ^[a-z][a-z0-9_]*$
PRIVATE NAME:   ^_[a-z][a-z0-9_]*$
CONST:          ^[A-Z][A-Z0-9_]*$
```

## Матрица «директория → допустимые типы файлов → ожидаемые суффиксы/префиксы»
| Директория | Типы | Ожидаемые паттерны |
|---|---|---|
| api | .py | ^api_.*\.py$ |
| cli | .py | ^cli_.*\.py$ |
| clients | .py | ^.*Client\.py$ (классы), файлы `client_.*\.py` |
| core | .py | без префиксов; базовые классы `*Base`, `*ABC` |
| pipelines | .py | `<provider>/<entity>/<stage>\.py` |
| schemas | .py,.yaml,.yml | `.*_schema\.(py|ya?ml)$` |
| utils | .py | `.*_utils\.py` |
| tests | .py | `test_.*\.py` |
| config | .yaml,.yml,.json | `^[a-z0-9_]+\.(yaml|yml|json)$` |
| docs | .md | `\d{2}-.*\.md` |

## DRY и антидублирование
- Общие утилиты в `utils/` (io_utils.py, string_utils.py, retry_utils.py).
- Единые HTTP/DB клиенты в `clients/` с базой `HttpClientBase`.
- Валидация и схемы в `schemas/` + единый валидатор.
- Перед добавлением новой функции — поиск аналогов и обоснование в PR, если не используется существующий код.
- Единые имена аргументов: `session, logger, config, run_id, limit, offset, timeout`.

## Ключи реестра стратегий
`<provider>:<entity>:<stage>` → класс-стратегия. Пример: `chembl:assay:extract`.