# Управление секретами для BioETL

Этот репозиторий больше не хранит реальные токены и ключи API. Шаблоны окружения
(`configs/templates/.env.key.template` и будущие `.env.*.template`) оставлены
только как каркасы и не должны содержать рабочих значений. Доступ к
секретам осуществляется через менеджер секретов (Vault) или через переменные
окружения, прокинутые в рантайм пайплайнов и CI.

## Шаблоны и политика Vault

- Шаблоны переменных окружения расположены в каталоге
  [`configs/templates/`](configs/templates/).
- Политика управления секретами и доступом описана в
  [`docs/security/00-vault-policy.md`](docs/security/00-vault-policy.md).

## Топология репозитория

Слои кода, тестов, конфигураций и артефактов описаны в разделе
[Repository Topology](docs/repository_topology.md). Используйте его как точку
старта для навигации по проекту и проверки правил размещения артефактов.

## Каталоги данных и образцы

- Полноразмерные выгрузки пайплайнов и отчёты перены в бакет `s3://bioactivity-data-lake/output`
  и публикуются как артефакты CI. Репозиторий хранит только конфигурацию и лёгкие примеры.
- Для демонстрации схем добавлены облегчённые выборки в `data/samples/`. Структура повторяет
  каталоги из `data/output` (например, `data/samples/_documents/documents_sample_20251021.csv`).
- Каталог `data/output/` очищен; в гите остаётся только `.gitkeep`, а попытки добавить крупные
  файлы блокируются новым pre-commit хуком и шагом CI `scripts/check_output_artifacts.py`.

## Стратегия хранения

### Prod / Staging

- **Vault** — источник правды для всех API-ключей (PubMed, Crossref, Semantic
  Scholar, IUPHAR и др.).
- **Названия секретов**: используйте единый префикс `bioetl/<environment>/` и
  храните ключи в формате JSON:

  ```json
  {
    "PUBMED_TOOL": "bioetl-document-pipeline",
    "PUBMED_EMAIL": "ml-team@example.org",
    "PUBMED_API_KEY": "...",
    "CROSSREF_MAILTO": "ml-team@example.org",
    "SEMANTIC_SCHOLAR_API_KEY": "...",
    "IUPHAR_API_KEY": "..."
  }
  ```

- **Доступ**: ограничьте ACL только сервисным аккаунтам пайплайна и членам
  команды данных.

### Локальная разработка

1. Скопируйте `configs/templates/.env.key.template` в `.env` (или `.env.key`, если инструмент
   его ожидает).
2. Выполните `vault kv get bioetl/dev/secrets > env.json` или получите значения
   иным способом от ответственного за секреты.
3. Распакуйте JSON в `.env` (например, `jq -r 'to_entries|map("\(.key)=\(.value)")|.[]' env.json > .env`).
4. Никогда не коммитьте `.env`, `.env.key` или другие файлы с реальными
   значениями.

## Как запускать локально

1. `cp configs/templates/.env.local.template .env`
2. `export $(grep -v '^#' .env | xargs)`
3. `pip install -e .[dev]`
4. `pytest`

`.env` и другие файлы с реальными значениями игнорируются Git (см. `.gitignore`) и
не подлежат коммиту.

### CI / Orchestration

- GitHub Actions/Argo должны читать секреты непосредственно из Vault или из
  настроенных переменных окружения (например, `SEMANTIC_SCHOLAR_API_KEY`), а не
  из файлов в репозитории.
- Рекомендуемые имена секретов: `PUBMED_TOOL`, `PUBMED_EMAIL`, `PUBMED_API_KEY`,
  `CROSSREF_MAILTO`, `SEMANTIC_SCHOLAR_API_KEY`, `IUPHAR_API_KEY`, `VOCAB_STORE`.
- При необходимости используйте GitHub OIDC + Vault для динамического получения
  ключей во время выполнения.

## Ротация ключей

### Crossref `CROSSREF_MAILTO`

- Обратитесь к владельцу учётной записи Crossref и обновите контактный email.
- Обновите запись в Vault и подтвердите, что пайплайны используют новый email.

### Semantic Scholar `SEMANTIC_SCHOLAR_API_KEY`

- Сгенерируйте новый токен в личном кабинете Semantic Scholar.
- Аннулируйте старый токен, т.к. он был опубликован ранее.
- Обновите Vault и оповестите команду о необходимости перезапустить пайплайны.

Для остальных ключей пересмотрите статусы и ротацию в стандартном квартальном
цикле безопасности.

## Локальные артефакты и отчёты

Служебные скрипты и golden-тесты записывают результаты в каталог `artifacts/` в
корне репозитория. Директория автоматически создаётся по мере запуска скриптов и
исключена из Git (`.gitignore`), поэтому внутри можно свободно генерировать
отчёты без риска закоммитить артефакты.

Основные сценарии:

- `make dicts.aggregate` собирает словари ChEMBL в
  `artifacts/chembl_dictionaries.yaml`.
- `python audit_docs.py` формирует CSV/Markdown отчёты по документации (gaps,
  linkcheck и др.) внутри `artifacts/`.
- Скрипты из `scripts/` (`link_check.py`, `inventory_docs.py`,
  `semantic_diff.py`, `validate_config_schemas.py` и т.д.) и golden-тесты из
  `tests/` автоматически создают одноимённые отчёты в `artifacts/`.

Чтобы очистить рабочее окружение, достаточно удалить каталог `artifacts/` (он
будет заново создан при следующем запуске нужных скриптов).

## Pre-commit хуки

- `pipx install pre-commit`
- `pre-commit install`
- `pre-commit install --hook-type commit-msg`
- `pre-commit run --all-files`

Запуск `pre-commit run --all-files --show-diff-on-failure` обязателен перед PR: ровно так же действует шаг CI. Для обновления версий хуков выполните `pre-commit autoupdate`, затем повторно прогоните все проверки и зафиксируйте изменения в `.pre-commit-config.yaml`.

При желании подключите [pre-commit.ci](https://pre-commit.ci/) для зеркалирования локальных проверок в PR.

## Обнаружение утечек секретов

### Автоматическая проверка

CI запускает `detect-secrets` по всем файлам репозитория. Проверка завершается с
ошибкой при обнаружении потенциального секрета. Ложноположительные срабатывания
фиксируйте через `.secrets.baseline` (не забудьте добавить поясняющий комментарий
в PR).

### Проверка перед коммитом

`pre-commit` конфигурация репозитория уже включает хуки `detect-secrets` и
`detect-private-key`. После выполнения `pre-commit install` и
`pre-commit install --hook-type commit-msg` проверки запускаются автоматически.

Для разового локального прогона по всему дереву используйте:

```bash
pre-commit run detect-secrets --all-files
```

Если нужно зафиксировать новое, но безопасное исключение, обновите baseline:

```bash
detect-secrets scan src tests configs scripts docs README.md --baseline .secrets.baseline --update
```

## Реакция на инциденты

1. Немедленно отзовите скомпрометированный ключ в панели управления поставщика
   API.
2. Обновите секрет в Vault и проконтролируйте деплой конфигураций.
3. Создайте инцидент в системе тикетов и задокументируйте временную шкалу.
4. Добавьте регрессионный тест или правило в `detect-secrets`, чтобы предотвратить
   повторение проблемы.

## CLI утилиты

Вспомогательные CLI были сконцентрированы в каталоге `scripts/` и теперь
запускаются единообразно через `python scripts/<имя>.py`. Перед использованием
установите зависимости в editable-режиме (`pip install -e .[dev]`) и выполняйте
команды из корня репозитория, чтобы относительные пути разрешались корректно.

| Команда | Назначение | Пример запуска |
| --- | --- | --- |
| `determinism_check` | Дважды запускает `activity_chembl` и `assay_chembl` в `--dry-run` и сравнивает структурированные логи. | `python scripts/determinism_check.py` |
| `schema_guard` | Валидирует ключевые конфигурации пайплайнов через `bioetl.config.loader` и проверяет поля детерминизма. | `python scripts/schema_guard.py` |
| `doctest_cli` | Извлекает примеры команд из документации, принудительно добавляет `--dry-run` и формирует отчёт о статусах. | `python scripts/doctest_cli.py` |
| `run_test_report` | Запускает `pytest`+coverage, собирает артефакты и пишет `meta.yaml` с контрольными суммами. | `python scripts/run_test_report.py --output-root audit_results/test-reports` |

Полный перечень служебных утилит с артефактами и примерами доступен в
[`docs/cli/03-cli-utilities.md`](docs/cli/03-cli-utilities.md).

## Architecture Decision Records (ADR)

Мы ведём ADR в каталоге [`docs/adr/`](docs/adr/). Чтобы задокументировать архитектурные изменения:

1. Скопируйте шаблон [`docs/adr/template.md`](docs/adr/template.md) в файл `docs/adr/<следующий-номер>-<краткое-имя>.md`.
2. Заполните разделы «Context», «Decision», «Consequences» и добавьте ссылки на код/документацию.
3. Обновите [`docs/INDEX.md`](docs/INDEX.md) — добавьте ссылку на новый ADR в раздел «Architecture Decision Records».
4. Укажите номер ADR в описании PR и отметьте чекбокс ADR в шаблоне PR.
