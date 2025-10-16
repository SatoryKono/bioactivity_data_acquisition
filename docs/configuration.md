# Конфигурация

Приоритеты источников конфигурации:

1. Дефолты в коде
2. YAML (`configs/config.yaml`, `configs/config_documents_full.yaml`)
3. Переменные окружения `BIOACTIVITY__*`
4. CLI-переопределения `--set key=value`

## Примеры

```bash
# базовый запуск
bioactivity-data-acquisition pipeline run --config configs/config.yaml

# override через CLI
bioactivity-data-acquisition pipeline run --config configs/config.yaml --set http.global.timeout_sec=10

# override через окружение
export BIOACTIVITY__HTTP__GLOBAL__TIMEOUT_SEC=10
```

## Ключи конфигурации (фрагмент)

| ключ | тип | дефолт | описание |
|---|---|---|---|
| http.global.timeout_sec | float | 30.0 | Таймаут HTTP по умолчанию |
| http.global.retries.total | int | 5 | Общее число попыток |
| http.global.retries.backoff_multiplier | float | 2.0 | Множитель backoff |
| http.global.headers.User-Agent | str | bioactivity-data-acquisition/0.1.0 | Идентификатор клиента |
| sources.chembl.http.base_url | str | <https://www.ebi.ac.uk/chembl/api/data> | Базовый URL ChEMBL |
| sources.crossref.http.base_url | str | <https://api.crossref.org/works> | Базовый URL Crossref |
| sources.openalex.http.base_url | str | <https://api.openalex.org/works> | Базовый URL OpenAlex |
| sources.pubmed.rate_limit.max_calls | int | 2 | Лимит запросов в секунду |
| sources.pubmed.http.base_url | str | <https://eutils.ncbi.nlm.nih.gov/entrez/eutils/> | Базовый URL PubMed |
| sources.semantic_scholar.rate_limit.period | float | 5.0 | Период лимита (сек) |
| io.input.documents_csv | path | data/input/documents.csv | Входной CSV |
| io.output.data_path | path | data/output/documents.csv | Путь датасета |
| io.output.qc_report_path | path | data/output/documents_qc_report.csv | Путь QC |
| io.output.correlation_path | path | data/output/documents_correlation.csv | Путь корреляций |
| runtime.workers | int | 4 | Кол-во потоков |
| runtime.limit | int/null | null | Ограничение записей |
| runtime.dry_run | bool | false | Тестовый режим |
| logging.level | str | INFO | Уровень логов |
| validation.strict | bool | true | Строгая валидация |
| validation.qc.max_missing_fraction | float | 0.05 | Допустимая доля пропусков |
| determinism.sort.by | list[str] | см. файл | Поля сортировки |
| determinism.column_order | list[str] | см. файл | Порядок колонок |

Полный перечень ключей уточняется по `configs/config_documents_full.yaml`.
