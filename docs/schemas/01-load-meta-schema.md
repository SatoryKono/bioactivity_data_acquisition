# 01 load-meta schema

Слой `load_meta` фиксирует происхождение каждой загрузки из внешних источников.
Таблица хранится в формате Parquet и валидируется схемой `LoadMetaSchema`.

## Колонки

| Колонка               | Описание                                                                       |
| --------------------- | ------------------------------------------------------------------------------ |
| `load_meta_id`        | UUID запроса, используется как внешний ключ во всех доменных таблицах          |
| `source_system`       | Каноническое имя источника (`chembl_rest`, `uniprot_idmapping`, `iuphar_rest`) |
| `source_release`      | Идентификатор релиза/дампа источника                                           |
| `source_api_version`  | Версия API на момент запроса                                                   |
| `request_base_url`    | Базовый URL конечной точки                                                     |
| `request_params_json` | Канонизированный JSON c параметрами запроса (`sort_keys=True`, UTF-8)          |
| `pagination_meta`     | JSON с агрегацией страниц (список событий пагинации)                           |
| `request_started_at`  | UTC таймстемп начала вызова                                                    |
| `request_finished_at` | UTC таймстемп окончания HTTP-вызова                                            |
| `ingested_at`         | UTC таймстемп фиксации в lake                                                  |
| `records_fetched`     | Количество записей, полученных по запросу                                      |
| `status`              | `success`, `warning` или `error`                                               |
| `error_message_opt`   | Текст ошибки (при наличии)                                                     |
| `retry_count`         | Количество повторных попыток                                                   |
| `job_id`              | Идентификатор джоба/оркестратора                                               |
| `operator`            | Человек/пайплайн, инициировавший загрузку                                      |
| `notes`               | Свободный комментарий                                                          |

## Стратегия логирования и связывания

- Каждый HTTP-вызов регистрируется через `LoadMetaStore`: `begin_record` →
  `update_pagination` → `finish_record`.
- `ChemblClient.paginate` автоматически прокидывает `load_meta_id` в каждую
  запись; пайплайны добавляют колонку напрямую в итоговые датафреймы.
- При ошибкаx `finish_record` фиксирует `status="error"`, текст исключения и
  счётчик ретраев.

## Тесты

- `tests/bioetl/schemas/test_load_meta.py` — позитивный и негативные кейсы на
  схему.
- `tests/bioetl/core/test_load_meta_store.py` — проверка атомарной записи и
  трассировки пагинации.
