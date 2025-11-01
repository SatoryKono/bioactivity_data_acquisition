# Отчет о дублировании кода

**Дата**: 2025-01-29  
**Ветка**: test_refactoring_32  
**Аннотация**: Систематический анализ дубликатов кода и функционала в репозитории. Выявлены кластеры типа Duplicate (копипаста), Alternative (альтернативные реализации) и Complementary (фрагменты, которые должны быть композицией одного модуля).

## Сводная таблица кластеров

| cluster_id | type | anchor | clones | similarity | diff_summary | merge_target | refactoring_steps | risk |
|------------|------|--------|--------|------------|--------------|--------------|-------------------|------|
| CLI-001 | Duplicate | [ref: repo:src/bioetl/cli/commands/crossref.py@test_refactoring_32] | [ref: repo:src/bioetl/cli/commands/openalex.py@test_refactoring_32], [ref: repo:src/bioetl/cli/commands/pubmed.py@test_refactoring_32], [ref: repo:src/bioetl/cli/commands/semantic_scholar.py@test_refactoring_32] | 95% | Только имена pipeline/источника и config paths различаются | `src/bioetl/cli/commands/external_source.py` | Создать generic factory с параметризацией | Low |
| PIPELINE-001 | Complementary | [ref: repo:src/bioetl/sources/crossref/pipeline.py@test_refactoring_32] | [ref: repo:src/bioetl/sources/openalex/pipeline.py@test_refactoring_32], [ref: repo:src/bioetl/sources/pubmed/pipeline.py@test_refactoring_32] | 90% | Идентичная структура, только adapter_definition и поля различаются | `src/bioetl/pipelines/external_source.py` | Уже используется базовый класс, но можно унифицировать adapter_definition | Low |
| HTTP-001 | Alternative | [ref: repo:src/bioetl/core/api_client.py@test_refactoring_32] | [ref: repo:src/bioetl/utils/chembl.py@test_refactoring_32] | 30% | `utils/chembl.py` использует прямой `requests.get()` вместо `UnifiedAPIClient` | `src/bioetl/utils/chembl.py` | Заменить `requests.get()` на вызов через `UnifiedAPIClient` или создать клиент-обертку | Medium |
| OUTPUT-001 | Complementary | [ref: repo:src/bioetl/core/output_writer.py@test_refactoring_32] | [ref: repo:src/bioetl/utils/output.py@test_refactoring_32] | 40% | `utils/output.py` - утилиты нормализации перед записью, `core/output_writer.py` - атомарная запись | Разделение ролей корректно, но можно улучшить интеграцию | Проверить, что все используют UnifiedOutputWriter для записи | Low |

## Детализация кластеров

### CLI-001: Дублирование CLI команд для external sources

**Тип**: Duplicate  
**Подобие**: 95%

**Якорь**:
[ref: repo:src/bioetl/cli/commands/crossref.py@test_refactoring_32]

**Клоны**:
- [ref: repo:src/bioetl/cli/commands/openalex.py@test_refactoring_32]
- [ref: repo:src/bioetl/cli/commands/pubmed.py@test_refactoring_32]
- [ref: repo:src/bioetl/cli/commands/semantic_scholar.py@test_refactoring_32]

**Различия**:
- Имя pipeline: `CrossrefPipeline`, `OpenAlexPipeline`, `PubMedPipeline`, `SemanticScholarPipeline`
- Имя команды: `crossref`, `openalex`, `pubmed`, `semantic_scholar`
- Путь к конфигу: `pipelines/crossref.yaml`, `pipelines/openalex.yaml`, и т.д.
- Описание: различается только источник

**План слияния**:
1. Создать generic factory `build_external_source_command_config(source_name: str)`
2. Использовать реестр pipeline для получения класса
3. Параметризовать config path и defaults
4. Удалить 4 дублирующихся файла

**Риски**: Low - структура идентична, изменения тривиальны

### PIPELINE-001: Комплементарные pipeline определения

**Тип**: Complementary  
**Подобие**: 90%

**Якорь**: [ref: repo:src/bioetl/sources/crossref/pipeline.py@test_refactoring_32]

**Клоны**:
- [ref: repo:src/bioetl/sources/openalex/pipeline.py@test_refactoring_32]
- [ref: repo:src/bioetl/sources/pubmed/pipeline.py@test_refactoring_32]
- [ref: repo:src/bioetl/sources/semantic_scholar/pipeline.py@test_refactoring_32]

**Анализ**:
Все используют `ExternalSourcePipeline` как базовый класс, что корректно. Но определения `*_ADAPTER_DEFINITION` повторяют одинаковую структуру с разными значениями. Возможна унификация через конфигурацию.

**План слияния**:
1. Перенести adapter_definition в YAML конфиги
2. Унифицировать загрузку через `configs/pipelines/*.yaml`
3. Оставить минимальные Python-классы только для type hints

**Риски**: Low - базовый класс уже унифицирует логику

### HTTP-001: Альтернативная реализация HTTP-запросов

**Тип**: Alternative  
**Подобие**: 30%

**Якорь**: [ref: repo:src/bioetl/core/api_client.py@test_refactoring_32]

**Клон**: [ref: repo:src/bioetl/utils/chembl.py@test_refactoring_32]

**Анализ**:
Функция `_request_status()` в `utils/chembl.py:114` использует прямой вызов `requests.get()` вместо унифицированного `UnifiedAPIClient`. Это нарушает архитектурное правило о централизации сетевых вызовов.

**Фрагмент кода**:
```python
def _request_status(base_url: str) -> Mapping[str, Any]:
    full_url = urljoin(base_url.rstrip("/") + "/", "status.json")
    response = requests.get(full_url, timeout=30)  # Прямой вызов requests
    response.raise_for_status()
    payload: Mapping[str, Any] = response.json()
    return payload
```

**План слияния**:
1. Изменить сигнатуру `fetch_chembl_release()` для обязательного использования `UnifiedAPIClient`
2. Удалить функцию `_request_status()` или переписать её на `api_client.request_json()`
3. Обновить все вызовы `fetch_chembl_release(str)` на использование клиента

**Риски**: Medium - требуется обновление всех мест использования

### OUTPUT-001: Комплементарные модули записи

**Тип**: Complementary  
**Подобие**: 40%

**Анализ**:
- `core/output_writer.py` - атомарная запись с валидацией, QC метриками
- `utils/output.py` - утилиты нормализации данных перед записью (`finalize_output_dataset`)

Роли разделены корректно, но нужно проверить, что все pipeline используют `UnifiedOutputWriter` для финальной записи.

**Риски**: Low - разделение ролей обосновано

## Статистика

- **Всего кластеров**: 4
- **Duplicate**: 1
- **Alternative**: 1
- **Complementary**: 2
- **Топ-5 по размеру**: CLI-001 (4 файла), PIPELINE-001 (4 файла), HTTP-001 (1 файл), OUTPUT-001 (2 файла)

## Рекомендации

1. **Приоритет 1 (High)**: HTTP-001 - устранение прямых `requests` вызовов вне `client/`
2. **Приоритет 2 (Medium)**: CLI-001 - унификация CLI команд для уменьшения копипасты
3. **Приоритет 3 (Low)**: PIPELINE-001 - рассмотреть вынос adapter_definition в конфиги
4. **Приоритет 4 (Low)**: OUTPUT-001 - проверить использование UnifiedOutputWriter везде

