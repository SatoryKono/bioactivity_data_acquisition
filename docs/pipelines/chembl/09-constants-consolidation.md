# Консолидация повторяющихся констант и шаблонов

| Конструкция | Текущие файлы | Целевой модуль | Комментарии |
| --- | --- | --- | --- |
| `API_ACTIVITY_FIELDS` + расширенные списки для `assay`, `target`, `document`, `testitem` | `src/bioetl/pipelines/chembl/activity/run.py`, `assay/run.py`, `target/run.py`, `document/normalize.py`, `testitem/*` | `src/bioetl/pipelines/chembl/_constants.py` | Списки идентичны по назначению, различаются только сущностью; перенос обеспечивает единый контроль версий и переиспользование в конфигурациях. |
| `MUST_HAVE_FIELDS`, `REQUIRED_FIELDS`, `DEFAULT_SELECT_FIELDS` | `chembl/assay/run.py`, `chembl/common/descriptor.py` | `src/bioetl/pipelines/chembl/_constants.py` | Жёстко зашитые `select_fields` пересекаются на всех пайплайнах, поэтому выносятся в провайдерский модуль. |
| `RELATIONS`, `STANDARD_TYPES`, справочники валидации | `src/bioetl/schemas/activity.py`, `chembl_activity_schema.py`, `pipelines/chembl/activity/run.py` | `src/bioetl/schemas/_validators.py` (функции проверки) и `src/bioetl/pipelines/chembl/_constants.py` (наборы значений) | Повторяется логика проверки `relation` и `standard_relation`; будет выделен валидатор `validate_relation(series)` с переиспользуемым набором значений. |
| Нормализация `select_fields` и сборка `filters_payload` | `src/bioetl/pipelines/chembl/common/descriptor.py`, `config/{activity,target,testitem,document}/__init__.py`, пайплайны | Новый mixin в `src/bioetl/clients/base.py`; Paginator/descriptor будут использовать единый helper | Позволит избавиться от локальных копипаст и гарантировать единый порядок сериализации списков. |
| Лог-события `page_fetched`, `next_link_resolved`, метрики пагинации | `chembl/common/descriptor.py`, `clients/client_chembl.py`, `clients/http/pagination.py` | Обновлённый `Paginator` + mixin событий | Все HTTP клиенты будут эмитить одинаковые структурные события через `UnifiedLogger`. |
| Конвертеры параметров (строковые ID, сортировки, фильтры) | `chembl/common/descriptor.py`, `activity/run.py`, `assay/run.py` | Новый helper в `src/bioetl/clients/base.py` | Гарантирует единое поведение при сериализации параметров и дедупликации. |

Матрица покрывает все дубликаты из поиска `next_link_resolved|page_fetched|filters_payload|select_fields|STANDARD_TYPES|RELATIONS`. Все переносы выполняются перед обновлением импортов и тестов.
