# План унификации интерфейсов ChEMBL-пайплайнов

## Сводка
1. Пять ChEMBL-пайплайнов (`activity`, `assay`, `document`, `target`, `testitem`) уже построены поверх `UnifiedPipelineBase`, но каждый по‑разному реализует стадии извлечения, трансформаций и валидации, что приводит к дублированию и множеству необязательных хук‑методов.
2. `ChemblPipelineBase` и `UnifiedPipelineBase` предоставляют единый движок (`run_descriptor_extraction`) и mixin’ы логирования/валидации, однако дочерние классы напрямую управляют клиентыми, dry‑run логикой и сводками, нарушая принцип единой ответственности.【F:src/bioetl/chembl/common/descriptor.py†L333-L1180】【F:src/bioetl/pipelines/unified_base.py†L1-L42】
3. Основные расхождения сосредоточены вокруг методов `extract_by_ids`, `pre_transform/domain_enrich/post_transform` против монолитного `transform`, а также вокруг того, как каждый пайплайн объявляет метаданные (handshake, release, qc) и что возвращает.
4. Целевой интерфейс должен разделять контракт извлечения (описанный дескриптором), контракт преобразований (единый `transform` с опциональными хуками) и контракт публикации метаданных (`augment_metadata`, `summarize_extraction`).
5. Требуется переработать базовые классы так, чтобы дочерние пайплайны описывали лишь специфику ChEMBL‑сущности, а остальной цикл (`extract_all`, dry‑run, batched fetcher, логирование) обеспечивался общей реализацией без изменения публичной сигнатуры `PipelineBase.run()`.

## Текущие интерфейсы

| Группа | Класс/модуль | Методы/сигнатуры | Особенности |
| --- | --- | --- | --- |
| Activity | `ChemblActivityPipeline` (`src/bioetl/pipelines/chembl/activity/run.py`) | `resolve_legacy_extract_ids(log, *args, **kwargs) -> Sequence[str] | None`, `extract_by_ids(ids) -> pd.DataFrame`, набор стадий `pre_transform`, `domain_enrich`, `post_transform`, `validate`, `write`. | Уникальный режим отказа legacy параметров, ручное построение fetcher’ов, локальный кеш, расширенный набор нормализаций и enrich‑стадий, собственный writer и QC отчёт.【F:src/bioetl/pipelines/chembl/activity/run.py†L189-L324】【F:src/bioetl/pipelines/chembl/activity/run.py†L701-L845】【F:src/bioetl/pipelines/chembl/activity/run.py†L2888-L3056】 |
| Assay | `ChemblAssayPipeline` (`src/bioetl/pipelines/chembl/assay/run.py`) | `build_descriptor()`, `extract_by_ids(ids) -> pd.DataFrame`, `pre_transform`, `domain_enrich`, `post_transform`, приватные `_serialize_array_fields/_harmonize_identifier_columns` и т.д. | Использует `build_standard_chembl_context`, но руками конструирует фабрики fetcher/finalize, управляет handshake’ом и release внутри дескриптора, что усложняет тестирование и повторное использование.【F:src/bioetl/pipelines/chembl/assay/run.py†L126-L415】 |
| Document | `ChemblDocumentPipeline` (`src/bioetl/pipelines/chembl/document/run.py`) | `build_descriptor()`, `_build_document_context`, `extract_by_ids(ids) -> pd.DataFrame`, `pre_transform/domain_enrich/post_transform`, `validate`. | Содержит сложный fetcher/finalize контекст (инъекция счётчиков API), а также локальные нормализаторы DOI/авторов; dry‑run логика и telemetry внедрены непосредственно в `extract_by_ids`.【F:src/bioetl/pipelines/chembl/document/run.py†L46-L220】【F:src/bioetl/pipelines/chembl/document/run.py†L242-L549】 |
| Target | `ChemblTargetPipeline` (`src/bioetl/pipelines/chembl/target/run.py`) | `build_descriptor()`, `extract_by_ids(ids) -> pd.DataFrame`, `pre_transform/domain_enrich/post_transform`. | Более простая реализация, но всё ещё вручную пробрасывает `metadata_filters`, лимиты и dry‑run события, хотя эти параметры дублируются с другими пайплайнами.【F:src/bioetl/pipelines/chembl/target/run.py†L72-L210】 |
| TestItem | `TestItemChemblPipeline` (`src/bioetl/pipelines/chembl/testitem/run.py`) | Специализированные `_fetch_chembl_release`, `resolve_chembl_release`, `build_descriptor()`, `extract_by_ids(ids) -> pd.DataFrame`, `transform(df)`, `augment_metadata(stats)`. | Единственный пайплайн с переопределённым `resolve_chembl_release` (возвращает release и extra metadata), использует моно‑стадийный `transform`, добавляет `augment_metadata` и хранит `chembl_db_version`/`api_version` в состоянии.【F:src/bioetl/pipelines/chembl/testitem/run.py†L43-L230】【F:src/bioetl/pipelines/chembl/testitem/run.py†L233-L468】 |
| База | `ChemblPipelineBase` (`src/bioetl/chembl/common/descriptor.py`) + `UnifiedPipelineBase` (`src/bioetl/pipelines/unified_base.py`) | `build_descriptor() -> ChemblExtractionDescriptor`, `extract_all()`, `run_descriptor_extraction(...) -> tuple[pd.DataFrame, BatchExtractionStats]`, `run_batched_extraction(...)`, стандартные hooks `prepare_run/finalize_run`. | База уже умеет строить клиентов, dry‑run и сводки, но не диктует обязательные стадии трансформаций/валидации; дочерние классы по‑прежнему создают fetcher/metadata вручную и переопределяют `extract_by_ids` полностью, хотя сигнатура одинакова.【F:src/bioetl/chembl/common/descriptor.py†L333-L1180】【F:src/bioetl/pipelines/unified_base.py†L1-L42】 |

## Целевой интерфейс
1. **Контракт извлечения (`ChemblDescriptorContract`)** — дескриптор описывает сущность (имя, id_column, must_have_fields), фабрики контекста (`build_context`), дополнительные хук‑функции (handshake, extra filters) и предоставляет `record_transform`/`summary_extra`. Дочерние пайплайны отвечают только за `build_descriptor()` и (опционально) `resolve_legacy_extract_ids`.
2. **Контракт трансформаций (`ChemblTransformContract`)** — `UnifiedPipelineBase` вызывает `transform(df)`; по умолчанию он делит выполнение на `pre_transform`, `domain_enrich`, `post_transform`. Пайплайны, которым нужна лишь часть стадий, переопределяют конкретные методы, но публичная точка входа остаётся `transform`.
3. **Контракт метаданных и финализации (`ChemblMetadataContract`)** — методы `augment_metadata(stats: BatchExtractionStats) -> Mapping[str, Any]` и `resolve_chembl_release(...) -> tuple[str | None, dict[str, Any]]` перемещаются в базовый класс, а дочерние пайплайны возвращают дополнительную информацию (например, `chembl_db_version`).
4. **Пример интерфейса**:
```python
class ChemblPipelineContract(Protocol):
    actor: str
    id_column: str
    extract_event_name: str

    def build_descriptor(self) -> ChemblExtractionDescriptor[Any]: ...
    def resolve_legacy_extract_ids(self, log: BoundLogger, *args: Any, **kwargs: Any) -> Sequence[str] | None: ...
    def run_descriptor_extraction(
        self,
        descriptor: ChemblExtractionDescriptor[Any],
        ids: Sequence[str] | None,
        *,
        limit: int | None,
        batch_size: int | None,
        chunk_size: int | None,
        max_batch_size: int | None,
        select_fields: Sequence[str] | None,
        metadata_filters: Mapping[str, Any] | None,
        finalize: FinalizeCallable | None = None,
    ) -> tuple[pd.DataFrame, BatchExtractionStats]: ...
    def transform(self, df: pd.DataFrame) -> pd.DataFrame: ...
    def validate(self, df: pd.DataFrame) -> pd.DataFrame: ...
    def augment_metadata(self, stats: BatchExtractionStats) -> Mapping[str, Any]: ...
```
5. **Сценарий использования** — CLI вызывает `pipeline.run(...)`, базовый класс определяет режим извлечения, собирает дескриптор, вызывает `run_descriptor_extraction` и затем единый `transform`→`validate`→`write`; дочерние классы предоставляют лишь `build_descriptor`, опциональные хуки трансформации и дополнительные метаданные.

## Предлагаемые изменения в коде
1. `src/bioetl/chembl/common/descriptor.py`
   - Формализовать `ChemblDescriptorContract`/`ChemblTransformContract` (Protocol или ABC), добавить helpers для `augment_metadata`, вынести повторяющиеся параметры `summary_extra`, `metadata_filters`, `dry_run_event` в `run_descriptor_extraction`.
   - Объявить в `ChemblPipelineBase` абстрактные методы `transform`, `validate`, `augment_metadata` с реализациями по умолчанию, чтобы дочерние пайплайны могли переопределять только необходимые стадии.
   - Гарантировать, что `resolve_chembl_release` возвращает `(release, metadata)`; базовая реализация отдаёт `(fetch_chembl_release(...), {})`, а `TestItemChemblPipeline` переопределяет, добавляя `chembl_db_version`/`api_version`.
2. `src/bioetl/pipelines/base.py`
   - Добавить хук `def extract(self) -> pd.DataFrame` (используемый `UnifiedPipelineBase`), который вызывает `_dispatch_extract_mode` и `run_descriptor_extraction`; `PipelineBase.run()` остаётся без изменений, но новый хук снижает дублирование.
   - Расширить `record_extract_metadata` чтобы автоматически объединять payload из `augment_metadata`.
3. `src/bioetl/pipelines/unified_base.py`
   - Реализовать поэтапный `transform`: `transform = post_transform(domain_enrich(pre_transform(df)))`, причём базовые методы возвращают `df` без изменений; `TestItem` может переопределить `transform` целиком.
   - Передавать `BatchExtractionStats` из `run_descriptor_extraction` в новый `augment_metadata` для записи в manifest/QC отчёты.
4. `src/bioetl/pipelines/chembl/*/run.py`
   - Сократить `extract_by_ids` до делегатов `return self.run_descriptor_extraction(self.build_descriptor(), ids, **options)[0]`, оставив только специфичную конфигурацию `batch_size`, `metadata_filters`, `summary_extra`.
   - Привести трансформационные хуки к общему контракту: где используется `domain_enrich`, переименовать в `transform` через вызовы `super()`; для `TestItem` добавить `pre_transform`/`post_transform` если появятся общие стадии.
   - Удалить локальные dry‑run обработчики, если они дублируют функциональность базового класса (оставить только специфичные сообщения).
5. Тесты (`tests/bioetl/pipelines/chembl/*`)
   - Обновить фикстуры под новый интерфейс (`augment_metadata`, `transform`), добавить snapshot’ы для логов/summary, гарантирующие, что старые CLI точки входа (`--input-file`, `--limit`, dry-run) ведут себя одинаково.

## План миграции и совместимость
1. **Фаза 1 — базовые классы**: расширить `ChemblPipelineBase`/`UnifiedPipelineBase`, добавить новые Protocol’ы и дефолтные реализации (`transform`, `augment_metadata`). Прогнать mypy/pytest для проверки обратной совместимости.
2. **Фаза 2 — извлечение**: переписать `extract_by_ids` в каждом ChEMBL‑пайплайне поверх новых helper’ов (`run_descriptor_extraction`, `augment_metadata`), убедившись, что CLI события (`extract_event_name`, dry-run) сохраняются.
3. **Фаза 3 — трансформации**: мигрировать пайплайны на общий `transform`→`validate`→`write` поток, удалить дублирующие `domain_enrich` вызовы или оставить их как приватные вспомогательные методы, вызываемые из `transform`.
4. **Фаза 4 — метаданные и тесты**: обновить генерацию manifest/QC, добавить unit‑тесты на `augment_metadata` и `resolve_chembl_release` (особенно `TestItem`); синхронизировать CLI tests, чтобы подтвердить, что `PipelineBase.run()` API не изменился.
5. **Фаза 5 — документация**: обновить developer docs/README с новым контрактом пайплайнов и описанием hook’ов (включая примеры дескрипторов для новых ChEMBL сущностей).

### Дополнительные замечания
- Выполнять миграцию по слоям: сначала база, затем пайплайны, затем тесты и документация, чтобы минимизировать конфликтующие изменения.
- На каждом шаге проверять `pytest`, `mypy`, `ruff` и контрольные CLI команды (dry-run и полноценный run) для всех пяти пайплайнов.
- Для внешних пользователей (CLI) важно оставить прежние команды (`bioetl run pipeline=...`), поэтому любые новые методы должны быть внутренними; при необходимости добавлять deprecated-обёртки вокруг удаляемых вспомогательных функций.
- После унификации интерфейсов зафиксировать стабильные точки расширения (descriptor factories, transform hooks) в документации, чтобы упрощать разработку новых ChEMBL пайплайнов.
- Особое внимание уделить `TestItemChemblPipeline`: при переезде release metadata необходимо сохранить совместимость с downstream системами, потребляющими `chembl_db_version`/`api_version` из manifest.
