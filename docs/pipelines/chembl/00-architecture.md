# Архитектура ChEMBL-пайплайнов

Документ описывает публичный API ChEMBL-слоя, взаимосвязь базовых классов и
точки расширения, которые необходимо использовать при разработке новых
пайплайнов или аудите существующих.

## Обзор слоёв

| Слой | Модуль | Ответственность |
| --- | --- | --- |
| CLI | `bioetl.cli.cli_app` | Регистрирует команды (`activity_chembl`, `assay_chembl`, `...`) и создаёт экземпляры пайплайнов с подготовленной конфигурацией. |
| Unified | `bioetl.pipelines.unified_base.UnifiedPipelineBase` | Композиция mixin-слоя (логирование, QC, Pandera-валидация, I/O) + дефолтная реализация `run()`. |
| ChEMBL база | `bioetl.chembl.common.descriptor.ChemblPipelineBase` | Общая логика `run_descriptor_extraction`, handshake релиза, фабрики клиентов и вспомогательные normalize-хуки. |
| Конкретные пайплайны | `bioetl.pipelines.chembl.<entity>.run` | Определяют `build_descriptor()`, кастомные трансформации и дополнительные метаданные. |

Все пять публичных пайплайнов (`activity`, `assay`, `document`, `target`,
`testitem`) наследуются от `UnifiedPipelineBase` → `ChemblPipelineBase`, поэтому
получают единый жизненный цикл и не должны реализовывать свою версию
`extract_by_ids` или batched fetcher.

## Контракт публичного API

```python
class ChemblPipelineContract(Protocol):
    id_column: str | None
    pipeline_code: str

    def run(self, output_dir: Path, *, extended: bool = False, include_qc_metrics: bool = False, **options: Any) -> RunResult: ...
    def build_descriptor(self) -> ChemblExtractionDescriptor[Any]: ...
    def run_descriptor_extraction(
        self,
        descriptor: ChemblExtractionDescriptor[Any],
        ids: Sequence[str] | None,
        *,
        summary_event: str,
        metadata_filters: Mapping[str, Any] | None = None,
        fetch_mode: Literal["default", "delegated"] = "default",
        **batch_kwargs: Any,
    ) -> tuple[pd.DataFrame, BatchExtractionStats]: ...
    def resolve_chembl_release(
        self,
        chembl_client: UnifiedAPIClient,
        log: BoundLogger,
        entity_client: Any | None = None,
    ) -> tuple[str | None, dict[str, Any]]: ...
```

Внешний пользователь взаимодействует через CLI или прямой вызов `run`. Внутренние
интеграции могут обращаться к `run_descriptor_extraction`, чтобы реюзать
логистику batched запросов, не переписывая CLI.

## Поток `run_descriptor_extraction`

1. **Дескриптор** (`build_descriptor`) возвращает объект с фабриками контекста,
   пустого DataFrame и финализации. Контекст включает `chembl_client`,
   сущностный клиент (`iterator`) и настройки источника.
2. **Построение контекста** — базовый класс вызывает `descriptor.build_context`,
   регистрирует клиентов, применяет `metadata_filters` и передаёт итоговый
   объект в fetcher/finalize фабрики.
3. **Resolve release** — `resolve_chembl_release` (по умолчанию вызывает
   `fetch_chembl_release`) возвращает `(release, metadata)`.
4. **Dry-run** — при `config.cli.dry_run` создаётся пустой (или специальный)
   DataFrame, логируется событие `summary_event`, а batched fetcher не
   запускается.
5. **Batched extraction** — `run_batched_extraction` получает `fetcher`,
   `batch_size`, `chunk_size`, `id_normalizer`, `chembl_release` и остальные
   параметры. Он возвращает DataFrame и `BatchExtractionStats`.
6. **Summary logging** — метод формирует полезную нагрузку (rows, duration,
   release, api_calls, extra metadata) и публикует её через BoundLogger.

## Управление стадиями и тестовое покрытие

- Все пайплайны реализуют `PipelineStagesProtocol`, поэтому базовый
  `StageFactory` (`src/bioetl/pipelines/base.py`) строит план `extract → transform
  → validate → write → cleanup`. Метод `create_stage_factory()` можно
  переопределить и вернуть кастомный набор `PipelineStageCommand`, например,
  чтобы выключить `transform` в dry-run. Юнит-тесты
  `tests/pipelines/test_pipeline_commands.py` фиксируют поведение фабрики и
  передачу CLI-флагов в `save_results`.
- Для CLI-команд ChEMBL предусмотрен stage-runner
  (`src/bioetl/pipelines/chembl/stage_runner.py`), позволяющий регистрировать
  пайплайны по alias, строить частичные планы (`extract` отдельно от `write`) и
  применять одинаковые стадии ко всем сущностям. Позитивные и негативные кейсы
  задокументированы в `tests/bioetl/pipelines/chembl/test_stage_runner.py`.
- Дополнительные smoke-тесты (`tests/bioetl/pipelines/test_unified_base.py`,
  `tests/bioetl/pipelines/test_pipeline_lifecycle.py`) проверяют, что хуки
  `prepare_run`/`finalize_run` вызываются корректно и что расширения не ломают
  контракт `RunResult`.

## Работа с релизами ChEMBL

- `ChemblPipelineBase.fetch_chembl_release()` получает статус из REST API и
  извлекает версию (`chembl_db_version`/`chembl_release`).
- `resolve_chembl_release()` добавляет к версии дополнительные поля, зависящие от
  пайплайна. `TestItemChemblPipeline` дописывает `api_version` и сохраняет их в
  manifest через `augment_metadata`.
- Значение кэшируется в `self.chembl_release` и пробрасывается в статистику и
  QC-отчёты.

## Типовые сценарии

### CLI-запуск (activity)

```bash
bioetl activity_chembl \
  --config configs/pipelines/activity/activity_chembl.yaml \
  --output-dir ./data/output/activity
```

CLI распарсит конфигурацию, создаст `ChemblActivityPipeline`, вызовет `run()` и
автоматически соберёт QC отчёты.

### Программный вызов с кастомными ID

```python
pipeline = ChemblTargetPipeline(config=cfg, run_id="manual")
descriptor = pipeline.build_descriptor()
frame, stats = pipeline.run_descriptor_extraction(
    descriptor,
    ids=["CHEMBL25", "CHEMBL190"],
    summary_event="target.custom_extract",
    metadata_filters={"status": "approved"},
)
```

## Расширение стека

1. Создать новый модуль `src/bioetl/pipelines/chembl/<entity>/run.py` и унаследовать класс от `UnifiedPipelineBase`.
2. Реализовать `build_descriptor()` с использованием существующих фабрик
   (`ChemblExtractionDescriptor`).
3. Переопределить `transform`/`validate` только при наличии специфичной логики;
   всё остальное предоставляет база.
4. Если нужно дополнительное metadata, реализовать `augment_metadata(stats)` и
   (при необходимости) `resolve_chembl_release`.
5. Добавить запись в `docs/pipelines/10-pipelines-catalog.md` и тесты под новую
   сущность.

## Требования к тестам и документации

- Для любого изменения `run_descriptor_extraction` обязателен unit-тест в
  `tests/bioetl/pipelines/chembl/common/test_descriptor_runner.py`.
- Конкретные пайплайны должны иметь smoke-тесты на трансформации и dry-run.
- Документацию в `docs/pipelines/chembl/<entity>/` необходимо обновлять при
  изменении публичных флагов CLI, формата output или набора метаданных.
