# Обзор базовых классов и кандидаты на удаление

## Дерево базовых классов и точек использования

| Базовый класс | Наследники | Основные точки использования |
| --- | --- | --- |
| `PipelineBaseCommon` (`bioetl/core/pipeline/orchestration.py`) | `ChemblActivityPipeline` | Оркестрация стадий через StageFactory и CLI-интерфейсы ChEMBL activity пайплайна. |
| `ChemblPipelineBase` (`bioetl/core/pipeline/unified.py`) | `ChemblEntityPipeline` (legacy) → `ChemblAssayPipeline`, `ChemblDocumentPipeline`, `ChemblTargetPipeline`, `ChemblTestItemPipeline` | Используется в файловой и QC-ориентированной версии ChEMBL пайплайнов. |
| `_BaseEntityClient` (`bioetl/clients/entities/_base.py`) | `ChemblActivityClient`, `ChemblAssayClient`, `ChemblTargetClient`, `ChemblDocumentClient`, `ChemblTestItemClient` | Общее обращение к UnifiedAPIClient: постраничный обход, выборка по ID. |
| `UnifiedPipelineBase` (`bioetl/core/pipeline/unified.py`) | Только тестовые заглушки (`tests/test_chembl_descriptor.py::MinimalPipeline`) | Предоставляет шаблонный жизненный цикл extract/transform/validate/write для unit-тестов. |
| `ChemblEntityPipeline` (**дублирующий модуль** `bioetl/pipelines/chembl/common.py`) | Нет | Файл не загружался из-за наличия пакета `bioetl.pipelines.chembl.common` (берётся `__init__.py` из поддиректории); импорты `from bioetl.pipelines.chembl.common import ...` резолвятся в пакет и используют реализацию `common/legacy.py`. Модуль удалён. |

## Кандидат на удаление

| Класс | Причина | Степень уверенности |
| --- | --- | --- |
| `ChemblEntityPipeline` из `src/bioetl/pipelines/chembl/common.py` (удалён) | Дублировал реализацию из `common/legacy.py`, но сам модуль не импортировался: при `import bioetl.pipelines.chembl.common` Python выбирает одноимённый пакет, а не файл. В кодовой базе не было прямых импортов из файлового модуля, только из пакета. | Высокая |

## Риски и влияние

- Потенциальные скрытые пользователи могли импортировать модуль напрямую (`bioetl.pipelines.chembl.common` как файл) при обходе стандартного импорта, но в текущем дереве исходников и тестов таких ссылок нет.
- Удаление дубликата уменьшит двусмысленность нейминга и снизит риск случайного подключения устаревшей версии базового класса.

## План безопасной чистки

1. Удалить файл `src/bioetl/pipelines/chembl/common.py` и обновить `__all__` пакета при необходимости (сейчас пакет уже экспортирует актуальную версию из `common/legacy.py`).
2. Запустить поиск по репозиторию (`rg "bioetl\.pipelines\.chembl\.common"`) и убедиться, что все импорты продолжают резолвиться в пакет (ожидается без изменений).
3. Прогнать имеющиеся тесты `pytest tests/test_chembl_descriptor.py tests/bioetl/pipelines/test_pipeline_commands.py` (покрывают UnifiedPipelineBase и CLI-пути) чтобы подтвердить отсутствие регрессий.
4. Опционально добавить smoke-тест импорта: `python -c "import bioetl.pipelines.chembl.common"` для гарантии корректного разрешения пути после удаления файла.

## Тесты, которые нужно адаптировать/создать

- Добавить модульный тест импорта пакета `bioetl.pipelines.chembl.common`, чтобы зафиксировать использование реализации из `common/legacy.py` и предотвратить повторное появление дублирующих модулей.
- При удалении файла достаточно повторно выполнить существующие тесты пайплайна (`pytest`), но отдельный smoke-тест импорта в `tests/bioetl/pipelines/test_imports.py` сделает ситуацию с пространством имён прозрачной.
