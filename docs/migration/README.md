# Migration guide

Этот гид описывает переход на финализированный публичный API BioETL и
укреплённые архитектурные границы.

## Пошаговая адаптация пайплайна

1. **Импортируйте публичный API из `bioetl`** — больше не требуется ленивый
   shim: `from bioetl import PipelineConfig, PipelineBase`.
2. **Перенастройте создание клиентов через фабрику.** Конструктор пайплайна
   должен принимать `client_factory` и прокидывать его в места, где ранее
   использовались прямые импорты клиентов.

   ```python
   from bioetl.clients import ChemblEntityClientFactory

   class ChemblAssayPipeline(BaseChemblPipeline):
       def __init__(self, config: PipelineConfig, run_id: str, **kwargs: Any) -> None:
           super().__init__(
               config,
               run_id,
               client_factory=kwargs.get(
                   "client_factory", ChemblEntityClientFactory.default()
               ),
           )
   ```

3. **Обновите вызовы общих helper'ов ChEMBL.** Импортируйте
   `ChemblPipelineBase`, `perform_chembl_handshake` и `normalize_identifiers`
   из `bioetl.chembl.common` без промежуточных шорткатов.
4. **Проверьте схему импорта доменного кода.** В домене запрещены импорты
   `bioetl.infrastructure.*` и `bioetl.clients.*`; CI oстанавливает PR при
   нарушении.
5. **Сделайте smoke-проверку.** Установите пакет в editable-режиме и выполните
   `pytest -q tests/smoke` для подтверждения корректности перенастроенного
   пайплайна.

## Удалённые шимы

- Ленивые реэкспорты в `bioetl.__init__` заменены на явный публичный API.
- `bioetl.chembl.common` больше не использует `__getattr__` для ленивой загрузки
  вспомогательных классов и функций.
- Пакет `bioetl.clients` загружает клиентов напрямую, без промежуточной карты
  атрибутов.

## FAQ

- **Почему нужен `client_factory`?** Фабрика упрощает тестирование и моки,
  сохраняя зависимости на инфраструктуру вне доменного слоя.
- **Что делать при срабатывании import-lint?** Перенесите инфраструктурные
  вызовы в orchestrator или сервис-слой, передавайте интерфейсы в домен через
  параметры конструктора или функции.
