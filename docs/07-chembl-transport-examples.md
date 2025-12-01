# Транспортный слой ChEMBL и протоколы клиентов

## Протоколы
- `ApiTransportProtocol` описывает минимальный HTTP-контракт (`request(method, path, ...)`, `close`).
- `EntityClientProtocol` описывает операции над сущностью (`get`, `list`/`fetch_all`, `fetch_by_ids`, `search`, `close`).
- Доменный слой работает только с этими протоколами и не зависит от конкретных реализаций транспорта или HTTP-библиотек.

## Рекомендованный транспорт
- Базовый стек строится на `UnifiedAPIClient` с адаптерами (`ChemblTransportAdapter`) и фабрикой устойчивых запросов (`ResilientRequestExecutorFactory`).
- Дополнительные кастомные транспорты (`RequestsTransport`, `AioHttpTransport`) удалены; вместо них используйте адаптеры `ApiTransportProtocol` поверх `UnifiedAPIClient`.
- `BaseChemblClient` остаётся чистым транспортом: он принимает любой `ApiTransportProtocol`, добавляя обёртку для логирования и единый интерфейс `request`.

## Клиенты сущностей
- `_BaseEntityClient` и `ChemblEntityClient` строят endpoints сами, принимая транспорт в конструктор и не наследуя сетевые клиенты.
- `ChemblEntityClientFactory` работает как конфигуратор: получает фабрику транспорта и возвращает готовые entity-клиенты без сетевых вызовов при создании.

## Публичные импорты пакета `bioetl.clients.chembl`
- Поддерживаемые верхнеуровневые классы и фабрики: `BaseChemblClient`, `ChemblEntityClient`, клиенты сущностей (`ChemblActivityClient`, `ChemblAssayClient`, `ChemblDocumentClient`, `ChemblTargetClient`, `ChemblTestItemClient`), `ChemblEntityClientFactory` (и её конфигурация/протоколы), нормализаторы (`BaseChemblNormalizer`, `ColumnMapping`, `ColumnNormalizationSpec`, `build_records_from_payload`), а также фабрики `default_chembl_factory`, `default_activity_client_factory`, `make_chembl_client` и `ChemblClientFactory`.
- Остальные вспомогательные элементы (пагинация, адаптеры, протоколы) импортируйте напрямую из соответствующих модулей. Доступ к ним через `bioetl.clients.chembl` теперь считается устаревшим и сопровождается `DeprecationWarning`.

## Кэширование
- `cache_entity_client` декорирует любой `EntityClientProtocol`, добавляя кэширование вызовов `get`/`fetch_by_ids` без изменения остальных методов.

## Быстрый старт
1. Соберите `UnifiedAPIClient` через `default_chembl_factory` или вручную, используя `ResilientRequestExecutorFactory` и `ChemblTransportAdapter` для получения совместимого `ApiTransportProtocol`.
2. Передайте фабрику транспорта в `ChemblEntityClientFactory` (или используйте словарь, возвращаемый `default_chembl_factory`) и создайте нужный entity-клиент.
3. При необходимости оберните клиент через `cache_entity_client` для мемоизации идентификаторов.
