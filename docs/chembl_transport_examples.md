# Транспортный слой ChEMBL и протоколы клиентов

## Протоколы
- `ApiTransportProtocol` описывает минимальный HTTP-контракт (`request(method, path, ...)`, `close`).
- `EntityClientProtocol` описывает операции над сущностью (`get`, `list`/`fetch_all`, `fetch_by_ids`, `search`, `close`).
- Доменный слой работает только с этими протоколами и не зависит от конкретных реализаций транспорта или HTTP-библиотек.

## Транспортные реализации
- `clients/transports.py` содержит пример синхронного транспорта на базе `requests` и асинхронного — на базе `aiohttp`.
- `BaseChemblClient` остаётся чистым транспортом: он адаптирует любой `ApiTransportProtocol`, добавляя обёртку для логирования и единый интерфейс `request`.

## Клиенты сущностей
- `_BaseEntityClient` и `ChemblEntityClient` строят endpoints сами, принимая транспорт в конструктор и не наследуя сетевые клиенты.
- `ChemblEntityClientFactory` работает как конфигуратор: получает фабрику транспорта и возвращает готовые entity-клиенты без сетевых вызовов при создании.

## Кэширование
- `cache_entity_client` декорирует любой `EntityClientProtocol`, добавляя кэширование вызовов `get`/`fetch_by_ids` без изменения остальных методов.

## Быстрый старт
1. Соберите транспорт (`RequestsTransport`, `AioHttpTransport` или `BaseChemblClient` поверх `UnifiedAPIClient`).
2. Передайте фабрику транспорта в `ChemblEntityClientFactory` и создайте нужный entity-клиент.
3. При необходимости оберните клиент через `cache_entity_client` для мемоизации идентификаторов.
