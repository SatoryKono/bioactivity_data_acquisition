## Сводка
- Проанализирована клиентская подсистема и выделены основные области дублирования в HTTP-взаимодействии, пагинации и нормализации данных. 
- Сформированы предложения по переносу повторяющейся логики в `BaseDataProvider`, базовые утилиты транспорта/пагинации и интерфейс `Normalizer` с провайдер-специализированными реализациями.
- Намечены примерные патчи и шаги интеграции провайдеров PubChem, PubMed, OpenAlex, Crossref, Semantic Scholar, UniProt и ChEMBL-адаптера в общие абстракции, а также контрактные тесты для сохранения поведения.

## Группы дублирования
| Группа | Файлы/классы | Тип дублирования | Рекомендация |
| --- | --- | --- | --- |
| Общая HTTP/пагинация для простых REST-клиентов | `BaseDataProvider.iter_pages`/`fetch_many` и `_normalize_page` повторяют ту же структуру обработки первой страницы, вычисления limit, нормализации и возврата `Page` | Повтор базовой итерации и подготовки запроса | Вынести подготовку параметров, первый запрос и цикл `iter_pages` в общий транспортный helper (например, `clients/base/pagination.py`) и вызывать его из провайдеров через `BaseDataProvider` |
| HTTP/пагинация в обогащателях | `BaseEnricherClient._iterate_pages`/`fetch_one`/`fetch_batch` реализуют ту же схему логирования, перебора страниц и подстановки `page_key`/`next_key` | Дублирование с `BaseDataProvider` (разные поля, но одинаковый паттерн) | Использовать единый пагинатор для `ApiTransportProtocol` + нормализатор, передавая стратегию и fallback, а не держать отдельный цикл внутри каждого класса |
| Пагинация и слияние параметров в ChEMBL-клиентах | `BaseChemblClient.fetch_many` и `iter_pages` повторяют вычисление `effective_pagination`, установку `limit`, первый GET и разворот через стратегию | Дублирование с `BaseDataProvider`/`BaseEnricherClient` и внутри самого класса | Перенести вычисление pagination params и запуск стратегии в общий метод `BaseChemblClient._iter_pages` или использовать тот же helper, что и для остальных провайдеров |
| Объявление маршрутных провайдеров | Файлы `providers/pubchem.py`, `openalex.py`, `crossref.py`, `pubmed.py`, `semantic_scholar.py`, `uniprot.py` вызывают `create_route_provider_class` с одинаковым шаблоном `RouteConfig` и `deprecated_aliases` | Шаблонные декларации маршрутов | Вынести декларации в общий реестр или фабрику конфигураций и генерировать провайдеры из таблицы конфигурации |
| Нормализация записей | `BaseChemblNormalizer.normalize` реализует типичные шаги: заполнение столбцов, преобразование типов, бизнес-ключ, хэш строки | Локальная нормализация без общего интерфейса; потенциально повторяется у других источников | Ввести интерфейс `Normalizer` и стандартные реализации (ChemblNormalizer, PubChemNormalizer и др.), подключать их в `fetch_many`/`fetch_one` перед возвратом |

## Предлагаемые абстракции
1. **Transport + Pagination helpers**
   - Ответственность: единообразный вызов транспорта (`ApiTransportProtocol`), подготовка параметров (`limit/page/next`), первый запрос и цикл итерации страниц.
   - Методы:
     - `paginate(endpoint, transport, params, strategy, pagination_params, logger, normalize=None)` — возвращает `PageStream`, скрывая обработку первой страницы и next-cursor.
     - `prepare_params(query, pagination_params)` — объединяет query + limit, учитывает `page_size`/`page_param`.
2. **BaseDataProvider улучшенный**
   - Ответственность: тонкая обёртка над транспортом с делегацией в общий пагинатор и опциональный нормализатор.
   - Методы:
     - `fetch_one(ref, normalizer=None)` — единый HTTP GET + нормализация.
     - `iter_pages(query, pagination, normalizer=None)` — делегирует в helper `paginate`.
     - `fetch_many(...)` — разворачивает поток страниц.
3. **Normalizer интерфейс**
   - Ответственность: конвертация сырого payload/record в канонический словарь/`DataFrame`.
   - Методы:
     - `normalize_record(raw: Mapping[str, Any]) -> dict[str, Any]` и/или `normalize_page(raw_page: Any, page_key: str | None) -> list[dict[str, Any]]`.
     - Базовые реализации: `ChemblNormalizer` (существующую логику вынести из `BaseChemblNormalizer`), `RouteProviderNormalizer` (работает с `normalize_payload`).
4. **Route provider registry**
   - Ответственность: декларативное описание маршрутов и alias-ов в одном месте.
   - Структуры:
     - `ROUTE_PROVIDER_SPECS: dict[str, RouteProviderSpec]` с именем, source, конфигурацией маршрутов, alias-ами.
     - Функция `build_route_providers(specs)` генерирует классы вместо дублированных файлов.

## Изменения в коде
`src/bioetl/clients/providers/base_provider.py`
-----------------------------
Текущий код сам формирует `params`/`limit`, делает первый запрос и использует `iter_pages`, затем вручную нормализует и собирает `Page`.

`clients/base/pagination.py` (новый)
-----------------------------
Ввести функции `prepare_params` и `paginate` с поддержкой `PaginationParams`, `normalize_payload` и `DefaultPaginationStrategy`.

`src/bioetl/clients/providers/base_provider.py`
-----------------------------
Заменить тело `iter_pages` на вызов `paginate`, передавая `self.transport`, `self._pagination_strategy`, `self._logger` и `self._normalize_page`.

`src/bioetl/clients/enricher_base.py`
-----------------------------
Удалить локальный цикл `_iterate_pages`/`fetch_batch` и делегировать в общий `paginate`, используя `BaseDataProvider` или обёртку `UnifiedProviderAdapter`.

`src/bioetl/clients/chembl/base.py`
-----------------------------
Перенести подготовку `effective_pagination`, первый GET и цикл `iter_pages` в общий метод или использовать новый helper; `fetch_many` и `iter_pages` сводятся к вызову `paginate` с нужными ключами и нормализатором.

`src/bioetl/clients/providers/*`
-----------------------------
Создать таблицу конфигураций маршрутов и фабрику; каждый модуль либо импортирует готовый класс из реестра, либо генерируется автоматически, убирая копипасту `RouteConfig(...)` и `deprecated_aliases`.

`src/bioetl/clients/chembl/normalization.py`
-----------------------------
Выделить интерфейс `Normalizer` (в `clients/base/interfaces.py` или новом модуле) и адаптировать существующий `BaseChemblNormalizer` под него, чтобы другие провайдеры могли реализовать аналогичные классы без дублирования бизнес-ключей и хэшей.

## Тестирование
- Контрактные тесты для всех провайдеров на моковом транспорте:
  - `fetch_one` возвращает один нормализованный элемент при успешном ответе и поднимает унифицированное исключение при ошибке транспорта.
  - `iter_pages` корректно уважает `page_size`/`page_param`/`next_key`, логирует первый запрос и возвращает `Page` с `next_cursor`.
  - `fetch_many` разворачивает все страницы и применяет нормализатор.
- Негативные кейсы: 4xx/5xx и пустые ответы приводят к одинаковым исключениям/логам во всех клиентах.
- Ретроспективная проверка поведения: для набора фиктивных ответов сравнить результаты старых и новых реализаций (`paginate_old` vs новый helper) до удаления старого пути; можно временно хранить обе версии и прогонять A/B тесты в рамках pytest.
