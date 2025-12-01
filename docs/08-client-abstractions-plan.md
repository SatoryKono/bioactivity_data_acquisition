lf# План устранения дублирования и выравнивания клиентов под единый каркас

## Сводка
Общий подход — вынести повторяющиеся части HTTP-слоя, пагинации, логирования и обработки ошибок в базовые классы/миксины и подключить их к существующим клиентам без изменения внешнего поведения. База покрывает сессию/заголовки/таймауты/ретраи, пагинацию и унифицированные хуки нормализации; провайдер-специфичный код остаётся только в маршрутах и преобразовании ответа.

## Группы дублирования
| Группа | Файлы/классы | Тип дублирования | Рекомендация |
| --- | --- | --- | --- |
| HTTP-вызовы с таймаутами/ретраями | `clients/enricher_base.OptionsAwareApiClient`, `clients/chembl/adapter.ChemblTransportAdapter` | Раздельные обёртки над `ApiTransportProtocol` с одинаковыми параметрами (`timeout_sec`, `max_retries`), логированием и проксированием `fetch_one/fetch_batch` | Вынести в `BaseHttpClient` с общими параметрами и контекстным логированием, адаптеры унаследовать |
| Пагинация по page_key/next_key/page_param | `clients/enricher_base.BaseEnricherClient._iterate_pages`, `clients/chembl/base.BaseChemblClient.fetch_many` | Похожие циклы пагинации и параметры, разные имена и расположение опций | Вынести в `PagedClientMixin` с единым `PaginationParams`, оставить стратегии в ChEMBL как частный случай |
| Алиасы и повторяющиеся методы получения/поиска | `clients/enricher_base.EnricherClientProtocol.fetch/search/call_route`, `clients/chembl/base.ChemblClientProtocol.fetch_many/fetch_all/list/fetch_page` | Разные имена для одинаковых операций, дублирующий код вызовов | Ввести единые методы в базовом классе, алиасы реализовать через thin-обёртки/DeprecatedMixin |
| Нормализация/обработка payload | `clients/enricher_base.BaseEnricherClient._iterate_pages`, `clients/chembl/normalization` | Частичная нормализация в клиентах, смешение с HTTP-логикой | Вынести в `NormalizerMixin`/`INormalizer` с явным вызовом из пайплайна или адаптера |
| Логирование и обработка ошибок | `_wrap_callable` в `BaseEnricherClient`, `_wrap_callable`/context в `ChemblTransportAdapter` | Разные контексты и формат логов, не единые исключения | Добавить `LoggingMixin` с единым форматом контекста (source, route/entity, path, method) и базовые исключения `ProviderError`, `PaginationError` |

## Предлагаемые абстракции
- **BaseHttpClient (абстрактный класс)**: управляет сессией/заголовками/timeout/retries, предоставляет `request`, `get_json`, `paginate_json`. Параметры конфигурации принимаются в виде `TransportOptions`; логирует начало/результат/исключения с `RequestContext`. Наследники реализуют `_send` (низкоуровневый транспорт) или переопределяют сериализацию параметров.
- **PagedClientMixin**: реализует `iter_pages` и `iterate_records` поверх `PaginationParams` и общего пагинатора (совместим с ChEMBL `PaginationStrategy`). Требует `request`/`get_json`; предоставляет хуки `extract_items(page)` и `next_cursor(page)` для провайдер-специфичных ответов.
- **LoggingMixin**: единый `_wrap_callable` с структурированным логированием (`source`, `route/entity`, `path`, `method`, `page`, `retries`) и переводом сетевых/HTTP ошибок в `ProviderError`/`PaginationError`.
- **NormalizerMixin/INormalizer**: контракт `normalize(payload | Iterable[dict]) -> Iterable[dict]` или `DataFrame`, опционально подключаемый в клиентах через композицию; по умолчанию no-op. Позволяет разделить HTTP и доменную нормализацию.
- **DeprecatedAliasMixin**: реализует алиасы (`fetch`, `search`, `list`, `fetch_page`, `fetch_all`, `call_route`) поверх базовых методов, помечая предупреждениями, чтобы сохранить совместимость.

## Изменения по файлам
| Файл/класс | Что вынести | Что оставить | Комментарий |
| --- | --- | --- | --- |
| `clients/enricher_base.BaseEnricherClient` | HTTP-прокси (`OptionsAwareApiClient`), пагинацию (`_iterate_pages`), `_wrap_callable` | Маршрутизация `route_name`, выбор page_key/next_key по умолчанию | Наследовать `BaseHttpClient` + `PagedClientMixin` + `DeprecatedAliasMixin`; оставить конструкцию маршрутов и defaults провайдера |
| `clients/chembl/adapter.ChemblTransportAdapter` | Логирование/обёртку `_wrap_callable`, общую валидацию ответов/исключений | Преобразование в ChEMBL-специфичный payload/metadata | Сделать наследником `BaseHttpClient`/`LoggingMixin`; оставить адаптацию схемы ответа |
| `clients/chembl/base.BaseChemblClient` | Пагинацию `fetch_many`, алиасы `fetch_page/list/fetch_all`, базовый `iterate_records` | Подстановка путей `/entity/{id}`, связь со стратегиями пагинации | Наследовать `PagedClientMixin` и `DeprecatedAliasMixin`; оставить связку с `PaginationStrategyResolver` |
| `clients/providers/*.py` (генерируемые классы) | Пагинационные циклы, проверки опций, алиасы `fetch/search/call_route` | Описание маршрутов, маппинг параметров на endpoint | Переиспользовать базу через общий класс, генератор создаёт наследника `BaseHttpClient`/`PagedClientMixin` |
| `clients/enricher_facade.py` | Нормализацию ошибок/логирование вызовов | Оркестрацию стратегий | Переключить на `LoggingMixin`/новые исключения, оставить стратегический выбор клиента |
| `clients/chembl/normalization.py` | Ничего (остаётся доменный код) | Маппинги колонок, построение DataFrame | Подключать через `NormalizerMixin` или отдельный сервис, чтобы убрать нормализацию из HTTP слоёв |

## Тестирование
- **Сетевые ошибки и ретраи**: unit-тесты для `BaseHttpClient._wrap_callable` и адаптеров, мокая транспорт (`requests`/`ApiTransportProtocol`) с исключениями/HTTP-кодами; проверить конвертацию в `ProviderError` и количество повторов.
- **Пагинация**: тесты `PagedClientMixin` на сценарии page_key/next_key/page_param, курсорную пагинацию и отсутствие страниц; убедиться, что оба семейства клиентов используют общую стратегию и возвращают одинаковый поток записей.
- **Таймауты**: тесты конфигурации `timeout_sec`/`max_retries` на уровне базового клиента и переопределений в вызовах.
- **Алиасы и совместимость**: smoke-тесты на старые методы (`fetch`, `search`, `fetch_all`, `call_route`) для ChEMBL и провайдеров, проверяющие, что они проксируют к новым методам и логируют предупреждения.
- **Нормализация**: тесты `NormalizerMixin` с no-op и кастомным нормализатором, чтобы убедиться, что HTTP-клиенты остаются независимыми от формата возврата.
- **Провайдерные smoke-тесты**: для каждого клиента (ChEMBL, Crossref, OpenAlex, PubChem, PubMed, Semantic Scholar, Uniprot) — мок ответа на одну страницу и многостраничный поток; проверка одинакового поведения по логированию, исключениям и пагинации.
