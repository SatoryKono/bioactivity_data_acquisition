# План выравнивания файловой структуры `src/bioetl/clients`

## Сводка
Текущая структура смешивает инфраструктурные слои, клиентов ChEMBL и других провайдеров в одном уровне, что скрывает роли модулей и усложняет расширение. Целевая структура разделяет базовые абстракции, клиентов по доменам (ChEMBL vs прочие), а также вспомогательные утилиты и типы, оставляя бизнес-логику и пайплайны за пределами пакета `clients`.

## Целевая структура
```
src/bioetl/clients/
  base/                  # Общие абстракции и инфраструктура: протоколы, базовые HTTP-клиенты, пагинация, нормализация, исключения
    __init__.py
    interfaces.py        # DataProviderProtocol, PaginationParams, RequestContext, исключения
    http.py              # BaseHttpClient, конфигурация транспорта/ретраев, структурированное логирование
    pagination.py        # Общие стратегии/миксины пагинации
    normalizers.py       # INormalizer + базовые реализации/no-op
    aliases.py           # DeprecatedAliasMixin и совместимость
  chembl/                # Клиенты и фабрики ChEMBL
    __init__.py
    adapter.py
    adapter_factory.py
    factories.py
    factory.py
    base.py
    entities.py
    pagination.py
    normalization.py
    compat.py            # Временные алиасы/совместимость
    facade.py | registry.py | descriptor_factory.py | strategy_resolver.py
  providers/             # Клиенты остальных источников (маршруты/адаптеры)
    __init__.py
    routes.py            # create_route_provider_class и RouteProviderBase
    base_provider.py     # Общая обёртка, реализующая DataProviderProtocol поверх маршрутов
    crossref.py
    openalex.py
    pubchem.py
    pubmed.py
    semantic_scholar.py
    uniprot.py
  factories/             # Фабрики клиентов и фасады для пайплайнов
    __init__.py
    enricher_factory.py
    client_registry.py   # реестр фабрик (реэкспорт из base при необходимости)
  utils/                 # Утилиты, специфичные для клиентов (логирование, преобразование payload)
    __init__.py
    common.py
  exceptions.py          # Тонкие реэкспорты из base/exceptions для обратной совместимости
  __init__.py            # Стабильные публичные импорты (протоколы, фабрики)
```

## План изменений по файлам
| Текущий путь | Новый путь | Причина | Комментарий |
| --- | --- | --- | --- |
| `src/bioetl/clients/base.py` | `src/bioetl/clients/base/interfaces.py` | Содержит протоколы/реестр фабрик; должен жить в базовом подпакете | Оставить тонкий реэкспорт `clients/base.py` на переходный период |
| `src/bioetl/clients/common.py` | `src/bioetl/clients/utils/common.py` | Вспомогательные функции не являются публичным API | Возможен реэкспорт через `utils/__init__.py` |
| `src/bioetl/clients/exceptions.py` | `src/bioetl/clients/base/interfaces.py` или отдельный `base/exceptions.py` | Исключения относятся к базовой инфраструктуре | В корне оставить файл-реэкспорт для совместимости |
| `src/bioetl/clients/enricher_base.py` | `src/bioetl/clients/providers/routes.py` + `src/bioetl/clients/base/http.py` + `src/bioetl/clients/base/pagination.py` | Содержит смешение маршрутизатора, HTTP-обёртки и пагинации | Разделить: генератор классов маршрутов → `routes.py`; базовая HTTP/пагинация → `base/` |
| `src/bioetl/clients/enricher_factory.py` | `src/bioetl/clients/factories/enricher_factory.py` | Это фабрика клиентов, логично держать в подпакете factories | Оставить реэкспорт в `clients/enricher_factory.py` до миграции вызовов |
| `src/bioetl/clients/enricher_facade.py` | — | Устаревший фасад обогащения | Удалён как неиспользуемый |
| `src/bioetl/clients/enricher_strategy_registry.py` | — | Реестр стратегий обогащения | Удалён вместе с фасадом |
| `src/bioetl/clients/chembl/*` | `src/bioetl/clients/chembl/*` (без изменений по именам файлов) | Уже выделенный домен; потребуется импортировать общие базовые классы из `base/` | Можно переместить `compat.py`/`facade.py`/`registry.py` рядом, сохраняя поддиректорию |
| `src/bioetl/clients/providers/*.py` | `src/bioetl/clients/providers/{routes.py, base_provider.py, <provider>.py}` | Приведение к единому входу для генератора маршрутов и базового клиента | Отдельный файл `routes.py` для декларативного генератора; тонкий базовый класс для DataProviderProtocol |
| `src/bioetl/clients/openalex.py`, `crossref.py`, `pubchem.py`, `pubmed.py`, `semantic_scholar.py`, `uniprot.py` | `src/bioetl/clients/providers/<same>.py` | Эти файлы уже фактически провайдеры; нужно собрать в подпакет | Оставить реэкспорт в корне на время миграции импорта |
| `src/bioetl/clients/pubchem.py` (корень) | `src/bioetl/clients/providers/pubchem.py` | Дублирует имя с провайдером; должен жить в подпакете | После переноса удалить корневой файл, оставить реэкспорт, если требуется |
| `src/bioetl/clients/__init__.py` | `src/bioetl/clients/__init__.py` (обновить импорты) | Стабильная точка входа | Экспортировать протоколы/фабрики из новых путей, логировать DeprecationWarnings для старых алиасов |

## Замечания по миграции
- **Обратная совместимость**: для публичных импортов (`from bioetl.clients import EnricherClientFactory`, `from bioetl.clients import crossref`) оставить корневые файлы-реэкспорты с предупреждениями до обновления пайплайнов; внутренние импорты внутри `clients` можно обновить сразу.
- **Нерушимые API**: протоколы клиентов и фабричные методы (`ClientFactory.create`, `EnricherClientFactory.create`, `ChemblEntityClientFactory`) задокументировать как стабильные и реэкспортировать из `clients/__init__.py`.
- **Этапы переноса**:
  1. Создать подпакеты `base/`, `factories/`, `utils/`, переместить протоколы/исключения/общие утилиты; добавить реэкспорты в старых файлах.
  2. Перенести провайдерные файлы в `providers/`, выделить `routes.py` и `base_provider.py`; обновить внутренние импорты в фабриках и фасадах.
  3. Очистить временные прокси в корне (`crossref.py`, `openalex.py` и др.) после обновления потребителей; сократить `compat.py` по мере перевода на новые базовые классы.
- **Проверки**: после каждого шага прогонять smoke-тесты клиентов и фасадов (минимум: импорт модулей, вызовы `fetch_one` на моках) и статический анализ импортов (`python -m compileall src/bioetl/clients`).
