# Политика создания и локализации новых объектов (ABC / Default / Impl и т.д.)

**Purpose and scope**  
Цель: установить однозначные правила именования, размещения, регистрации и поддержки абстракций (ABC/Protocol), их дефолтных фабрик (Default) и конкретных реализаций (Impl). Политика применяется ко всему коду в `src/bioetl/clients` и сопутствующей документации, реестрам и CI-конфигурации.  

**Scope and sources of truth**  
- `src/bioetl/clients/base/contracts.py` — shared Protocol/ABC (source of truth для общих контрактов)  
- `src/bioetl/clients/<domain>/contracts.py` — domain-scoped contracts  
- `src/bioetl/clients/<domain>/factories.py` — Default factories (рекомендуемая локализация)  
- `src/bioetl/clients/<domain>/impl/` — реализационные модули (Impl)  
- `src/bioetl/clients/base/abc_registry.yaml` — машинный реестр ABC (source of truth)  
- `src/bioetl/clients/base/abc_impls.yaml` — мэппинг Default/Impl (source of truth)  
- `docs/ABC_INDEX.md` — человекочитаемый каталог ABC (source of truth для людей)  
- CI configuration (`.github/workflows/*`) — правила проверки и исполнение политик

---

## Enforcement and exceptions  <!-- таблица: кто, как, когда -->
| #таблицы-01.01 | Rule / Subject | Responsible | Exception process | Актуальность | Description |
|---:|---|---|---|:---:|---|
| 01.01 | Enforcement Owner | Architecture team (or designated owner) | Exceptions require documented PR, approval by architecture owner, and a time-limited waiver recorded in `docs/exceptions.md`. | true | The architecture team is responsible for enforcing policy, reviewing exceptions, and ensuring compliance across repositories and CI. |
| 01.02 | Exception Recording | PR author & approver | All exceptions are recorded with rationale, expiry date and reviewer in `docs/exceptions.md` and linked in PR. | true | Exceptions are not informal — they must be recorded in a central doc and include a timeline and mitigation plan. |
| 01.03 | Audit & Revocation | Security/Architecture | Periodic audit (quarterly) to revoke stale exceptions; violations trigger remediation plan. | true | Policy compliance is audited periodically; stale exceptions are revoked and remediation tracked until complete. |

---

## 1. Общие правила именования (таблица правил)  
| #таблицы-02.01 | Rule | Pattern / Example | Актуальность | Description |
|---:|---|---:|:---:|---|
| 02.01 | Class names must be PascalCase | `ChemblDataClient` | true | Имена классов должны быть в PascalCase; это упрощает чтение, соответствие стайлгайду и автоматическую проверку по регулярным выражениям. |
| 02.02 | Functions/methods in snake_case | `fetch_one`, `iter_pages` | true | Функции и методы обязаны использовать snake_case: это согласуется с PEP8 и снижает неоднозначность интерфейсов и сигнатур. |
| 02.03 | Filenames in snake_case.py | `data_client.py` | true | Файлы должны быть названы в snake_case с расширением `.py`, чтобы навигация и импорт были предсказуемыми и совместимыми с packaging. |
| 02.04 | Private names start with _ | `_resilient_transport` | true | Приватные сущности обозначаются ведущим подчёркиванием; это сигнал для разработчиков, что API не предназначен для публичного использования. |
| 02.05 | Export public symbols in __all__ | `__all__ = ["ChemblDataClient"]` | true | Публичные сущности должны быть явно перечислены в `__all__` или реэкспортированы в `__init__.py` для стабильного публичного API. |

---

## 2. Роли классов — однозначные суффиксы (таблица)  
| #таблицы-03.01 | Role | Required Suffix | Актуальность | Description |
|---:|---:|---:|:---:|---|
| 03.01 | Factory (general) | `Factory` | true | Фабрики, создающие сущности, обязательно заканчиваются на `Factory`, чтобы сразу было понятно, что класс занимается инстанциированием конфигураций или объектов. |
| 03.02 | ClientFactory | `ClientFactory` | true | Фабрики, создающие клиенты, обязаны содержать `ClientFactory` в имени; это позволяет быстро отличить фабрики клиентов от других фабрик. |
| 03.03 | DataClient (contract impl) | `DataClient` | true | Реализации контракта `DataClient` должны содержать `DataClient`, что явно указывает на выполнение конкретного протокола доступа к данным. |
| 03.04 | Client | `Client` | true | Общие клиенты, не реализующие строго DataClient, используют суффикс `Client`, обозначая уровень ответственности и контрактность. |
| 03.05 | Facade | `Facade` | true | Фасады высокого уровня заканчиваются на `Facade`, чтобы отличать их от низкоуровневых клиентов и фабрик. |
| 03.06 | Registry | `Registry` | true | Реестры обязаны иметь суффикс `Registry`, чтобы имя отражало роль хранения и поиска фабрик/реализаций. |
| 03.07 | Adapter / Transport | `Adapter` / `Transport` | true | Низкоуровневые адаптеры и транспорты используют эти суффиксы для ясного разграничения ответственности в сети/IO. |
| 03.08 | Protocol / ABC | `Protocol` / `ABC` | true | Контракты оформляются как `Protocol` или `ABC`, что однозначно сигнализирует о их интерфейсной роли. |
| 03.09 | Config / Model / Params | `Config` / `Model` / `Params` | true | Конфигурационные и модельные объекты используют соответствующие суффиксы, для однозначного определения их назначения. |
| 03.10 | Error | `Error` | true | Исключения именуются с суффиксом `Error`, что упрощает их обработку и поиск в проекте. |

---

## 3. Префиксы функций и смысл (таблица)  
| #таблицы-04.01 | Prefix | When to use | Актуальность | Description |
|---:|---:|---:|:---:|---|
| 04.01 | `get_` | Cheap/local reads or cached values | true | `get_` используется для дешёвых локальных операций, обычно без сетевого IO; это помогает отличать быстрые и дорогие вызовы. |
| 04.02 | `fetch_` | Network/IO operations | true | `fetch_` обозначает дорогостоящие сетевые или IO-операции; это предупреждает о возможных задержках и необходимости таймаутов. |
| 04.03 | `iter_` | Return lazy generator/iterator | true | `iter_` для ленивых генераторов и итераций; важно, чтобы возвращаемый тип был итератором, а не `list`. |
| 04.04 | `create_/build_/make_/default_` | Object/factory creation | true | Префиксы для создания: `create_` явное создание, `build_` — сборка конфигурации, `default_` — фабрика по умолчанию. |
| 04.05 | `register_` | Registration actions | true | `register_` подходит для регистрации фабрик/адаптеров в реестре; такие методы изменяют глобальные маппинги. |
| 04.06 | `resolve_/ensure_` | Normalization | true | `resolve_` и `ensure_` используются для подготовки/нормализации данных и конфигураций перед основной операцией. |
| 04.07 | `validate_/parse_/serialize_` | Validation/parsing | true | Эти префиксы обозначают валидацию/парсинг/сериализацию — операции чисто преобразующего характера. |
| 04.08 | `on_` | Event handlers/callbacks | true | `on_` для коллбэков/обработчиков событий; чётко обозначает реактивную роль метода. |
| 04.09 | `is_/has_/can_` | Boolean checks | true | Для методов, возвращающих булево значение, применять положительную форму `is_`, `has_` или `can_`. |

---

## 4. Правила свойств и параметров (таблица)  
| #таблицы-05.01 | Rule | Pattern / Example | Актуальность | Description |
|---:|---|---:|:---:|---|
| 05.01 | Public properties as nouns | `name`, `source` | true | Публичные свойства должны быть существительными, предоставляя ясное описание состояния объекта, что облегчает понимание API и автодок. |
| 05.02 | Private properties with leading underscore | `_delegate`, `_transport` | true | Приватные поля отмечаются `_`, явный договор нежелания внешнего использования и уменьшение утечек API. |
| 05.03 | Boolean flags in positive form | `is_enabled`, `allow_cache` | true | Булевы имена должны быть позитивными, предотвращая логические ошибки при проверках и улучшая читаемость условий. |
| 05.04 | Config/options names | `config`, `options` | true | Использовать полные слова `config`/`options` вместо сокращений, чтобы избежать неоднозначности и облегчить рефакторинг. |
| 05.05 | Collections in plural | `records`, `adapters` | true | Коллекции именовать во множественном числе для четкого семантического разделения одиночных и множественных сущностей. |

---

## 5. Правила для файлов и пакетов (таблица)  
| #таблицы-06.01 | Artifact | Filename / Path | Актуальность | Description |
|---:|---|---:|:---:|---|
| 06.01 | Shared contracts | `src/bioetl/clients/base/contracts.py` | true | Общие протоколы и типы хранятся в одном центре, облегчая переиспользование и поддержание единого контракта для всех доменов. |
| 06.02 | Domain contracts | `src/bioetl/clients/<domain>/contracts.py` | true | Домен-специфичные контракты располагаются рядом с реализациями домена, что повышает локальную читаемость и уменьшает разрозненность. |
| 06.03 | Default factories | `src/bioetl/clients/<domain>/factories.py` | true | Фабрики по умолчанию должны быть в `factories.py`, чтобы потребитель мог легко найти рекомендуемую точку входа для клиента. |
| 06.04 | Impl directory | `src/bioetl/clients/<domain>/impl/` | true | Все конкретные реализации помещаются в `impl/` для удобства навигации и разграничения публичных API и внутренней реализации. |
| 06.05 | Transport/adapter files | `transport.py`, `*_adapter.py` | true | Низкоуровневые транспортные и адаптерные реализации находятся в отдельном файле для явного разграничения уровней абстракции. |
| 06.06 | Registry files | `registry.py` | true | Реестры должны находиться в `registry.py`, централизуя регистрацию и получение фабрик, облегчая тестирование и инверсию зависимостей. |

---

## 6. Политика создания новых ABC / Protocol (таблица правил и процесс)
| #таблицы-07.01 | Rule | Актуальность | Description |
|---:|---:|:---:|---|
| 07.01 | Default mandatory on ABC creation | true | При создании нового ABC обязательно создать Default-фабрику, даже если реальная реализация отсутствует — это обеспечивает точку входа и снижает барьер использования. |
| 07.02 | Default may be a stub if no Impl exists | true | Если реальных реализаций нет, Default может возвращать stub или бросать NotImplemented, но фабрика должна существовать и быть документирована. |
| 07.03 | Adding Impl does not require new Default | true | При появлении новой реализации достаточно зарегистрировать её в списке Impl; создание нового Default требуется только по обоснованной архитектурной причине. |
| 07.04 | ABC docstring mandatory structured block | true | Каждый ABC должен иметь докстринг со структурированными полями: краткое описание, публичный интерфейс, локализация, Default и ссылка на `abc_impls.yaml`. |

---

## 7. Default и Impl — детальные требования (таблица)  
| #таблицы-08.01 | Artifact | Placement / Pattern | Актуальность | Description |
|---:|---|---:|:---:|---|
| 08.01 | Default factory naming | `default_<domain>_<entity>` | true | Имя дефолтной фабрики должно быть однозначным и содержать `default_` для мгновенного понимания, что это рекомендуемый конструктор. |
| 08.02 | Default placement | `src/bioetl/clients/<domain>/factories.py` | true | Размещать дефолты в `factories.py` — центральная точка обнаружения рекомендуемых конфигураций и облегчения импорта. |
| 08.03 | Impl naming | `[Domain][Entity][Role]Impl` | true | Реализации именуются с суффиксом `Impl` и опциональной дополнительной спецификацией, например `HTTPImpl`, чтобы показать тип транспорта или поведения. |
| 08.04 | Impl placement | `src/bioetl/clients/<domain>/impl/` | true | Все имплементации размещаются в `impl/` для упорядочивания и разделения публичных и внутренних артефактов. |
| 08.05 | Registration of Impl | `abc_impls.yaml` update | true | Каждая новая имплементация требует добавления записи в `abc_impls.yaml`, чтобы CI и разработчики знали о существующих вариантах. |

---

## 8. Реестры и форматы (таблица)  
| #таблицы-09.01 | Artifact | Format / Example | Актуальность | Description |
|---:|---|---:|:---:|---|
| 09.01 | abc_registry.yaml | YAML list of ABC objects with name, description, public_interface, file_path, exported_name, default_factory, domain | true | Машинный реестр ABC обеспечивает централизованный каталог интерфейсов и их локализации для автоматизированных проверок и документации. |
| 09.02 | abc_impls.yaml | Mapping ABC -> { default: [], impls: [] } | true | `abc_impls.yaml` содержит актуальные Default и Impl для каждого ABC; необходим для CI, discovery и выбора реализаций. |
| 09.03 | docs/ABC_INDEX.md | Human-readable table with same fields | true | Человекочитаемая версия реестра помогает разработчикам быстро ознакомиться с контрактами и рекомендациями по использованию. |

---

## 9. Докстринг у ABC — формат и проверка (таблица)  
| #таблицы-10.01 | Field | Required / Example | Актуальность | Description |
|---:|---:|---:|:---:|---|
| 10.01 | Краткое описание | Required — 1-2 предложения | true | Докстринг обязан начинаться с краткого описания, объясняющего назначение ABC и область его применения в проекте. |
| 10.02 | Публичный интерфейс | Required — methods with signatures | true | В докстринге перечислить основные публичные методы и их сигнатуры; это помогает генерации документации и быстрому пониманию контракта. |
| 10.03 | Локализация | Required — path to file | true | Указать точную локализацию файла, где объявлен ABC, чтобы избежать путаницы и облегчить рефакторинг. |
| 10.04 | Default/Impl pointers | Required — ссылочные строки | true | Включить ссылку на Default factory и указание места `abc_impls.yaml`, чтобы пользователи знали рекомендованные реализации и альтернативы. |

---

## 10. PR-чеклист (таблица)  
| #таблицы-11.01 | Item | Required for | Актуальность | Description |
|---:|---|---:|:---:|---|
| 11.01 | Correct class name & file | All PRs adding classes | true | При добавлении класса имя и файл должны соответствовать политике; CI проверяет соответствие суффиксов и расположение. |
| 11.02 | Default for new ABC | PR that creates new ABC | true | Новое ABC должно сопровождаться Default factory (или stub) и записью в `abc_impls.yaml`; это обязательный шаг. |
| 11.03 | Impl registration | PR adding Impl | true | Новая имплементация должна добавляться в `abc_impls.yaml`, сопровождаться тестами и документацией. |
| 11.04 | Docs & registry update | PR adding/modifying ABC or Default | true | `docs/ABC_INDEX.md` и `abc_registry.yaml` должны быть обновлены параллельно с кодом. |
| 11.05 | Tests | All changes that affect behavior | true | Unit и при необходимости integration тесты обязаны быть добавлены, чтобы изменения были проверяемы в CI. |

---

## 11. CI / pre-commit checks (таблица)  
| #таблицы-12.01 | Check | What it verifies | Актуальность | Description |
|---:|---|---:|:---:|---|
| 12.01 | Naming regex checks | Class suffix and function prefixes | true | CI выполняет статическую проверку имён классов и функций по набору регекс-правил, чтобы исключить вариативность в нейминге. |
| 12.02 | Default existence for new ABC | abc_impls.yaml contains default | true | При добавлении нового ABC CI проверяет, что `abc_impls.yaml` содержит `default` запись или валидный stub. |
| 12.03 | abc_registry sync | abc_registry.yaml содержит ABC | true | CI проверяет, что каждый ABC объявлен и зарегистрирован в `abc_registry.yaml`. |
| 12.04 | Docstring block presence | ABC has structured docstring | true | Парсер проверяет, что у ABC есть требуемый структурированный докстринг с необходимыми полями. |
| 12.05 | File/class name match | Public class ↔ filename | true | Если файл содержит один публичный класс, CI сверяет имя класса и имя файла для единообразия. |

---

## 12. Enforcement and exceptions — подробности (таблица)  
| #таблицы-13.01 | Exception Type | Who approves | Validity period | How recorded | Актуальность | Description |
|---:|---:|---:|---:|---:|:---:|---|
| 13.01 | Naming exception | Architecture owner | up to 90 days | Record in `docs/exceptions.md` + PR note | true | Если необходима временная отступка по именованию, архитектурный владелец одобряет её на ограниченный период; все детали документируются. |
| 13.02 | No-default-on-ABC exception | Architecture board | up to 30 days | PR rationale and `docs/exceptions.md` | true | Разрешение не создавать Default при новой ABC может быть выдано, но должно быть документировано с чётким обоснованием и сроком. |
| 13.03 | Registry-sync delay | Repo maintainer | 7 days | Issue linked to PR and `abc_registry.yaml` | true | Если реестр не обновлён вовремя, поддерживающий репозиторий создаёт issue и даёт 7 дней на исправление, после чего CI блокирует merge. |

---

## 13. Examples & templates (short pointers)
- `contracts_template.py` — Protocol/ABC template with required docstring block.  
- `factories_template.py` — Default factory template, with docstring indicating recommended parameters.  
- `impl_http_template.py` — Impl skeletal class.  
- `registry_template.py` — Domain registry example.  
- `naming_checks.py` — Simple script for regex checks and verifying Default presence.  

---

## 14. Auditability and change log
- All changes to Default or ABC public interfaces must be logged in `UPGRADING.md` with date, author, reason, and migration notes.  
- Periodic audits (quarterly) are run by Architecture; results are recorded and any stale exceptions are revoked.

---

## 15. Quick cheat-sheet (1 page)
- New ABC? Create Default (even stub), add ABC to `abc_registry.yaml` and `docs/ABC_INDEX.md`, add Default to `abc_impls.yaml`.  
- New Impl? Place in `impl/`, add to `abc_impls.yaml`, add tests. No Default needed.  
- Naming? Classes PascalCase, funcs snake_case, filenames snake_case.py. Specific suffix rules: see §3.  
- Docstrings for ABC must include required fields; CI enforces it.

---

## Appendices: Example `abc_registry.yaml` entry (machine-readable)
```yaml
- name: DataClientProtocol
  description: "Контракт для клиентов доступа к данным: fetch_one, iter_pages и др."
  public_interface:
    - "fetch_one(self, request: ClientRequest) -> dict"
    - "iter_pages(self, params: PaginationParams) -> Iterator[Page]"
  file_path: "src/bioetl/clients/base/contracts.py"
  exported_name: "DataClientProtocol"
  default_factory: "src/bioetl/clients/chembl/factories.py::default_chembl_data_client"
  domain: "base"
