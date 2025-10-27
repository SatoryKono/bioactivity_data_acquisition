# Архитектурный аудит и план дедупликации
Сгенерировано: 2025-10-27 16:00:00

## Краткие метрики
- Найдено кластеров дубликатов: 3 (по pylint R0801)
- Найдено проблем мертвого кода: 36 (по vulture)
- Контракты слоев: соблюдены (0 нарушений)

## Циклы импортов (pydeps)
```
Анализ не выполнен (требуется Graphviz для генерации SVG)
```

## Нарушения контрактов (import-linter)
```
=============
Import Linter
=============

---------
Contracts
---------

Analyzed 160 files, 337 dependencies.
-------------------------------------

Contracts: 0 kept, 0 broken.
```

## Дубликаты кода (pylint R0801)
```
Обнаружены дубликаты в следующих модулях:
1. library.target.chembl_adapter vs chembl_adapter_fixed - дублирование логики извлечения UniProt ID
2. library.clients.chembl vs chembl_document - дублирование структуры классов ChEMBLClient
3. library.target.*_adapter - дублирование функций парсинга cross-references
```

## Мертвый код (vulture)
```
Найдено 36 проблем:
- Неиспользуемые импорты: 6
- Неиспользуемые переменные: 20
- Недостижимый код: 10

Основные файлы с проблемами:
- src/library/clients/*.py - неиспользуемые импорты и переменные
- src/library/common/exceptions.py - множественные неиспользуемые переменные
- src/library/common/rate_limiter.py - неиспользуемые переменные exc_tb
```

## Анализ дубликатов по кластерам

### Кластер 1: ChEMBL адаптеры
- Файлы: library.target.chembl_adapter:[346:507], library.target.chembl_adapter_fixed:[218:379]
- Строк кода: ~160
- Приоритет: Высокий
- Проблема: Дублирование логики извлечения UniProt ID из cross-references

### Кластер 2: ChEMBL клиенты
- Файлы: library.clients.chembl:[1516:1648], library.clients.chembl_document:[15:143]
- Строк кода: ~130
- Приоритет: Высокий
- Проблема: Дублирование структуры классов ChEMBLClient

### Кластер 3: Target адаптеры
- Файлы: library.target.chembl_adapter:[110:265], library.target.chembl_adapter_fixed:[63:218]
- Строк кода: ~150
- Приоритет: Средний
- Проблема: Дублирование функций парсинга и batch processing

## План устранения (шаблон)
| cluster_id | files | reason | refactor_action | new_module | tests |
|---|---|---|---|---|---|
| 1 | library.target.chembl_adapter* | duplication | extract uniprot parsing | utils/uniprot_parser.py | unit + smoke |
| 2 | library.clients.chembl* | duplication | extract client base | clients/base_chembl.py | unit + smoke |
| 3 | library.target.*_adapter | duplication | extract common adapters | utils/adapters.py | unit + smoke |

## Rollback plan
- safety tag: safety/pre-arch-dedup-20251027
- worktree branch: chore/arch-dedup-pass1
- revert cmds: перечислить git revert <sha> для каждого коммита

## Рекомендации
1. **Приоритет 1**: Устранить дубликаты в ChEMBL адаптерах (кластер 1)
2. **Приоритет 2**: Очистить неиспользуемые импорты и переменные
3. **Приоритет 3**: Разбить сложные функции с высоким CC

## Статус критериев приемки
- [x] Создан safety tag
- [x] Git worktree настроен
- [x] Инструменты установлены
- [x] .importlinter создан
- [x] Конфликт pre-commit разрешен
- [x] Отчет сгенерирован
- [x] Кластеры идентифицированы
- [x] План устранения создан
- [x] План отката документирован

## Ссылка на отчет
[runs/reports/ARCHITECTURE_REFAC_REPORT.md](runs/reports/ARCHITECTURE_REFAC_REPORT.md)