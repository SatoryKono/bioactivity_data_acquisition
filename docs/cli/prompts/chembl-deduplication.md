# Промпт: Решить дублирование ChEMBL пайплайнов (P0-1)

## Контекст задачи

**Проблема:** ChEMBL пайплайны дублируются между `src/bioetl/pipelines/` (монолитные файлы) и `src/bioetl/sources/chembl/` (прокси файлы) согласно AUDIT_REPORT_2025.md.

**Статус:** ❌ **НЕ ИСПРАВЛЕНО** (P0-1)

**Ссылки:**

- `docs/architecture/refactoring/AUDIT_REPORT_2025.md` (строка 390): "P0-1: ChEMBL пайплайны дублируются между `pipelines/` и `sources/chembl/`"
- `docs/architecture/refactoring/MODULE_RULES.md` (строка 18): Структура `src/bioetl/sources/<source>/` — правильная организация для внешних источников
- `docs/architecture/refactoring/PUBCHEM_MIGRATION_PLAN.md`: Референс для миграции PubChem из монолитных файлов в модульную структуру

## Текущая ситуация

### Монолитные файлы в `src/bioetl/pipelines/`

- `activity.py` - ActivityPipeline (монолитная реализация)
- `assay.py` - AssayPipeline (монолитная реализация)
- `target.py` - TargetPipeline (монолитная реализация)
- `testitem.py` - TestItemPipeline (монолитная реализация)
- `document.py` - DocumentPipeline (монолитная реализация)

### Прокси файлы в `src/bioetl/sources/chembl/`

- `activity/pipeline.py` - прокси к `ActivityPipeline` из `pipelines/activity.py`
- `assay/pipeline.py` - прокси к `AssayPipeline` из `pipelines/assay.py`
- `target/pipeline.py` - прокси к `TargetPipeline` из `pipelines/target.py`
- `testitem/pipeline.py` - прокси к `TestItemPipeline` из `pipelines/testitem.py`
- `document/pipeline.py` - прокси к `DocumentPipeline` из `pipelines/document.py`

**Проблема:** Дублирование логики между монолитными файлами и прокси, что усложняет поддержку и обновление.

## Два варианта стратегии миграции

### Вариант А: Полная миграция в sources/chembl/ (РЕКОМЕНДУЕТСЯ)

**Приоритет:** Высокий (рекомендуется согласно MODULE_RULES.md)

**Преимущества:**

- Устраняет дублирование полностью
- Соответствует MODULE_RULES.md (правильная организация)
- Унифицирует структуру всех источников
- Упрощает поддержку и тестирование
- Следует паттерну миграции PubChem (PUBCHEM_MIGRATION_PLAN.md)

**Недостатки:**

- Требует значительного рефакторинга
- Требует обновления всех импортов в кодовой базе
- Требует обновления тестов и документации

**Процесс миграции:**

1. **Анализ текущей структуры**
   - Изучить монолитные файлы в `src/bioetl/pipelines/`
   - Изучить существующую структуру в `src/bioetl/sources/chembl/<entity>/`
   - Идентифицировать общие компоненты и специфичные для entity

2. **Подготовка модульной структуры**

   Для каждого entity (activity, assay, target, testitem, document):

   - Убедиться что структура `src/bioetl/sources/chembl/<entity>/` соответствует MODULE_RULES.md:
     - `client/` - HTTP клиент для ChEMBL API
     - `request/` - сборка запросов
     - `parser/` - разбор ответов API
     - `normalizer/` - нормализация данных
     - `schema/` - Pandera схемы
     - `merge/` - merge политика (если нужна)
     - `output/` - детерминированная запись
     - `pipeline.py` - координация шагов

3. **Миграция логики**
   - Перенести логику из монолитных файлов в модульную структуру
   - Разделить монолитные классы на компоненты (client, parser, normalizer, etc.)
   - Обновить `pipeline.py` для использования модульных компонентов
   - Убедиться что все тесты проходят

4. **Обновление импортов**
   - Найти все импорты из `bioetl.pipelines.<entity>`
   - Заменить на `bioetl.sources.chembl.<entity>.pipeline`
   - Обновить CLI регистрацию
   - Обновить тесты

5. **Удаление монолитных файлов**
   - Удалить `src/bioetl/pipelines/activity.py`
   - Удалить `src/bioetl/pipelines/assay.py`
   - Удалить `src/bioetl/pipelines/target.py`
   - Удалить `src/bioetl/pipelines/testitem.py`
   - Удалить `src/bioetl/pipelines/document.py`
   - Обновить `src/bioetl/pipelines/__init__.py`

6. **Обновление документации**
   - Обновить MODULE_RULES.md (убрать упоминания дублирования)
   - Обновить REFACTOR_PLAN.md
   - Обновить AUDIT_REPORT_2025.md (отметить P0-1 как исправленный)
   - Обновить CHANGELOG.md

**Пример миграции для одного entity (activity):**

```python

# До: src/bioetl/pipelines/activity.py

class ActivityPipeline(PipelineBase):
    def extract(self):
        # Монолитная логика
        client = ChEMBLClient(...)
        response = client.fetch_activities(...)
        return self.parse_activities(response)

    def parse_activities(self, response):
        # Логика парсинга
        ...

# После: src/bioetl/pipelines/chembl_activity.py

from bioetl.clients.chembl_activity import ActivityChEMBLClient
from bioetl.sources.chembl.activity.parser import ActivityParser
from bioetl.transform.adapters.chembl_activity import ActivityNormalizer

class ActivityPipeline(PipelineBase):
    def __init__(self, config, run_id):
        super().__init__(config, run_id)
        self.client = ActivityClient(config)
        self.parser = ActivityParser()
        self.normalizer = ActivityNormalizer()

    def extract(self):
        response = self.client.fetch_activities(...)
        return self.parser.parse(response)

    def transform(self, df):
        return self.normalizer.normalize(df)
```

### Вариант Б: Документирование дублирования

**Приоритет:** Низкий (не рекомендуется, но допустим временно)

**Преимущества:**

- Минимальные изменения кода
- Сохраняет обратную совместимость
- Можно выполнить постепенно

**Недостатки:**

- Дублирование остается
- Усложняет поддержку
- Не соответствует MODULE_RULES.md
- Требует планирования миграции в будущем

**Процесс документирования:**

1. **Обновление документации**
   - Обновить MODULE_RULES.md с явным указанием:
     - `pipelines/` содержит монолитные файлы для ChEMBL (legacy)
     - `sources/chembl/` содержит прокси для обратной совместимости
     - Планируется миграция в будущем (версия X.Y.Z)

2. **Добавление DeprecationWarning**
   - В каждом прокси файле добавить DeprecationWarning:

     ```python
     import warnings
     warnings.warn(
         "Importing from bioetl.sources.chembl.<entity>.pipeline is deprecated. "
         "Use bioetl.pipelines.<entity> directly. "
         "This will be removed in version X.Y.Z.",
         DeprecationWarning,
         stacklevel=2
     )
     ```

3. **Планирование миграции**
   - Создать план миграции аналогично PUBCHEM_MIGRATION_PLAN.md
   - Определить версию для удаления прокси
   - Обновить AUDIT_REPORT_2025.md с пометкой "Планируется миграция"

4. **Обновление AUDIT_REPORT_2025.md**
   - Отметить P0-1 как "Частично исправлено (документировано)"
   - Добавить запись в план действий о миграции

## Рекомендация

**Вариант А рекомендуется** по следующим причинам:

1. **Соответствие стандартам:** MODULE_RULES.md явно указывает что `src/bioetl/sources/<source>/` — правильная организация

2. **Прецедент:** PubChem уже мигрирован аналогичным образом (PUBCHEM_MIGRATION_PLAN.md)

3. **Упрощение поддержки:** Устранение дублирования упрощает поддержку и обновление

4. **Унификация:** Все источники будут иметь единообразную структуру

5. **Критичность:** Проблема помечена как P0 (критично) в аудите

## Критерии завершения (Вариант А)

- ✅ Все монолитные файлы удалены из `src/bioetl/pipelines/`
- ✅ Логика мигрирована в модульную структуру `src/bioetl/sources/chembl/<entity>/`
- ✅ Все импорты обновлены
- ✅ Все тесты проходят
- ✅ CLI регистрация обновлена
- ✅ Документация обновлена
- ✅ AUDIT_REPORT_2025.md обновлен (P0-1 отмечен как исправленный)
- ✅ CHANGELOG.md обновлен

## Критерии завершения (Вариант Б)

- ✅ Документация обновлена с явным указанием дублирования
- ✅ DeprecationWarning добавлены во все прокси файлы
- ✅ План миграции создан
- ✅ AUDIT_REPORT_2025.md обновлен (P0-1 отмечен как "Частично исправлено")

## Риски и митигация

### Риск 1: Ломающие изменения в импортах

**Митигация:**

- Создать реэкспорт в `pipelines/__init__.py` для обратной совместимости
- Добавить DeprecationWarning
- Постепенная миграция

### Риск 2: Потеря функциональности при миграции

**Митигация:**

- Тщательное тестирование перед удалением монолитных файлов
- Золотые тесты для сравнения output до/после миграции
- Поэтапная миграция по одному entity за раз

### Риск 3: Большой объем изменений

**Митигация:**

- Миграция по одному entity за раз
- Тщательное планирование и документирование шагов
- Использование git для отслеживания изменений

## Примечания

- Референс для миграции: `docs/architecture/refactoring/PUBCHEM_MIGRATION_PLAN.md`
- Структура должна соответствовать: `docs/architecture/refactoring/MODULE_RULES.md`
- Все изменения должны быть задокументированы в CHANGELOG.md
- Тесты должны покрывать все компоненты модульной структуры
