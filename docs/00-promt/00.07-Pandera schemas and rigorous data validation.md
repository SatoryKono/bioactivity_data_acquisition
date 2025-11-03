Промт: «Pandera-схемы и жёсткая валидация данных»
Роль и режим

Ты — системный аналитик ETL и разработчик. Работаешь строго по содержимому ветки test_refactoring_32 репозитория SatoryKono/bioactivity_data_acquisition. Никаких внешних предположений. Источник истины — только файлы репозитория. Все ссылки на код и документы указывай строго так:

[ref: repo:<path>@refactoring_001]

Обязательные внутренние источники

[ref: repo:README.md@refactoring_001]

[ref: repo:docs/requirements/00-architecture-overview.md@refactoring_001]

[ref: repo:docs/pipelines/PIPELINES.md@refactoring_001]

[ref: repo:src/bioetl/pipelines/base.py@refactoring_001]

[ref: repo:src/bioetl/configs/models.py@refactoring_001]

[ref: repo:docs/qc/QA_QC.md@refactoring_001]

[ref: repo:tests/@refactoring_001]

Обязательные внешние факты (для ссылок в пояснениях)

DataFrameSchema(strict=..., ordered=..., coerce=...), колонко-порядок и строгий режим. 
pandera.readthedocs.io
+2
pandera.readthedocs.io
+2

Проверки и DataFrame-уровневые Check, в том числе групповые/мультиколоночные. 
pandera.readthedocs.io
+1

Коэрсинг типов и нюансы nullable. 
pandera.readthedocs.io
+1

Экспорт/импорт схем в YAML для фиксации версии/портабельности. 
pandera.readthedocs.io

Нет «родной» версии схемы в Pandera, версионирование реализуем на уровне проекта. 
GitHub

Задача

Подготовь самодостаточную спецификацию «Pandera-схемы и жёсткая валидация данных» для ветки test_refactoring_32. Документ должен описывать:

как в проекте объявляются схемы Pandera для каждого пайплайна;

как обеспечиваются строгий состав колонок, порядок и типы;

как выполняются DataFrame-уровневые проверки, включая уникальность по композиции колонок;

как устроены версии схем и политика эволюции;

как эти правила интегрируются с CLI, конфигами и профилем determinism.yaml.

Что нужно выдать

Один файл Markdown: docs/validation/01-pandera-schemas.md.

Структура содержания
01. Обзор и цели

Роль Pandera в проекте: контракт данных для стадий transform и validate в каркасе PipelineBase.

Опора на строгий режим (strict) и фиксированный порядок колонок (ordered) для детерминизма выгрузок. Короткая справка по coerce для типобезопасности. 
pandera.readthedocs.io
+2
pandera.readthedocs.io
+2

02. Базовый контракт схем

Требования к каждой схеме:

strict=True или strict="filter" с объяснением последствий;

ordered=True для фиксации колонко-порядка;

coerce=True на уровне схемы или колонок;

явные dtype, nullable, unique там, где применимо. 
pandera.readthedocs.io
+1

Формат объявления: DataFrameSchema и/или SchemaModel с примерами. 
pandera.readthedocs.io
+1

03. Каталог схем по пайплайнам

Для activity, assay, target, document, testitem:

путь к файлу схемы;

таблица колонок: name | dtype | required | nullable | checks | comment;

бизнес-ключ и правило уникальности (см. §05);

ссылка на тесты.

Все пути в формате [ref: repo:...@refactoring_001].

04. Типы и коэрсинг

Политика типов: используемые dtype и их коэрсинг при validate().

Нормы nullable с оговоркой, что типовая проверка первична и может «сломать» желаемую null-политику. 
pandera.readthedocs.io
+1

05. DataFrame-уровневые проверки

Шаблон для уникальности по композиции колонок с pa.Check над всем DataFrame:

пример проверки отсутствия дублей ~df.duplicated(subset=[...]).any();

примеры групповых/мультиколоночных проверок и сигнатуры Check на уровне DataFrame. 
pandera.readthedocs.io
+1

06. Жёсткая колонко-дисциплина

strict=True/strict="filter": запрет лишних колонок, реакция на недостающие, оговорка про «фильтрацию» и риски. 
pandera.readthedocs.io

ordered=True: фиксируем порядок; политика отказа при несоответствии. 
pandera.readthedocs.io

07. Версии схем и эволюция

Отсутствие встроенного версионирования в Pandera и проектная политика:

версию схемы храним в репозитории и в meta.yaml выгрузки;

при изменениях повышаем schema_version, ведём CHANGELOG схем;

миграции golden-артефактов. 
GitHub

Рекомендовать экспорт схем в YAML (to_yaml) и контроль диффов на PR. 
pandera.readthedocs.io

08. Интеграция с CLI и конфигами

Где схема вызывается в пайплайне: стадия validate.

Как PipelineConfig и профиль determinism.yaml влияют на поведение валидации/сериализации, не нарушая контракт схемы.

Ссылки на точки входа CLI и места подмешивания профилей:

[ref: repo:src/bioetl/cli/app.py@refactoring_001]

[ref: repo:src/bioetl/configs/models.py@refactoring_001]

[ref: repo:docs/configs/CONFIGS.md@refactoring_001]

09. Примеры (минимум)

Мини-схема activity с strict, ordered, coerce, колонками и DataFrame-проверкой уникальности бизнес-ключа.

Демонстрация провала валидации при лишней колонке и при неправильном порядке.

Экспорт в YAML и обратный импорт (кратко). 
pandera.readthedocs.io

10. Тест-план

Юнит-тесты:

успешная валидация корректного набора;

провал при лишней/пропавшей колонке;

провал при неправильном порядке;

провал при нарушении композиционной уникальности.

Интеграционные: прогон пайплайна, проверка что validate отрабатывает до write, а meta.yaml фиксирует schema_version.

Golden-тесты: стабильность колонко-порядка и типов между запусками.

Требования к оформлению

Язык: русский.

Один файл docs/validation/01-pandera-schemas.md.

Внутренние ссылки только в формате [ref: repo:<path>@refactoring_001].

Внешние технические сноски — только на официальные ресурсы Pandera (доки и reference), для справок о поведении strict, ordered, coerce, Check, YAML-экспорт. 
pandera.readthedocs.io
+5
pandera.readthedocs.io
+5
pandera.readthedocs.io
+5

Критерии приёмки (MUST)

Для каждого пайплайна задекларирована Pandera-схема с strict=True и ordered=True. 
pandera.readthedocs.io
+1

Определены DataFrame-уровневые проверки для бизнес-ключей и критичных инвариантов. 
pandera.readthedocs.io

Описана и применена проектная политика версионирования схем с фиксацией в meta.yaml; даны инструкции по YAML-экспорту. 
GitHub
+1

Тест-план покрывает «лишние/недостающие колонки», «неверный порядок», «коэрсинг/nullable», «композиционная уникальность». 
pandera.readthedocs.io