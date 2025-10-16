# Структура входных и выходных данных

## Обзор

Проект `bioactivity*data*acquisition`обрабатывает данные о биоактивности из
различных источников, включая ChEMBL, PubMed, Crossref, OpenAlex и Semantic
Scholar. Данные проходят через ETL-процесс, который включает извлечение,
трансформацию и загрузку с обогащением из внешних источников.

## Входные данные (data/input/)

### 1. documents.csv

**Описание**: Основной файл с документами из ChEMBL, содержащий метаданные
публикаций.

| Колонка | Тип | Обязательная | Описание |
|---------|-----|--------------|----------|
|`document_chembl_id`| string | Да | Уникальный идентификатор документа в ChEMBL |
|`title`| string | Да | Название публикации |
|`doi`| string | Нет | Digital Object Identifier |
|`pubmed*id`| string | Нет | PubMed идентификатор |
|`doc*type`| string | Нет | Тип документа |
|`journal`| string | Нет | Название журнала |
|`year`| int | Нет | Год публикации |
|`abstract`| string | Нет | Аннотация документа |
|`authors`| string | Нет | Авторы документа |
|`classification`| float | Нет | Классификация документа |
|`document_contains_external_links`| bool | Нет | Содержит ли документ внешние ссылки |
|`first*page`| int | Нет | Номер первой страницы |
|`is*experimental*doc`| bool | Нет | Является ли документ экспериментальным |
|`issue`| int | Нет | Номер выпуска журнала |
|`last*page`| float | Нет | Номер последней страницы |
|`month`| int | Нет | Месяц публикации |
|`volume`| float | Нет | Том журнала |
|`citation`| string | Нет | Форматированная литературная ссылка |

**Примечание**: Колонки с названиями журналов (содержащие 'journal' в названии)
автоматически нормализуются для обеспечения единообразия.

**Размер**: ~15,677 записей

### 2. activity.csv

**Описание**: Данные о биоактивности соединений.

| Колонка | Тип | Обязательная | Описание |
|---------|-----|--------------|----------|
|`activity*chembl*id`| string | Да | Уникальный идентификатор активности |
|`assay*chembl*id`| string | Да | Идентификатор эксперимента |
|`compound*key`| string | Да | Ключ соединения |
|`compound*name`| string | Да | Название соединения |
|`document*chembl*id`| string | Да | Идентификатор документа |
|`molecule*chembl*id`| string | Да | Идентификатор молекулы |
|`standard*type`| string | Да | Тип стандартного значения |
|`standard*value`| float | Да | Стандартное значение активности |
|`target*chembl*id`| string | Да | Идентификатор мишени |
|`original*activity*exact`| float | Да | Точное значение активности |
|`pA*value`| float | Да | pA значение |
|`bao*endpoint`| string | Да | BAO endpoint |
|`bao*format`| string | Да | BAO формат |
|`IUPHAR*class`| string | Да | IUPHAR класс |
|`IUPHAR*subclass`| string | Да | IUPHAR подкласс |
|`gene*index`| string | Да | Индекс гена |
|`taxon*index`| int | Да | Индекс таксона |

**Размер**: ~145,952 записей

### 3. assay.csv

**Описание**: Данные об экспериментах (ассаях).

| Колонка | Тип | Обязательная | Описание |
|---------|-----|--------------|----------|
|`assay*chembl*id`| string | Да | Уникальный идентификатор эксперимента |
|`target*chembl*id`| string | Да | Идентификатор мишени |
|`target*name`| string | Да | Название мишени |
|`target*type`| string | Да | Тип мишени |
|`document*chembl*id`| string | Да | Идентификатор документа |
|`description`| string | Да | Описание эксперимента |
|`bao*format`| string | Да | BAO формат |
|`assay*cell*type`| string | Нет | Тип клетки |
|`assay*subcellular*fraction`| string | Нет | Субклеточная фракция |
|`assay*tissue`| string | Нет | Ткань |
|`substrate*name`| string | Нет | Название субстрата |
|`version`| int | Нет | Версия |
|`year`| int | Нет | Год |
|`month`| int | Нет | Месяц |

**Размер**: ~41,578 записей

### 4. target.csv

**Описание**: Данные о биологических мишенях.

| Колонка | Тип | Обязательная | Описание |
|---------|-----|--------------|----------|
|`target*chembl*id`| string | Да | Уникальный идентификатор мишени |
|`target*names`| string | Да | Названия мишени |
|`target*uniprot*id`| string | Да | UniProt идентификатор |
|`primaryAccession`| string | Да | Основной аксесс-номер |
|`organism`| string | Да | Организм |
|`isoform*ids`| string | Нет | Идентификаторы изоформ |
|`isoform*names`| string | Нет | Названия изоформ |
|`isoforms`| int | Нет | Количество изоформ |
|`pH*dependence`| string | Нет | pH зависимость |

**Размер**: ~1,962 записи

### 5. testitem.csv

**Описание**: Данные о тестируемых соединениях (молекулах).

| Колонка | Тип | Обязательная | Описание |
|---------|-----|--------------|----------|
|`molecule*chembl*id`| string | Да | Уникальный идентификатор молекулы |
|`all*names`| string | Да | Все названия соединения |
|`canonical*smiles`| string | Да | Каноническая SMILES структура |
|`chirality`| int | Да | Хиральность |
|`inchi*key*from*mol`| string | Да | InChI ключ из молекулы |
|`inchi*key*from*smiles`| string | Да | InChI ключ из SMILES |
|`is*radical`| bool | Да | Является ли радикалом |
|`molecule*type`| string | Да | Тип молекулы |
|`mw*freebase`| float | Да | Молекулярный вес свободного основания |
|`n*stereocenters`| int | Да | Количество стереоцентров |
|`nstereo`| int | Да | Количество стереоизомеров |
|`num*ro5*violations`| int | Да | Количество нарушений правила Липински |
|`salt*chembl*id`| string | Нет | Идентификатор соли |
|`standard*inchi*key`| string | Нет | Стандартный InChI ключ |
|`standard*inchi*skeleton`| string | Нет | Скелет стандартного InChI |
|`standard*inchi*stereo`| string | Нет | Стерео стандартного InChI |
|`mw*<100*or*>1000`| bool | Нет | Молекулярный вес <100 или >1000 |

**Размер**: ~41,385 записей

### 6. cell.csv

**Описание**: Идентификаторы клеточных линий.

| Колонка | Тип | Обязательная | Описание |
|---------|-----|--------------|----------|
|`cell*chembl*id`| string | Да | Уникальный идентификатор клеточной линии |

**Размер**: ~9,407 записей

### 7. tissue.csv

**Описание**: Идентификаторы тканей.

| Колонка | Тип | Обязательная | Описание |
|---------|-----|--------------|----------|
|`tissue*chembl*id`| string | Да | Уникальный идентификатор ткани |

**Размер**: ~62 записи

## Выходные данные (data/output/)

### 1. documents*YYYYMMDD.csv

**Описание**: Обогащенные данные документов с информацией из всех внешних
источников.

#### Основные поля ChEMBL (оригинальные)

| Колонка | Тип | Описание |
|---------|-----|----------|
|`document*chembl*id`| string | ChEMBL идентификатор документа |
|`title`| string | Название документа |
|`doi`| string | Digital Object Identifier |
|`pubmed*id`| string | PubMed идентификатор |
|`doc*type`| string | Тип документа |
|`journal`| string | Название журнала |
|`year`| int | Год публикации |
|`abstract`| string | Аннотация |
|`authors`| string | Авторы |
|`classification`| float | Классификация |
|`document*contains*external*links`| bool | Содержит внешние ссылки |
|`first*page`| int | Первая страница |
|`is*experimental*doc`| bool | Экспериментальный документ |
|`issue`| int | Номер выпуска |
|`last*page`| float | Последняя страница |
|`month`| int | Месяц |
|`volume`| float | Том |

#### Поля обогащения

| Колонка | Тип | Описание |
|---------|-----|----------|
|`source`| string | Источник данных (chembl, crossref, openalex, pubmed, semantic_scholar) |

#### OpenAlex поля

| Колонка | Тип | Описание |
|---------|-----|----------|
|`openalex_doi_key`| string | DOI ключ для соединения |
|`openalex_title`| string | Название из OpenAlex |
|`openalex*doc*type`| string | Тип документа из OpenAlex |
|`openalex*type*crossref`| string | Crossref тип из OpenAlex |
|`openalex*publication*year`| int | Год публикации из OpenAlex |
|`openalex*error`| string | Ошибка OpenAlex |

#### Crossref поля

| Колонка | Тип | Описание |
|---------|-----|----------|
|`crossref*title`| string | Название из Crossref |
|`crossref*doc*type`| string | Тип документа из Crossref |
|`crossref*subject`| string | Предмет из Crossref |
|`crossref*error`| string | Ошибка Crossref |

#### Semantic Scholar поля

| Колонка | Тип | Описание |
|---------|-----|----------|
|`semantic_scholar_pmid`| string | PMID из Semantic Scholar |
|`semantic_scholar_doi`| string | DOI из Semantic Scholar |
|`semantic_scholar_semantic_scholar_id`| string | Semantic Scholar ID |
|`semantic_scholar_publication_types`| string | Типы публикаций из Semantic Scholar |
|`semantic_scholar_venue`| string | Место публикации из Semantic Scholar |
|`semantic_scholar_external_ids`| string | Внешние ID из Semantic Scholar (JSON) |
|`semantic_scholar_error`| string | Ошибка Semantic Scholar |

#### PubMed поля

| Колонка | Тип | Описание |
|---------|-----|----------|
|`pubmed*pmid`| string | PubMed ID |
|`pubmed*doi`| string | DOI из PubMed |
|`pubmed*article*title`| string | Название статьи из PubMed |
|`pubmed*abstract`| string | Аннотация из PubMed |
|`pubmed*journal*title`| string | Название журнала из PubMed |
|`pubmed*volume`| string | Том из PubMed |
|`pubmed*issue`| string | Выпуск из PubMed |
|`pubmed*start*page`| string | Начальная страница из PubMed |
|`pubmed*end*page`| string | Конечная страница из PubMed |
|`pubmed*publication*type`| string | Тип публикации из PubMed |
|`pubmed*mesh*descriptors`| string | MeSH дескрипторы из PubMed |
|`pubmed*mesh*qualifiers`| string | MeSH квалификаторы из PubMed |
|`pubmed*chemical*list`| string | Список химических веществ из PubMed |
|`pubmed*year*completed`| int | Год завершения из PubMed |
|`pubmed*month*completed`| int | Месяц завершения из PubMed |
|`pubmed*day*completed`| int | День завершения из PubMed |
|`pubmed*year*revised`| int | Год пересмотра из PubMed |
|`pubmed*month*revised`| int | Месяц пересмотра из PubMed |
|`pubmed*day*revised`| int | День пересмотра из PubMed |
|`pubmed*issn`| string | ISSN из PubMed |
|`pubmed*error`| string | Ошибка PubMed |

#### ChEMBL дополнительные поля

| Колонка | Тип | Описание |
|---------|-----|----------|
|`chembl*title`| string | Название из ChEMBL |
|`chembl*doi`| string | DOI из ChEMBL |
|`chembl*pubmed*id`| string | PubMed ID из ChEMBL |
|`chembl*journal`| string | Журнал из ChEMBL |
|`chembl*year`| int | Год из ChEMBL |
|`chembl*volume`| string | Том из ChEMBL |
|`chembl*issue`| string | Выпуск из ChEMBL |

**Размер**: ~10 записей (в тестовом наборе)

### 2. documents*YYYYMMDD*qc.csv

**Описание**: Метрики контроля качества обработки документов.

| Колонка | Тип | Описание |
|---------|-----|----------|
|`metric`| string | Название метрики QC |
|`value`| int | Значение метрики QC |

#### Примеры метрик

-`row*count`: Общее количество обработанных записей

- `enabled*sources`: Количество включенных источников данных

- `chembl*records`: Количество записей из ChEMBL

- `crossref*records`: Количество записей из Crossref

- `openalex*records`: Количество записей из OpenAlex

- `pubmed*records`: Количество записей из PubMed

- `semantic*scholar*records`: Количество записей из Semantic Scholar

## Схемы данных (src/library/schemas/)

### 1. DocumentInputSchema

**Файл**: `document*input*schema.py`
**Описание**: Pandera схема для входных данных документов из ChEMBL CSV файлов.

### 2. DocumentOutputSchema

**Файл**:`document*output*schema.py`
**Описание**: Pandera схема для обогащенных данных документов из всех
источников.

### 3. RawBioactivitySchema

**Файл**:`input*schema.py`
**Описание**: Pandera схема для сырых данных биоактивности из API.

### 4. NormalizedBioactivitySchema

**Файл**:`output*schema.py`
**Описание**: Pandera схема для нормализованных данных биоактивности.

### 5. DocumentQCSchema

**Файл**:`document*output*schema.py`
**Описание**: Pandera схема для метрик QC документов.

## API источники данных

### 1. ChEMBL API

**Базовый URL**: [https://www.ebi.ac.uk/chembl/api/data](https://www.ebi.ac.uk/chembl/api/data)

**Документация**: [https://chembl.gitbook.io/chembl-interface-documentation/web-services/chembl-data-web-services](https://chembl.gitbook.io/chembl-interface-documentation/web-services/chembl-data-web-services)

**Лимиты**: 20 запросов/сек, без аутентификации

**Эндпоинт**: `/document/{doc*id}`#### Извлекаемые поля ChEMBL

-`document*chembl*id`- Идентификатор документа в ChEMBL

-`title`- Название публикации

-`doi`- Digital Object Identifier

-`pubmed*id`- PubMed идентификатор

-`doc*type`- Тип документа (по умолчанию: "PUBLICATION")

-`journal`- Название журнала

-`year`- Год публикации

-`volume`- Том журнала

-`issue`- Номер выпуска

-`abstract`- Аннотация (если доступна)

### 2. Crossref API

**Базовый URL**: [https://api.crossref.org/works](https://api.crossref.org/works)

**Документация**: [https://github.com/CrossRef/rest-api-doc](https://github.com/CrossRef/rest-api-doc)

**Лимиты**: 50 запросов/сек (free), 100 запросов/сек (plus token)

**Эндпоинты**:

- `/{doi}`- поиск по DOI

-`?filter=pmid:{pmid}`- поиск по PubMed ID

-`?query.bibliographic={doi}`- fallback поиск

#### Извлекаемые поля Crossref

-`crossref*title`- Название из Crossref

-`crossref*doc*type`- Тип документа (journal-article, book-chapter, etc.)

-`crossref*subject`- Предметная область (автоматически определяется по журналу)

#### Особенности Crossref

- Автоматическое определение предметной области по названию журнала

- Fallback поиск при отсутствии точного совпадения по DOI

- Обработка пустых списков subjects

### 3. OpenAlex API

**Базовый URL**:`[https://api.openalex.org/works`](<https://api.openalex.org/works`>)

**Документация**: [https://docs.openalex.org/](https://docs.openalex.org/)

**Лимиты**: 10 запросов/сек, без аутентификации

**Эндпоинты**:

- [https://doi.org/{doi}](https://doi.org/{doi}) - прямой поиск по DOI

- `?filter=doi:{doi}`- фильтр по DOI

-`?filter=pmid:{pmid}`- поиск по PubMed ID

#### Извлекаемые поля OpenAlex

-`openalex*doi*key`- DOI для соединения данных

-`openalex*title`- Название из OpenAlex

-`openalex*doc*type`- Тип документа

-`openalex*type*crossref`- Crossref тип документа

-`openalex*publication*year`- Год публикации (извлекается из разных полей)

#### Особенности OpenAlex

- Специальная обработка rate limiting (429 ошибок)

- Извлечение года публикации из поля`published-print`

- Fallback на`display*name`для названия

### 4. PubMed API (E-utilities)

**Базовый URL**: [https://eutils.ncbi.nlm.nih.gov/entrez/eutils/](https://eutils.ncbi.nlm.nih.gov/entrez/eutils/)

**Документация**: [https://www.ncbi.nlm.nih.gov/books/NBK25501/](https://www.ncbi.nlm.nih.gov/books/NBK25501/)

**Лимиты**: 3 запроса/сек (без ключа), 10 запросов/сек (с API ключом)

**Эндпоинты**:

- `esummary.fcgi`- получение метаданных

-`efetch.fcgi`- получение полного контента

#### Извлекаемые поля PubMed

-`pubmed*pmid`- PubMed идентификатор

-`pubmed*doi`- DOI из PubMed

-`pubmed*article*title`- Название статьи

-`pubmed*abstract`- Полный текст аннотации

-`pubmed*journal*title`- Название журнала

-`pubmed*volume`- Том журнала

-`pubmed*issue`- Номер выпуска

-`pubmed*start*page`/`pubmed*end*page`- Страницы

-`pubmed*publication*type`- Тип публикации

-`pubmed*mesh*descriptors`- MeSH дескрипторы

-`pubmed*mesh*qualifiers`- MeSH квалификаторы

-`pubmed*chemical*list`- Список химических веществ

-`pubmed*year*completed`/`pubmed*month*completed`/`pubmed*day*completed`- Даты завершения

-`pubmed*year*revised`/`pubmed*month*revised`/`pubmed*day*revised`- Даты пересмотра

-`pubmed*issn`- ISSN журнала

#### Особенности PubMed

- Двухэтапный процесс: esummary + efetch для получения полной информации

- XML парсинг для извлечения DOI и аннотации

- Обработка множественных авторов

- Извлечение дат из поля`history`

### 5. Semantic Scholar API

**Базовый
URL**:`[https://api.semanticscholar.org/graph/v1/paper`](<https://api.semanticscholar.org/graph/v1/paper`>)

**Документация**: [https://api.semanticscholar.org/](https://api.semanticscholar.org/)

**Лимиты**: 100 запросов/сек, без аутентификации

**Эндпоинты**:

- `PMID:{pmid}`- поиск по PubMed ID

-`batch`- массовые запросы

#### Извлекаемые поля Semantic Scholar

-`semantic*scholar*pmid`- PubMed ID из Semantic Scholar

-`semantic*scholar*doi`- DOI из Semantic Scholar

-`semantic*scholar*semantic*scholar*id`- Уникальный ID в Semantic Scholar

-`semantic*scholar*publication*types`- Типы публикаций

-`semantic*scholar*venue`- Место публикации

-`semantic*scholar*external*ids`- Внешние идентификаторы (JSON)

#### Особенности Semantic Scholar

- Поддержка batch запросов для множественных PMID

- Извлечение внешних ID в JSON формате

- Fallback стратегия для обработки ошибок

## Стратегии обработки ошибок

### Rate Limiting (429 ошибки)

- Автоматическое ожидание с использованием заголовка`Retry-After`

- Экспоненциальный backoff с множителем 2.0-5.0

- Специальная обработка для OpenAlex и PubMed

### Fallback стратегии

- **Crossref**: Поиск по библиографическому запросу при отсутствии точного DOI

- **OpenAlex**: Фильтр по DOI при неудаче прямого запроса

- **PubMed**: Fallback данные при недоступности API

- **Semantic Scholar**: Адаптивная стратегия с увеличением задержек

### Обработка временных ошибок

- Максимум 3-15 повторных попыток в зависимости от API

- Timeout: 30-60 секунд

- Логирование всех ошибок для мониторинга

## Процесс обработки данных

1. **Извлечение**: Данные загружаются из CSV файлов в папке`data/input/`2. **Обогащение**: Данные
дополняются информацией из внешних API в следующем
порядке:

   - ChEMBL (базовые данные)

   - Crossref (метаданные публикаций)

   - OpenAlex (дополнительные метаданные)

   - PubMed (медицинские аннотации и MeSH)

   - Semantic Scholar (академические метаданные)

3. **Трансформация**: Объединение и нормализация данных из всех источников
4. **Загрузка**: Результаты сохраняются в папку`data/output/`с датой в
названии файла
5. **QC**: Генерируются метрики контроля качества и корреляционный анализ

## Расширенный корреляционный анализ

### Типы корреляций

#### Числовые корреляции

- **Корреляция Пирсона**: Линейная корреляция между числовыми переменными

- **Корреляция Спирмена**: Ранговая корреляция для нелинейных зависимостей

- **Ковариационная матрица**: Мера совместной изменчивости переменных

#### Категориальные корреляции

- **Cramér's V**: Мера ассоциации между категориальными переменными

- **Таблицы сопряженности**: Детальный анализ распределения категорий

- **Chi-squared статистики**: Тесты значимости ассоциаций

#### Смешанные корреляции

- **Eta-squared**: Связь между числовыми и категориальными переменными

- **Point-biserial correlation**: Корреляция между числовой и бинарной переменными

#### Кросс-корреляции

- **Лаговые корреляции**: Анализ временных зависимостей

- **Скользящие корреляции**: Динамика корреляций во времени

### Автоматические инсайты

- Обнаружение сильных корреляций (|r| > 0.8)

- Выявление умеренных корреляций (|r| > 0.7)

- Рекомендации по обработке мультиколлинеарности

- Статистическая значимость корреляций

### Выходные файлы корреляционного анализа

- **Корреляционная матрица**: Все парные корреляции между переменными

- **Инсайты**: Автоматически сгенерированные выводы о значимых корреляциях

- **Визуализации**: Heatmaps и scatter plots для визуального анализа

- **Статистики**: p-values, confidence intervals, effect sizes

## Типы данных

- **string**: Текстовые данные

- **int**: Целочисленные значения

- **float**: Числа с плавающей точкой

- **bool**: Логические значения (True/False)

- **pd.Timestamp**: Временные метки

## Конфигурация API

### Глобальные настройки

- **Timeout**: 30 секунд (60 секунд для PubMed)

- **Retries**: 3-15 попыток в зависимости от API

- **Backoff multiplier**: 2.0-5.0 для экспоненциальной задержки

- **User-Agent**:`bioactivity-data-acquisition/0.1.0`

### Настройки по API

#### ChEMBL

- Лимит: 20 запросов/сек

- Аутентификация: Bearer token (опционально)

- Headers:`Accept: application/json`

#### Crossref

- Лимит: 50 запросов/сек (free), 100 запросов/сек (plus)

- Аутентификация: Crossref-Plus-API-Token

- Поддержка пагинации: cursor-based

#### OpenAlex

- Лимит: 10 запросов/сек

- Без аутентификации

- Специальная обработка rate limiting

#### PubMed

- Лимит: 3 запроса/сек (без ключа), 10 запросов/сек (с ключом)

- Аутентификация: API key через параметр`api*key`

- Использует E-utilities (esummary + efetch)

#### Semantic Scholar

- Лимит: 100 запросов/сек

- Без аутентификации

- Поддержка batch запросов

## Мониторинг и диагностика

### Скрипты проверки API

-`check*api*limits.py`- Полная проверка всех API

-`check*specific*limits.py`- Детальная информация о лимитах

-`quick*api*check.py`- Быстрая проверка конкретного API

-`api*health*check.py`- Мониторинг состояния API

### Метрики мониторинга

- Время ответа API

- Статус коды ответов

- Количество ошибок 429 (rate limiting)

- Успешность запросов по источникам

- Использование fallback стратегий

### Логирование

- Структурированные логи для каждого API клиента

- Отслеживание rate limiting событий

- Мониторинг fallback использований

- Детальные ошибки для диагностики

## Ограничения и валидация

- Все схемы используют Pandera для валидации типов данных

- Обязательные поля не могут быть NULL

- Опциональные поля помечены как`nullable=True`

- Некоторые поля имеют ограничения на значения (например,`activity*value > 0`)

- Источники данных ограничены предопределенным списком: `["chembl", "crossref", "openalex", "pubmed", "semantic*scholar"]`

- Автоматическая обработка ошибок API с fallback стратегиями

- Rate limiting с экспоненциальным backoff

- Валидация ответов API на соответствие ожидаемой структуре

## Группировка полей в выходных данных

### Обзор группировки

Выходные данные организованы в логические группы полей для улучшения анализа и
сравнения данных между источниками. Все поля сгруппированы по типу информации и
источнику данных.

### Полная структура группировки полей

```

Основные поля документа
├── document*chembl*id, doi, pubmed*id, classification, etc.
Группа полей PMID
├── chembl*pubmed*id, crossref*pmid, openalex*pmid, pubmed*pmid,
semantic*scholar*pmid
Группа полей названий статей
├── chembl*title, crossref*title, openalex*title, pubmed*article*title,
semantic*scholar*title
Группа полей аннотаций
├── chembl*abstract, crossref*abstract, openalex*abstract, pubmed*abstract,
semantic*scholar*abstract
Группа полей авторов
├── chembl*authors, crossref*authors, openalex*authors, pubmed*authors,
semantic*scholar*authors
Поля из отдельных источников
├── ChEMBL, Crossref, OpenAlex, PubMed, Semantic Scholar
Группа полей DOI
├── chembl*doi, openalex*doi*key, pubmed*doi, semantic*scholar*doi
Группа полей типов публикации
├── doc*type, crossref*doc*type, openalex*doc*type, openalex*type*crossref,
pubmed*publication*type, semantic*scholar*publication*types
Группа полей ISSN
├── chembl*issn, crossref*issn, openalex*issn, pubmed*issn,
semantic*scholar*issn
Группа полей названий журналов
├── chembl*journal, crossref*journal, openalex*journal, pubmed*journal*title,
semantic*scholar*venue
Группа полей годов издания
├── chembl*year, crossref*year, openalex*year, pubmed*year
Группа полей томов
├── chembl*volume, crossref*volume, openalex*volume, pubmed*volume
Группа полей выпусков
├── chembl*issue, crossref*issue, openalex*issue, pubmed*issue
Группа полей первых страниц
├── crossref*first*page, openalex*first*page, pubmed*start*page
Группа полей последних страниц
├── crossref*last*page, openalex*last*page, pubmed*end*page
Группа полей ошибок
├── chembl*error, crossref*error, openalex*error, pubmed*error,
semantic*scholar*error

```

### Детальное описание групп полей

#### 1. Группа полей PMID

Поля содержат PubMed идентификаторы из всех источников:

- **`chembl*pmid`**- PMID из ChEMBL

-**`crossref*pmid`**- PMID из Crossref

-**`openalex*pmid`**- PMID из OpenAlex

-**`pubmed*pmid`**- PMID из PubMed

-**`semantic*scholar*pmid`**- PMID из Semantic Scholar
**Использование**: Связывание записей между источниками, дедупликация,
валидация.

#### 2. Группа полей названий статей

Поля содержат названия публикаций из всех источников:

- **`chembl*title`**- Название статьи из ChEMBL

-**`crossref*title`**- Название статьи из Crossref

-**`openalex*title`**- Название статьи из OpenAlex

-**`pubmed*article*title`**- Название статьи из PubMed

-**`semantic*scholar*title`**- Название статьи из Semantic Scholar
**Использование**: Сравнение названий между источниками, анализ полноты данных.

#### 3. Группа полей аннотаций

Поля содержат аннотации (abstracts) из всех источников:

- **`chembl*abstract`**- Аннотация из ChEMBL

-**`crossref*abstract`**- Аннотация из Crossref

-**`openalex*abstract`**- Аннотация из OpenAlex

-**`pubmed*abstract`**- Аннотация из PubMed

-**`semantic*scholar*abstract`**- Аннотация из Semantic Scholar
**Использование**: Текстовый анализ, сравнение содержания, создание объединенных
аннотаций.

#### 4. Группа полей авторов

Поля содержат авторов публикаций из всех источников:

- **`chembl*authors`**- Авторы из ChEMBL

-**`crossref*authors`**- Авторы из Crossref

-**`openalex*authors`**- Авторы из OpenAlex

-**`pubmed*authors`**- Авторы из PubMed

-**`semantic*scholar*authors`**- Авторы из Semantic Scholar
**Использование**: Анализ авторства, поиск по авторам, анализ научного
сотрудничества.

#### 5. Группа полей ISSN

Поля содержат ISSN (International Standard Serial Number) из всех источников:

- **`chembl*issn`**- ISSN из ChEMBL

-**`crossref*issn`**- ISSN из Crossref

-**`openalex*issn`**- ISSN из OpenAlex

-**`pubmed*issn`**- ISSN из PubMed

-**`semantic*scholar*issn`**- ISSN из Semantic Scholar
**Использование**: Идентификация журналов, валидация ISSN, связывание данных по
журналам.

#### 6. Группа полей названий журналов

Поля содержат названия журналов из всех источников:

- **`chembl*journal`**- Название журнала из ChEMBL

-**`crossref*journal`**- Название журнала из Crossref

-**`openalex*journal`**- Название журнала из OpenAlex

-**`pubmed*journal*title`**- Название журнала из PubMed

-**`semantic*scholar*venue`**- Название журнала из Semantic Scholar
**Использование**: Анализ публикаций по журналам, нормализация названий
журналов.

#### 7. Группа полей годов издания

Поля содержат годы публикации из всех источников:

- **`chembl*year`**- Год из ChEMBL

-**`crossref*year`**- Год из Crossref

-**`openalex*year`**- Год из OpenAlex

-**`pubmed*year`**- Год из PubMed
**Использование**: Временной анализ публикаций, валидация дат.

#### 8. Группа полей томов

Поля содержат номера томов из всех источников:

- **`chembl*volume`**- Том из ChEMBL

-**`crossref*volume`**- Том из Crossref

-**`openalex*volume`**- Том из OpenAlex

-**`pubmed*volume`**- Том из PubMed
**Использование**: Создание полных библиографических ссылок.

#### 9. Группа полей выпусков

Поля содержат номера выпусков из всех источников:

- **`chembl*issue`**- Номер выпуска из ChEMBL

-**`crossref*issue`**- Номер выпуска из Crossref

-**`openalex*issue`**- Номер выпуска из OpenAlex

-**`pubmed*issue`**- Номер выпуска из PubMed
**Использование**: Детализация библиографических ссылок.

#### 10. Группа полей первых страниц

Поля содержат номера первых страниц из всех источников:

- **`crossref*first*page`**- Первая страница из Crossref

-**`openalex*first*page`**- Первая страница из OpenAlex

-**`pubmed*start*page`**- Начальная страница из PubMed
**Использование**: Создание точных библиографических ссылок.

#### 11. Группа полей последних страниц

Поля содержат номера последних страниц из всех источников:

- **`crossref*last*page`**- Последняя страница из Crossref

-**`openalex*last*page`**- Последняя страница из OpenAlex

-**`pubmed*end*page`**- Конечная страница из PubMed
**Использование**: Определение диапазона страниц публикации.

#### 12. Группа полей ошибок

Поля содержат информацию об ошибках из всех источников:

- **`chembl*error`**- Ошибка из ChEMBL

-**`crossref*error`**- Ошибка из Crossref

-**`openalex*error`**- Ошибка из OpenAlex

-**`pubmed*error`**- Ошибка из PubMed

-**`semantic*scholar*error`**- Ошибка из Semantic Scholar
**Использование**: Мониторинг качества данных, диагностика проблем с API.

### Преимущества группировки полей

1. **Логическая организация**: Связанные поля сгруппированы вместе
2. **Упрощенный анализ**: Легко сравнивать данные одного типа между источниками
3. **Контроль качества**: Быстрое выявление расхождений в данных
4. **Улучшенная читаемость**: Понятная структура конфигурации
5. **Эффективная валидация**: Группировка для проверки целостности данных

### Примеры использования группировки

#### Анализ заполненности по группам

```

def analyze*field*groups(df):
    """Анализирует заполненность полей по группам."""
    groups = {
'PMID': ['chembl*pmid', 'crossref*pmid', 'openalex*pmid', 'pubmed*pmid',
'semantic*scholar*pmid'],
'Titles': ['chembl*title', 'crossref*title', 'openalex*title',
'pubmed*article*title', 'semantic*scholar*title'],
'Abstracts': ['chembl*abstract', 'crossref*abstract', 'openalex*abstract',
'pubmed*abstract', 'semantic*scholar*abstract'],
'Authors': ['chembl*authors', 'crossref*authors', 'openalex*authors',
'pubmed*authors', 'semantic*scholar*authors'],
'ISSN': ['chembl*issn', 'crossref*issn', 'openalex*issn', 'pubmed*issn',
'semantic*scholar*issn'],
'Journals': ['chembl*journal', 'crossref*journal', 'openalex*journal',
'pubmed*journal*title', 'semantic*scholar*venue'],
'Years': ['chembl*year', 'crossref*year', 'openalex*year', 'pubmed*year'],
'Volumes': ['chembl*volume', 'crossref*volume', 'openalex*volume',
'pubmed*volume'],
'Issues': ['chembl*issue', 'crossref*issue', 'openalex*issue', 'pubmed*issue'],
'First Pages': ['crossref*first*page', 'openalex*first*page',
'pubmed*start*page'],
'Last Pages': ['crossref*last*page', 'openalex*last*page', 'pubmed*end*page'],
'Errors': ['chembl*error', 'crossref*error', 'openalex*error', 'pubmed*error',
'semantic*scholar*error']
    }
    for group*name, fields in groups.items():
        total*records = len(df)
        filled*records = df[fields].notna().any(axis=1).sum()
print(f"{group*name}: {filled*records}/{total*records}
({filled*records/total*records*100:.1f}%)")

```

#### Создание полных библиографических ссылок

```

def create*complete*citation(record):
    """Создает полную библиографическую ссылку из сгруппированных полей."""

## Используем приоритет источников: PubMed > ChEMBL > Crossref > OpenAlex > Semantic Scholar

## Название журнала

    journal = (record['pubmed*journal*title'] or
               record['chembl*journal'] or
               record['crossref*journal'] or
               record['openalex*journal'] or
               record['semantic*scholar*venue'])

## Год

    year = (record['pubmed*year'] or
            record['chembl*year'] or
            record['crossref*year'] or
            record['openalex*year'])

## Том

    volume = (record['pubmed*volume'] or
              record['chembl*volume'] or
              record['crossref*volume'] or
              record['openalex*volume'])

## Выпуск

    issue = (record['pubmed*issue'] or
             record['chembl*issue'] or
             record['crossref*issue'] or
             record['openalex*issue'])

## Страницы

    first*page = (record['pubmed*start*page'] or
                  record['crossref*first*page'] or
                  record['openalex*first*page'])
    last*page = (record['pubmed*end*page'] or
                 record['crossref*last*page'] or
                 record['openalex*last*page'])

## Формируем цитату

    parts = []
    if journal: parts.append(journal)
    if year: parts.append(f"({year})")
    if volume: parts.append(f"Vol. {volume}")
    if issue: parts.append(f"Issue {issue}")
    if first*page:
        if last*page and last*page != first*page:
            parts.append(f"pp. {first*page}-{last*page}")
        else:
            parts.append(f"p. {first*page}")
    return ", ".join(parts)

```

#### Мониторинг качества данных

```

def monitor*data*quality(df):
    """Мониторинг качества данных по группам полей."""
error*fields = ['chembl*error', 'crossref*error', 'openalex*error',
'pubmed*error', 'semantic*scholar*error']
    total*records = len(df)
    records*with*errors = df[error*fields].notna().any(axis=1).sum()
    error*rate = (records*with*errors / total*records) *100
    print(f"Общая статистика:")
    print(f"  Всего записей: {total*records}")
    print(f"  Записей с ошибками: {records*with*errors}")
    print(f"  Процент ошибок: {error*rate:.2f}%")

## Детальная статистика по источникам

    for field in error*fields:
        source*errors = df[field].notna().sum()
        source*rate = (source*errors / total*records)* 100
        print(f"  {field}: {source*errors} ошибок ({source*rate:.2f}%)")

```

### Конфигурация группировки

Группировка полей настроена в файле`configs/config*documents*full.yaml`в
секции`determinism.column*order`. Порядок полей определяет структуру выходных
данных и обеспечивает логичную организацию информации.

Все изменения касаются только порядка колонок в конфигурации, не затрагивая
логику обработки данных.
 
