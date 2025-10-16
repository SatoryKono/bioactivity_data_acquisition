# Структура входных и выходных данных

## Обзор

Проект `bioactivity_data_acquisition` обрабатывает данные о биоактивности из различных источников, включая ChEMBL, PubMed, Crossref, OpenAlex и Semantic Scholar. Данные проходят через ETL-процесс, который включает извлечение, трансформацию и загрузку с обогащением из внешних источников.

## Входные данные (data/input/)

### 1. documents.csv

**Описание**: Основной файл с документами из ChEMBL, содержащий метаданные публикаций.

| Колонка | Тип | Обязательная | Описание |
|---------|-----|--------------|----------|
| `document_chembl_id` | string | Да | Уникальный идентификатор документа в ChEMBL |
| `title` | string | Да | Название публикации |
| `doi` | string | Нет | Digital Object Identifier |
| `pubmed_id` | string | Нет | PubMed идентификатор |
| `doc_type` | string | Нет | Тип документа |
| `journal` | string | Нет | Название журнала |
| `year` | int | Нет | Год публикации |
| `abstract` | string | Нет | Аннотация документа |
| `authors` | string | Нет | Авторы документа |
| `classification` | float | Нет | Классификация документа |
| `document_contains_external_links` | bool | Нет | Содержит ли документ внешние ссылки |
| `first_page` | int | Нет | Номер первой страницы |
| `is_experimental_doc` | bool | Нет | Является ли документ экспериментальным |
| `issue` | int | Нет | Номер выпуска журнала |
| `last_page` | float | Нет | Номер последней страницы |
| `month` | int | Нет | Месяц публикации |
| `volume` | float | Нет | Том журнала |
| `citation` | string | Нет | Форматированная литературная ссылка |

**Примечание**: Колонки с названиями журналов (содержащие 'journal' в названии) автоматически нормализуются для обеспечения единообразия.

**Размер**: ~15,677 записей

### 2. activity.csv

**Описание**: Данные о биоактивности соединений.

| Колонка | Тип | Обязательная | Описание |
|---------|-----|--------------|----------|
| `activity_chembl_id` | string | Да | Уникальный идентификатор активности |
| `assay_chembl_id` | string | Да | Идентификатор эксперимента |
| `compound_key` | string | Да | Ключ соединения |
| `compound_name` | string | Да | Название соединения |
| `document_chembl_id` | string | Да | Идентификатор документа |
| `molecule_chembl_id` | string | Да | Идентификатор молекулы |
| `standard_type` | string | Да | Тип стандартного значения |
| `standard_value` | float | Да | Стандартное значение активности |
| `target_chembl_id` | string | Да | Идентификатор мишени |
| `original_activity_exact` | float | Да | Точное значение активности |
| `pA_value` | float | Да | pA значение |
| `bao_endpoint` | string | Да | BAO endpoint |
| `bao_format` | string | Да | BAO формат |
| `IUPHAR_class` | string | Да | IUPHAR класс |
| `IUPHAR_subclass` | string | Да | IUPHAR подкласс |
| `gene_index` | string | Да | Индекс гена |
| `taxon_index` | int | Да | Индекс таксона |

**Размер**: ~145,952 записей

### 3. assay.csv

**Описание**: Данные об экспериментах (ассаях).

| Колонка | Тип | Обязательная | Описание |
|---------|-----|--------------|----------|
| `assay_chembl_id` | string | Да | Уникальный идентификатор эксперимента |
| `target_chembl_id` | string | Да | Идентификатор мишени |
| `target_name` | string | Да | Название мишени |
| `target_type` | string | Да | Тип мишени |
| `document_chembl_id` | string | Да | Идентификатор документа |
| `description` | string | Да | Описание эксперимента |
| `bao_format` | string | Да | BAO формат |
| `assay_cell_type` | string | Нет | Тип клетки |
| `assay_subcellular_fraction` | string | Нет | Субклеточная фракция |
| `assay_tissue` | string | Нет | Ткань |
| `substrate_name` | string | Нет | Название субстрата |
| `version` | int | Нет | Версия |
| `year` | int | Нет | Год |
| `month` | int | Нет | Месяц |

**Размер**: ~41,578 записей

### 4. target.csv

**Описание**: Данные о биологических мишенях.

| Колонка | Тип | Обязательная | Описание |
|---------|-----|--------------|----------|
| `target_chembl_id` | string | Да | Уникальный идентификатор мишени |
| `target_names` | string | Да | Названия мишени |
| `target_uniprot_id` | string | Да | UniProt идентификатор |
| `primaryAccession` | string | Да | Основной аксесс-номер |
| `organism` | string | Да | Организм |
| `isoform_ids` | string | Нет | Идентификаторы изоформ |
| `isoform_names` | string | Нет | Названия изоформ |
| `isoforms` | int | Нет | Количество изоформ |
| `pH_dependence` | string | Нет | pH зависимость |

**Размер**: ~1,962 записи

### 5. testitem.csv

**Описание**: Данные о тестируемых соединениях (молекулах).

| Колонка | Тип | Обязательная | Описание |
|---------|-----|--------------|----------|
| `molecule_chembl_id` | string | Да | Уникальный идентификатор молекулы |
| `all_names` | string | Да | Все названия соединения |
| `canonical_smiles` | string | Да | Каноническая SMILES структура |
| `chirality` | int | Да | Хиральность |
| `inchi_key_from_mol` | string | Да | InChI ключ из молекулы |
| `inchi_key_from_smiles` | string | Да | InChI ключ из SMILES |
| `is_radical` | bool | Да | Является ли радикалом |
| `molecule_type` | string | Да | Тип молекулы |
| `mw_freebase` | float | Да | Молекулярный вес свободного основания |
| `n_stereocenters` | int | Да | Количество стереоцентров |
| `nstereo` | int | Да | Количество стереоизомеров |
| `num_ro5_violations` | int | Да | Количество нарушений правила Липински |
| `salt_chembl_id` | string | Нет | Идентификатор соли |
| `standard_inchi_key` | string | Нет | Стандартный InChI ключ |
| `standard_inchi_skeleton` | string | Нет | Скелет стандартного InChI |
| `standard_inchi_stereo` | string | Нет | Стерео стандартного InChI |
| `mw_<100_or_>1000` | bool | Нет | Молекулярный вес <100 или >1000 |

**Размер**: ~41,385 записей

### 6. cell.csv

**Описание**: Идентификаторы клеточных линий.

| Колонка | Тип | Обязательная | Описание |
|---------|-----|--------------|----------|
| `cell_chembl_id` | string | Да | Уникальный идентификатор клеточной линии |

**Размер**: ~9,407 записей

### 7. tissue.csv

**Описание**: Идентификаторы тканей.

| Колонка | Тип | Обязательная | Описание |
|---------|-----|--------------|----------|
| `tissue_chembl_id` | string | Да | Уникальный идентификатор ткани |

**Размер**: ~62 записи

## Выходные данные (data/output/)

### 1. documents_YYYYMMDD.csv

**Описание**: Обогащенные данные документов с информацией из всех внешних источников.

#### Основные поля ChEMBL (оригинальные)

| Колонка | Тип | Описание |
|---------|-----|----------|
| `document_chembl_id` | string | ChEMBL идентификатор документа |
| `title` | string | Название документа |
| `doi` | string | Digital Object Identifier |
| `pubmed_id` | string | PubMed идентификатор |
| `doc_type` | string | Тип документа |
| `journal` | string | Название журнала |
| `year` | int | Год публикации |
| `abstract` | string | Аннотация |
| `authors` | string | Авторы |
| `classification` | float | Классификация |
| `document_contains_external_links` | bool | Содержит внешние ссылки |
| `first_page` | int | Первая страница |
| `is_experimental_doc` | bool | Экспериментальный документ |
| `issue` | int | Номер выпуска |
| `last_page` | float | Последняя страница |
| `month` | int | Месяц |
| `volume` | float | Том |

#### Поля обогащения

| Колонка | Тип | Описание |
|---------|-----|----------|
| `source` | string | Источник данных (chembl, crossref, openalex, pubmed, semantic_scholar) |

#### OpenAlex поля

| Колонка | Тип | Описание |
|---------|-----|----------|
| `openalex_doi_key` | string | DOI ключ для соединения |
| `openalex_title` | string | Название из OpenAlex |
| `openalex_doc_type` | string | Тип документа из OpenAlex |
| `openalex_type_crossref` | string | Crossref тип из OpenAlex |
| `openalex_publication_year` | int | Год публикации из OpenAlex |
| `openalex_error` | string | Ошибка OpenAlex |

#### Crossref поля

| Колонка | Тип | Описание |
|---------|-----|----------|
| `crossref_title` | string | Название из Crossref |
| `crossref_doc_type` | string | Тип документа из Crossref |
| `crossref_subject` | string | Предмет из Crossref |
| `crossref_error` | string | Ошибка Crossref |

#### Semantic Scholar поля

| Колонка | Тип | Описание |
|---------|-----|----------|
| `semantic_scholar_pmid` | string | PMID из Semantic Scholar |
| `semantic_scholar_doi` | string | DOI из Semantic Scholar |
| `semantic_scholar_semantic_scholar_id` | string | Semantic Scholar ID |
| `semantic_scholar_publication_types` | string | Типы публикаций из Semantic Scholar |
| `semantic_scholar_venue` | string | Место публикации из Semantic Scholar |
| `semantic_scholar_external_ids` | string | Внешние ID из Semantic Scholar (JSON) |
| `semantic_scholar_error` | string | Ошибка Semantic Scholar |

#### PubMed поля

| Колонка | Тип | Описание |
|---------|-----|----------|
| `pubmed_pmid` | string | PubMed ID |
| `pubmed_doi` | string | DOI из PubMed |
| `pubmed_article_title` | string | Название статьи из PubMed |
| `pubmed_abstract` | string | Аннотация из PubMed |
| `pubmed_journal_title` | string | Название журнала из PubMed |
| `pubmed_volume` | string | Том из PubMed |
| `pubmed_issue` | string | Выпуск из PubMed |
| `pubmed_start_page` | string | Начальная страница из PubMed |
| `pubmed_end_page` | string | Конечная страница из PubMed |
| `pubmed_publication_type` | string | Тип публикации из PubMed |
| `pubmed_mesh_descriptors` | string | MeSH дескрипторы из PubMed |
| `pubmed_mesh_qualifiers` | string | MeSH квалификаторы из PubMed |
| `pubmed_chemical_list` | string | Список химических веществ из PubMed |
| `pubmed_year_completed` | int | Год завершения из PubMed |
| `pubmed_month_completed` | int | Месяц завершения из PubMed |
| `pubmed_day_completed` | int | День завершения из PubMed |
| `pubmed_year_revised` | int | Год пересмотра из PubMed |
| `pubmed_month_revised` | int | Месяц пересмотра из PubMed |
| `pubmed_day_revised` | int | День пересмотра из PubMed |
| `pubmed_issn` | string | ISSN из PubMed |
| `pubmed_error` | string | Ошибка PubMed |

#### ChEMBL дополнительные поля

| Колонка | Тип | Описание |
|---------|-----|----------|
| `chembl_title` | string | Название из ChEMBL |
| `chembl_doi` | string | DOI из ChEMBL |
| `chembl_pubmed_id` | string | PubMed ID из ChEMBL |
| `chembl_journal` | string | Журнал из ChEMBL |
| `chembl_year` | int | Год из ChEMBL |
| `chembl_volume` | string | Том из ChEMBL |
| `chembl_issue` | string | Выпуск из ChEMBL |

**Размер**: ~10 записей (в тестовом наборе)

### 2. documents_YYYYMMDD_qc.csv

**Описание**: Метрики контроля качества обработки документов.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `metric` | string | Название метрики QC |
| `value` | int | Значение метрики QC |

#### Примеры метрик

- `row_count`: Общее количество обработанных записей
- `enabled_sources`: Количество включенных источников данных
- `chembl_records`: Количество записей из ChEMBL
- `crossref_records`: Количество записей из Crossref
- `openalex_records`: Количество записей из OpenAlex
- `pubmed_records`: Количество записей из PubMed
- `semantic_scholar_records`: Количество записей из Semantic Scholar

## Схемы данных (src/library/schemas/)

### 1. DocumentInputSchema

**Файл**: `document_input_schema.py`

**Описание**: Pandera схема для входных данных документов из ChEMBL CSV файлов.

### 2. DocumentOutputSchema

**Файл**: `document_output_schema.py`

**Описание**: Pandera схема для обогащенных данных документов из всех источников.

### 3. RawBioactivitySchema

**Файл**: `input_schema.py`

**Описание**: Pandera схема для сырых данных биоактивности из API.

### 4. NormalizedBioactivitySchema

**Файл**: `output_schema.py`

**Описание**: Pandera схема для нормализованных данных биоактивности.

### 5. DocumentQCSchema

**Файл**: `document_output_schema.py`

**Описание**: Pandera схема для метрик QC документов.

## API источники данных

### 1. ChEMBL API

**Базовый URL**: `https://www.ebi.ac.uk/chembl/api/data`

**Документация**: <https://chembl.gitbook.io/chembl-interface-documentation/web-services/chembl-data-web-services>

**Лимиты**: 20 запросов/сек, без аутентификации

**Эндпоинт**: `/document/{doc_id}`

#### Извлекаемые поля ChEMBL

- `document_chembl_id` - Идентификатор документа в ChEMBL
- `title` - Название публикации
- `doi` - Digital Object Identifier
- `pubmed_id` - PubMed идентификатор
- `doc_type` - Тип документа (по умолчанию: "PUBLICATION")
- `journal` - Название журнала
- `year` - Год публикации
- `volume` - Том журнала
- `issue` - Номер выпуска
- `abstract` - Аннотация (если доступна)

### 2. Crossref API

**Базовый URL**: `https://api.crossref.org/works`

**Документация**: <https://github.com/CrossRef/rest-api-doc>

**Лимиты**: 50 запросов/сек (free), 100 запросов/сек (plus token)

**Эндпоинты**:

- `/{doi}` - поиск по DOI
- `?filter=pmid:{pmid}` - поиск по PubMed ID
- `?query.bibliographic={doi}` - fallback поиск

#### Извлекаемые поля Crossref

- `crossref_title` - Название из Crossref
- `crossref_doc_type` - Тип документа (journal-article, book-chapter, etc.)
- `crossref_subject` - Предметная область (автоматически определяется по журналу)

#### Особенности Crossref

- Автоматическое определение предметной области по названию журнала
- Fallback поиск при отсутствии точного совпадения по DOI
- Обработка пустых списков subjects

### 3. OpenAlex API

**Базовый URL**: `https://api.openalex.org/works`

**Документация**: <https://docs.openalex.org/>

**Лимиты**: 10 запросов/сек, без аутентификации

**Эндпоинты**:

- `<https://doi.org/{doi}>` - прямой поиск по DOI
- `?filter=doi:{doi}` - фильтр по DOI
- `?filter=pmid:{pmid}` - поиск по PubMed ID

#### Извлекаемые поля OpenAlex

- `openalex_doi_key` - DOI для соединения данных
- `openalex_title` - Название из OpenAlex
- `openalex_doc_type` - Тип документа
- `openalex_type_crossref` - Crossref тип документа
- `openalex_publication_year` - Год публикации (извлекается из разных полей)

#### Особенности OpenAlex

- Специальная обработка rate limiting (429 ошибок)
- Извлечение года публикации из поля `published-print`
- Fallback на `display_name` для названия

### 4. PubMed API (E-utilities)

**Базовый URL**: `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/`

**Документация**: <https://www.ncbi.nlm.nih.gov/books/NBK25501/>

**Лимиты**: 3 запроса/сек (без ключа), 10 запросов/сек (с API ключом)

**Эндпоинты**:

- `esummary.fcgi` - получение метаданных
- `efetch.fcgi` - получение полного контента

#### Извлекаемые поля PubMed

- `pubmed_pmid` - PubMed идентификатор
- `pubmed_doi` - DOI из PubMed
- `pubmed_article_title` - Название статьи
- `pubmed_abstract` - Полный текст аннотации
- `pubmed_journal_title` - Название журнала
- `pubmed_volume` - Том журнала
- `pubmed_issue` - Номер выпуска
- `pubmed_start_page` / `pubmed_end_page` - Страницы
- `pubmed_publication_type` - Тип публикации
- `pubmed_mesh_descriptors` - MeSH дескрипторы
- `pubmed_mesh_qualifiers` - MeSH квалификаторы
- `pubmed_chemical_list` - Список химических веществ
- `pubmed_year_completed` / `pubmed_month_completed` / `pubmed_day_completed` - Даты завершения
- `pubmed_year_revised` / `pubmed_month_revised` / `pubmed_day_revised` - Даты пересмотра
- `pubmed_issn` - ISSN журнала

#### Особенности PubMed

- Двухэтапный процесс: esummary + efetch для получения полной информации
- XML парсинг для извлечения DOI и аннотации
- Обработка множественных авторов
- Извлечение дат из поля `history`

### 5. Semantic Scholar API

**Базовый URL**: `https://api.semanticscholar.org/graph/v1/paper`

**Документация**: <https://api.semanticscholar.org/>

**Лимиты**: 100 запросов/сек, без аутентификации

**Эндпоинты**:

- `PMID:{pmid}` - поиск по PubMed ID
- `batch` - массовые запросы

#### Извлекаемые поля Semantic Scholar

- `semantic_scholar_pmid` - PubMed ID из Semantic Scholar
- `semantic_scholar_doi` - DOI из Semantic Scholar
- `semantic_scholar_semantic_scholar_id` - Уникальный ID в Semantic Scholar
- `semantic_scholar_publication_types` - Типы публикаций
- `semantic_scholar_venue` - Место публикации
- `semantic_scholar_external_ids` - Внешние идентификаторы (JSON)

#### Особенности Semantic Scholar

- Поддержка batch запросов для множественных PMID
- Извлечение внешних ID в JSON формате
- Fallback стратегия для обработки ошибок

## Стратегии обработки ошибок

### Rate Limiting (429 ошибки)

- Автоматическое ожидание с использованием заголовка `Retry-After`
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

1. **Извлечение**: Данные загружаются из CSV файлов в папке `data/input/`
2. **Обогащение**: Данные дополняются информацией из внешних API в следующем порядке:
   - ChEMBL (базовые данные)
   - Crossref (метаданные публикаций)
   - OpenAlex (дополнительные метаданные)
   - PubMed (медицинские аннотации и MeSH)
   - Semantic Scholar (академические метаданные)
3. **Трансформация**: Объединение и нормализация данных из всех источников
4. **Загрузка**: Результаты сохраняются в папку `data/output/` с датой в названии файла
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
- **User-Agent**: `bioactivity-data-acquisition/0.1.0`

### Настройки по API

#### ChEMBL

- Лимит: 20 запросов/сек
- Аутентификация: Bearer token (опционально)
- Headers: `Accept: application/json`

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
- Аутентификация: API key через параметр `api_key`
- Использует E-utilities (esummary + efetch)

#### Semantic Scholar

- Лимит: 100 запросов/сек
- Без аутентификации
- Поддержка batch запросов

## Мониторинг и диагностика

### Скрипты проверки API

- `check_api_limits.py` - Полная проверка всех API
- `check_specific_limits.py` - Детальная информация о лимитах
- `quick_api_check.py` - Быстрая проверка конкретного API
- `api_health_check.py` - Мониторинг состояния API

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
- Опциональные поля помечены как `nullable=True`
- Некоторые поля имеют ограничения на значения (например, `activity_value > 0`)
- Источники данных ограничены предопределенным списком: `["chembl", "crossref", "openalex", "pubmed", "semantic_scholar"]`
- Автоматическая обработка ошибок API с fallback стратегиями
- Rate limiting с экспоненциальным backoff
- Валидация ответов API на соответствие ожидаемой структуре

## Группировка полей в выходных данных

### Обзор группировки

Выходные данные организованы в логические группы полей для улучшения анализа и сравнения данных между источниками. Все поля сгруппированы по типу информации и источнику данных.

### Полная структура группировки полей

```text
Основные поля документа
├── document_chembl_id, doi, pubmed_id, classification, etc.
Группа полей PMID
├── chembl_pubmed_id, crossref_pmid, openalex_pmid, pubmed_pmid, semantic_scholar_pmid
Группа полей названий статей
├── chembl_title, crossref_title, openalex_title, pubmed_article_title, semantic_scholar_title
Группа полей аннотаций
├── chembl_abstract, crossref_abstract, openalex_abstract, pubmed_abstract, semantic_scholar_abstract
Группа полей авторов
├── chembl_authors, crossref_authors, openalex_authors, pubmed_authors, semantic_scholar_authors
Поля из отдельных источников
├── ChEMBL, Crossref, OpenAlex, PubMed, Semantic Scholar
Группа полей DOI
├── chembl_doi, openalex_doi_key, pubmed_doi, semantic_scholar_doi
Группа полей типов публикации
├── doc_type, crossref_doc_type, openalex_doc_type, openalex_type_crossref, pubmed_publication_type, semantic_scholar_publication_types
Группа полей ISSN
├── chembl_issn, crossref_issn, openalex_issn, pubmed_issn, semantic_scholar_issn
Группа полей названий журналов
├── chembl_journal, crossref_journal, openalex_journal, pubmed_journal_title, semantic_scholar_venue
Группа полей годов издания
├── chembl_year, crossref_year, openalex_year, pubmed_year
Группа полей томов
├── chembl_volume, crossref_volume, openalex_volume, pubmed_volume
Группа полей выпусков
├── chembl_issue, crossref_issue, openalex_issue, pubmed_issue
Группа полей первых страниц
├── crossref_first_page, openalex_first_page, pubmed_start_page
Группа полей последних страниц
├── crossref_last_page, openalex_last_page, pubmed_end_page
Группа полей ошибок
├── chembl_error, crossref_error, openalex_error, pubmed_error, semantic_scholar_error
```

### Детальное описание групп полей

#### 1. Группа полей PMID

Поля содержат PubMed идентификаторы из всех источников:

- **`chembl_pmid`** - PMID из ChEMBL
- **`crossref_pmid`** - PMID из Crossref  
- **`openalex_pmid`** - PMID из OpenAlex
- **`pubmed_pmid`** - PMID из PubMed
- **`semantic_scholar_pmid`** - PMID из Semantic Scholar

**Использование**: Связывание записей между источниками, дедупликация, валидация.

#### 2. Группа полей названий статей

Поля содержат названия публикаций из всех источников:

- **`chembl_title`** - Название статьи из ChEMBL
- **`crossref_title`** - Название статьи из Crossref
- **`openalex_title`** - Название статьи из OpenAlex
- **`pubmed_article_title`** - Название статьи из PubMed
- **`semantic_scholar_title`** - Название статьи из Semantic Scholar

**Использование**: Сравнение названий между источниками, анализ полноты данных.

#### 3. Группа полей аннотаций

Поля содержат аннотации (abstracts) из всех источников:

- **`chembl_abstract`** - Аннотация из ChEMBL
- **`crossref_abstract`** - Аннотация из Crossref
- **`openalex_abstract`** - Аннотация из OpenAlex
- **`pubmed_abstract`** - Аннотация из PubMed
- **`semantic_scholar_abstract`** - Аннотация из Semantic Scholar

**Использование**: Текстовый анализ, сравнение содержания, создание объединенных аннотаций.

#### 4. Группа полей авторов

Поля содержат авторов публикаций из всех источников:

- **`chembl_authors`** - Авторы из ChEMBL
- **`crossref_authors`** - Авторы из Crossref
- **`openalex_authors`** - Авторы из OpenAlex
- **`pubmed_authors`** - Авторы из PubMed
- **`semantic_scholar_authors`** - Авторы из Semantic Scholar

**Использование**: Анализ авторства, поиск по авторам, анализ научного сотрудничества.

#### 5. Группа полей ISSN

Поля содержат ISSN (International Standard Serial Number) из всех источников:

- **`chembl_issn`** - ISSN из ChEMBL
- **`crossref_issn`** - ISSN из Crossref
- **`openalex_issn`** - ISSN из OpenAlex
- **`pubmed_issn`** - ISSN из PubMed
- **`semantic_scholar_issn`** - ISSN из Semantic Scholar

**Использование**: Идентификация журналов, валидация ISSN, связывание данных по журналам.

#### 6. Группа полей названий журналов

Поля содержат названия журналов из всех источников:

- **`chembl_journal`** - Название журнала из ChEMBL
- **`crossref_journal`** - Название журнала из Crossref
- **`openalex_journal`** - Название журнала из OpenAlex
- **`pubmed_journal_title`** - Название журнала из PubMed
- **`semantic_scholar_venue`** - Название журнала из Semantic Scholar

**Использование**: Анализ публикаций по журналам, нормализация названий журналов.

#### 7. Группа полей годов издания

Поля содержат годы публикации из всех источников:

- **`chembl_year`** - Год из ChEMBL
- **`crossref_year`** - Год из Crossref
- **`openalex_year`** - Год из OpenAlex
- **`pubmed_year`** - Год из PubMed

**Использование**: Временной анализ публикаций, валидация дат.

#### 8. Группа полей томов

Поля содержат номера томов из всех источников:

- **`chembl_volume`** - Том из ChEMBL
- **`crossref_volume`** - Том из Crossref
- **`openalex_volume`** - Том из OpenAlex
- **`pubmed_volume`** - Том из PubMed

**Использование**: Создание полных библиографических ссылок.

#### 9. Группа полей выпусков

Поля содержат номера выпусков из всех источников:

- **`chembl_issue`** - Номер выпуска из ChEMBL
- **`crossref_issue`** - Номер выпуска из Crossref
- **`openalex_issue`** - Номер выпуска из OpenAlex
- **`pubmed_issue`** - Номер выпуска из PubMed

**Использование**: Детализация библиографических ссылок.

#### 10. Группа полей первых страниц

Поля содержат номера первых страниц из всех источников:

- **`crossref_first_page`** - Первая страница из Crossref
- **`openalex_first_page`** - Первая страница из OpenAlex
- **`pubmed_start_page`** - Начальная страница из PubMed

**Использование**: Создание точных библиографических ссылок.

#### 11. Группа полей последних страниц

Поля содержат номера последних страниц из всех источников:

- **`crossref_last_page`** - Последняя страница из Crossref
- **`openalex_last_page`** - Последняя страница из OpenAlex
- **`pubmed_end_page`** - Конечная страница из PubMed

**Использование**: Определение диапазона страниц публикации.

#### 12. Группа полей ошибок

Поля содержат информацию об ошибках из всех источников:

- **`chembl_error`** - Ошибка из ChEMBL
- **`crossref_error`** - Ошибка из Crossref
- **`openalex_error`** - Ошибка из OpenAlex
- **`pubmed_error`** - Ошибка из PubMed
- **`semantic_scholar_error`** - Ошибка из Semantic Scholar

**Использование**: Мониторинг качества данных, диагностика проблем с API.

### Преимущества группировки полей

1. **Логическая организация**: Связанные поля сгруппированы вместе
2. **Упрощенный анализ**: Легко сравнивать данные одного типа между источниками
3. **Контроль качества**: Быстрое выявление расхождений в данных
4. **Улучшенная читаемость**: Понятная структура конфигурации
5. **Эффективная валидация**: Группировка для проверки целостности данных

### Примеры использования группировки

#### Анализ заполненности по группам

```python
def analyze_field_groups(df):
    """Анализирует заполненность полей по группам."""
    groups = {
        'PMID': ['chembl_pmid', 'crossref_pmid', 'openalex_pmid', 'pubmed_pmid', 'semantic_scholar_pmid'],
        'Titles': ['chembl_title', 'crossref_title', 'openalex_title', 'pubmed_article_title', 'semantic_scholar_title'],
        'Abstracts': ['chembl_abstract', 'crossref_abstract', 'openalex_abstract', 'pubmed_abstract', 'semantic_scholar_abstract'],
        'Authors': ['chembl_authors', 'crossref_authors', 'openalex_authors', 'pubmed_authors', 'semantic_scholar_authors'],
        'ISSN': ['chembl_issn', 'crossref_issn', 'openalex_issn', 'pubmed_issn', 'semantic_scholar_issn'],
        'Journals': ['chembl_journal', 'crossref_journal', 'openalex_journal', 'pubmed_journal_title', 'semantic_scholar_venue'],
        'Years': ['chembl_year', 'crossref_year', 'openalex_year', 'pubmed_year'],
        'Volumes': ['chembl_volume', 'crossref_volume', 'openalex_volume', 'pubmed_volume'],
        'Issues': ['chembl_issue', 'crossref_issue', 'openalex_issue', 'pubmed_issue'],
        'First Pages': ['crossref_first_page', 'openalex_first_page', 'pubmed_start_page'],
        'Last Pages': ['crossref_last_page', 'openalex_last_page', 'pubmed_end_page'],
        'Errors': ['chembl_error', 'crossref_error', 'openalex_error', 'pubmed_error', 'semantic_scholar_error']
    }
    for group_name, fields in groups.items():
        total_records = len(df)
        filled_records = df[fields].notna().any(axis=1).sum()
        print(f"{group_name}: {filled_records}/{total_records} ({filled_records/total_records*100:.1f}%)")
```

#### Создание полных библиографических ссылок

```python
def create_complete_citation(record):
    """Создает полную библиографическую ссылку из сгруппированных полей."""
    # Используем приоритет источников: PubMed > ChEMBL > Crossref > OpenAlex > Semantic Scholar
    # Название журнала
    journal = (record['pubmed_journal_title'] or 
               record['chembl_journal'] or 
               record['crossref_journal'] or 
               record['openalex_journal'] or 
               record['semantic_scholar_venue'])
    # Год
    year = (record['pubmed_year'] or 
            record['chembl_year'] or 
            record['crossref_year'] or 
            record['openalex_year'])
    # Том
    volume = (record['pubmed_volume'] or 
              record['chembl_volume'] or 
              record['crossref_volume'] or 
              record['openalex_volume'])
    # Выпуск
    issue = (record['pubmed_issue'] or 
             record['chembl_issue'] or 
             record['crossref_issue'] or 
             record['openalex_issue'])
    # Страницы
    first_page = (record['pubmed_start_page'] or 
                  record['crossref_first_page'] or 
                  record['openalex_first_page'])
    last_page = (record['pubmed_end_page'] or 
                 record['crossref_last_page'] or 
                 record['openalex_last_page'])
    # Формируем цитату
    parts = []
    if journal: parts.append(journal)
    if year: parts.append(f"({year})")
    if volume: parts.append(f"Vol. {volume}")
    if issue: parts.append(f"Issue {issue}")
    if first_page:
        if last_page and last_page != first_page:
            parts.append(f"pp. {first_page}-{last_page}")
        else:
            parts.append(f"p. {first_page}")
    return ", ".join(parts)
```

#### Мониторинг качества данных

```python
def monitor_data_quality(df):
    """Мониторинг качества данных по группам полей."""
    error_fields = ['chembl_error', 'crossref_error', 'openalex_error', 'pubmed_error', 'semantic_scholar_error']
    total_records = len(df)
    records_with_errors = df[error_fields].notna().any(axis=1).sum()
    error_rate = (records_with_errors / total_records) * 100
    print(f"Общая статистика:")
    print(f"  Всего записей: {total_records}")
    print(f"  Записей с ошибками: {records_with_errors}")
    print(f"  Процент ошибок: {error_rate:.2f}%")
    # Детальная статистика по источникам
    for field in error_fields:
        source_errors = df[field].notna().sum()
        source_rate = (source_errors / total_records) * 100
        print(f"  {field}: {source_errors} ошибок ({source_rate:.2f}%)")
```

### Конфигурация группировки

Группировка полей настроена в файле `configs/config_documents_full.yaml` в секции `determinism.column_order`. Порядок полей определяет структуру выходных данных и обеспечивает логичную организацию информации.

Все изменения касаются только порядка колонок в конфигурации, не затрагивая логику обработки данных.
 
