# Анализ причин отсутствия данных в пайплайне мишеней

## Обзор

Проведен анализ причин отсутствия данных в колонках пайплайна мишеней путем сравнения с референсным проектом `ChEMBL_data_acquisition6`. Выявлены ключевые проблемы в архитектуре и реализации адаптеров.

## Проблемные колонки

### 1. ChEMBL-специфичные поля
- `component_description` - отсутствует в текущей реализации
- `CHEMBL.PROTEIN_CLASSIFICATION.pref_name` - не извлекается из API
- `reaction_ec_numbers` - не обрабатывается корректно

### 2. UniProt поля
- `uniprot_id_primary` - не заполняется из-за проблем с маппингом
- `uniprot_ids_all` - не собираются все доступные ID
- `taxon_id` - не извлекается из UniProt данных
- `sequence_length` - отсутствует в текущей реализации
- `uniprot_version` - не добавляется в метаданные

### 3. IUPHAR поля
- `iuphar_target_id` - не заполняется из-за отсутствия маппинга
- `iuphar_name` - не извлекается из IUPHAR данных
- `iuphar_family_id` - не определяется семейство
- `iuphar_full_id_path` - не строится иерархия
- `iuphar_full_name_path` - не строится путь имен
- `iuphar_type` - не определяется тип
- `iuphar_class` - не определяется класс
- `iuphar_subclass` - не определяется подкласс
- `iuphar_chain` - не строится цепочка классификации
- `iuphar_gene_symbol` - не извлекается символ гена
- `iuphar_hgnc_id` - не извлекается HGNC ID
- `iuphar_hgnc_name` - не извлекается HGNC имя
- `iuphar_uniprot_id_primary` - не маппится UniProt ID
- `iuphar_uniprot_name` - не извлекается UniProt имя
- `iuphar_organism` - не извлекается организм
- `iuphar_taxon_id` - не извлекается таксономический ID
- `iuphar_description` - не извлекается описание
- `iuphar_abbreviation` - не извлекается аббревиатура

### 4. GtoPdb поля
- `gtop_target_id` - не заполняется
- `gtop_synonyms` - не извлекаются синонимы
- `gtop_natural_ligands_n` - не подсчитываются лиганды
- `gtop_interactions_n` - не подсчитываются взаимодействия
- `gtop_function_text_short` - не извлекается описание функции

### 5. Системные поля
- `unified_tax_id` - не создается унифицированный таксономический ID
- `protein_class_pred_L1` - не выполняется предсказание класса белка
- `protein_class_pred_L2` - не выполняется предсказание класса белка
- `protein_class_pred_L3` - не выполняется предсказание класса белка
- `protein_class_pred_evidence` - не собирается доказательство
- `protein_class_pred_rule_id` - не определяется правило
- `validation_errors` - не собираются ошибки валидации
- `extraction_errors` - не собираются ошибки извлечения
- `chembl_release` - не извлекается версия ChEMBL

## Основные причины проблем

### 1. Архитектурные различия

**Референсный проект (`ChEMBL_data_acquisition6`):**
- Использует специализированные библиотеки (`library.integration.*`)
- Имеет полноценные парсеры для каждого источника
- Реализует сложную логику маппинга между источниками
- Использует CSV-словари для IUPHAR классификации

**Текущий проект:**
- Использует упрощенные адаптеры
- Отсутствуют специализированные парсеры
- Неполная реализация маппинга между источниками
- Отсутствуют CSV-словари для IUPHAR

### 2. Проблемы в ChEMBL адаптере

```python
# Текущая реализация (src/library/target/chembl_adapter.py)
def _parse_target_data_with_batch_mapping(target_data, target_id, mapping_cfg, batch_mappings):
    # Упрощенная логика парсинга
    # Отсутствует извлечение component_description
    # Не обрабатываются protein_classifications
    # Не собираются reaction_ec_numbers
```

**Референсная реализация:**
```python
# e:\github\ChEMBL_data_acquisition6\library\pipelines\target\chembl_target.py
def _parse_target_record(data: dict[str, Any], mapping_cfg: UniprotMappingCfg) -> dict[str, Any]:
    # Полная логика парсинга с извлечением всех полей
    # Обработка компонентов, классификаций, реакций
    # Корректный маппинг к UniProt
```

### 3. Проблемы в UniProt адаптере

**Текущая реализация:**
- Отсутствует полноценный парсинг JSON ответов
- Не извлекаются все необходимые поля
- Не реализован маппинг к IUPHAR

**Референсная реализация:**
- Полноценные функции извлечения (`extract_names`, `extract_organism`, etc.)
- Обработка всех типов UniProt данных
- Интеграция с Guide-to-Pharmacology

### 4. Проблемы в IUPHAR адаптере

**Текущая реализация:**
- Отсутствуют CSV-словари
- Не реализована классификация
- Отсутствует маппинг к другим источникам

**Референсная реализация:**
- Полноценный `IUPHARData` класс
- `IUPHARClassifier` для классификации
- Загрузка CSV-словарей
- Сложная логика маппинга

### 5. Отсутствие интеграции между источниками

**Текущий проект:**
- Источники работают независимо
- Отсутствует унификация данных
- Нет приоритизации источников

**Референсный проект:**
- Сложная логика объединения данных
- Приоритизация источников (UniProt > IUPHAR > GtoPdb > ChEMBL)
- Унификация полей

## Рекомендации по исправлению

### 1. Немедленные исправления

1. **Добавить извлечение `component_description`** в ChEMBL адаптер:
```python
def _parse_target_data_with_batch_mapping(target_data, target_id, mapping_cfg, batch_mappings):
    # Добавить извлечение component_description из первого компонента
    components = target_data.get("target_components", [])
    if components and len(components) > 0:
        component_description = components[0].get("component_description", "")
    else:
        component_description = ""
```

2. **Исправить извлечение `CHEMBL.PROTEIN_CLASSIFICATION.pref_name`**:
```python
# Добавить обработку protein_classifications
protein_classifications = target_data.get("protein_classifications", [])
if protein_classifications:
    # Извлечь pref_name из классификаций
    pref_names = [cls.get("pref_name", "") for cls in protein_classifications]
    protein_class_pref_name = "|".join(filter(None, pref_names))
```

3. **Добавить извлечение `reaction_ec_numbers`**:
```python
# Обработать компоненты для поиска EC номеров
ec_numbers = []
for component in components:
    synonyms = component.get("target_component_synonyms", [])
    for synonym in synonyms:
        if synonym.get("syn_type") == "EC_NUMBER":
            ec_numbers.append(synonym.get("component_synonym", ""))
reaction_ec_numbers = "|".join(filter(None, ec_numbers))
```

### 2. Среднесрочные исправления

1. **Реализовать полноценный UniProt парсер** по образцу референсного проекта
2. **Добавить CSV-словари для IUPHAR** классификации
3. **Реализовать маппинг между источниками**
4. **Добавить предсказание класса белка**

### 3. Долгосрочные исправления

1. **Рефакторинг архитектуры** по образцу референсного проекта
2. **Добавление полноценных интеграционных библиотек**
3. **Реализация сложной логики унификации данных**

## Заключение

Основная проблема заключается в упрощенной архитектуре текущего проекта по сравнению с референсным. Необходимо либо портировать функциональность из референсного проекта, либо реализовать недостающую логику с нуля.

Приоритетные исправления:
1. Исправить ChEMBL адаптер для извлечения всех полей
2. Реализовать полноценный UniProt парсер
3. Добавить IUPHAR классификацию
4. Реализовать маппинг между источниками

Без этих исправлений пайплайн мишеней будет продолжать генерировать неполные данные.
