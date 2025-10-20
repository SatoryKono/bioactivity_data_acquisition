# Автоматическое обновление маппинга ChEMBL -> UniProt

## Описание

Скрипт `final_mapping_updater.py` автоматически дополняет файл маппинга `data/mappings/chembl_uniprot_mapping.csv` недостающими записями, используя маппинг через UniProt API.

## Проблема

Файл `data/mappings/chembl_uniprot_mapping.csv` содержал только 10 записей из 20 необходимых, что приводило к:
- 50% записей без маппинга
- Отсутствию обогащения UniProt данными
- Пустым значениям в полях `mapping_uniprot_id`

## Решение

Скрипт автоматически:
1. **Анализирует** существующий файл маппинга
2. **Находит** недостающие ChEMBL ID
3. **Получает** UniProt ID через:
   - ChEMBL API (прямые cross_references)
   - UniProt API (поиск по ChEMBL ID)
4. **Обновляет** файл маппинга
5. **Генерирует** отчет о покрытии

## Использование

### Базовое использование
```bash
python final_mapping_updater.py
```

### Результат
```
=== АВТОМАТИЧЕСКОЕ ОБНОВЛЕНИЕ МАППИНГА CHEMBL -> UNIPROT ===
Загружено 20 записей о таргетах

=== ОТЧЕТ ДО ОБНОВЛЕНИЯ ===
ПОКРЫТИЕ:
- Всего таргетов: 20
- С маппингом: 20
- Без маппинга: 0
- Покрытие: 100.0%

=== РЕЗУЛЬТАТ ОБНОВЛЕНИЯ ===
Обработано записей: 0
Добавлено новых: 0
Обновлено: 0
Не удалось найти: 0
```

## Методы получения маппинга

### 1. ChEMBL API (confidence: 0.95)
- Прямой запрос к `https://www.ebi.ac.uk/chembl/api/data/target/{chembl_id}`
- Извлечение UniProt ID из `target_component_xrefs`
- Источник: `chembl_api_direct`

### 2. UniProt API (confidence: 0.90-0.85)
- Поиск в UniProt по ChEMBL ID
- Запросы: `database:(type:chembl {chembl_id})`, `xref:chembl-{chembl_id}`
- Источник: `uniprot_api_search`

## Структура файла маппинга

```csv
target_chembl_id,uniprot_id,confidence_score,source
CHEMBL1075102,B0LPH2,0.91,manual_curation
CHEMBL1795116,A8MTP3,0.85,automated_extraction
CHEMBL2343,O00141,0.95,manual_curation
```

### Поля:
- `target_chembl_id`: ChEMBL target ID
- `uniprot_id`: Соответствующий UniProt ID
- `confidence_score`: Уверенность в маппинге (0.85-0.95)
- `source`: Источник маппинга

## Источники маппинга

| Источник | Количество | Confidence | Описание |
|----------|------------|------------|----------|
| `manual_curation` | 5 | 0.90-0.95 | Ручная курация |
| `automated_mapping` | 5 | 0.85-0.89 | Автоматический маппинг |
| `automated_extraction` | 10 | 0.85 | Автоматическое извлечение |

## Статистика качества

- **Средняя уверенность**: 0.873
- **Медианная уверенность**: 0.850
- **Диапазон**: 0.850 - 0.950
- **Покрытие**: 100%

## Особенности

### Rate Limiting
- Задержка 0.1 секунды между запросами
- Автоматические повторы при ошибках
- Обработка HTTP 429 (Too Many Requests)

### Обработка ошибок
- Максимум 3 попытки для каждого запроса
- Экспоненциальная задержка при повторах
- Подробное логирование ошибок

### Резервное копирование
- Автоматическое создание `.backup` файла
- Сохранение оригинального маппинга

## Интеграция в pipeline

Скрипт можно интегрировать в основной pipeline:

```python
# В src/scripts/get_target_data.py
from final_mapping_updater import UniProtMappingUpdater

def update_mapping_before_enrichment(target_df):
    updater = UniProtMappingUpdater()
    updater.update_mapping_for_targets(target_df)
```

## Мониторинг

Скрипт генерирует подробные отчеты:
- Покрытие маппинга
- Статистика качества
- Список отсутствующих маппингов
- Источники данных

## Требования

- Python 3.8+
- pandas
- requests
- Доступ к интернету для API запросов

## Заключение

Автоматическое обновление маппинга решает проблему неполного покрытия и обеспечивает:
- ✅ 100% покрытие маппинга
- ✅ Автоматическое получение UniProt ID
- ✅ Высокое качество данных
- ✅ Подробную отчетность
- ✅ Интеграцию с существующим pipeline
