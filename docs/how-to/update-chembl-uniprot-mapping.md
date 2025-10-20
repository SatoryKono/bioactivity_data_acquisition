# Обновление маппинга ChEMBL → UniProt

## Обзор

Скрипт `final_mapping_updater.py` автоматически дополняет файл маппинга `data/mappings/chembl_uniprot_mapping.csv` недостающими записями, используя ChEMBL и UniProt API.

## Использование

```bash
python final_mapping_updater.py
```

## Как это работает

1. Анализ текущего файла маппинга
2. Поиск недостающих ChEMBL ID
3. Получение UniProt ID:
   - ChEMBL API (прямые cross_references)
   - UniProt API (поиск по ChEMBL ID)
4. Обновление CSV маппинга
5. Генерация отчёта о покрытии

## Методы получения маппинга

### ChEMBL API (confidence: 0.95)
- `https://www.ebi.ac.uk/chembl/api/data/target/{chembl_id}` → `target_component_xrefs`

### UniProt API (confidence: 0.90-0.85)
- Запросы поиска: `database:(type:chembl {chembl_id})`, `xref:chembl-{chembl_id}`

## Формат файла маппинга

```csv
target_chembl_id,uniprot_id,confidence_score,source
CHEMBL1075102,B0LPH2,0.91,manual_curation
CHEMBL1795116,A8MTP3,0.85,automated_extraction
CHEMBL2343,O00141,0.95,manual_curation
```

Поля:
- `target_chembl_id` — ChEMBL target ID
- `uniprot_id` — Соответствующий UniProt ID
- `confidence_score` — Уверенность в маппинге (0.85-0.95)
- `source` — Источник маппинга

## Отчёт об обновлении

Скрипт печатает агрегированную статистику до/после обновления: покрытие, добавленные/обновлённые записи, не найденные идентификаторы.

## Интеграция в пайплайн

```python
# В src/scripts/get_target_data.py
from final_mapping_updater import UniProtMappingUpdater

def update_mapping_before_enrichment(target_df):
    updater = UniProtMappingUpdater()
    updater.update_mapping_for_targets(target_df)
```

## Практические замечания

- Rate limiting: ставьте небольшие задержки и ретраи; обрабатывайте HTTP 429
- Бэкапы: скрипт создаёт `.backup` оригинального файла маппинга
- Требования: Python 3.8+, pandas, requests


