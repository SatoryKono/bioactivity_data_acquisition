# Руководство по сохранению регистра в данных

## Проблема

Ранее функция `_normalize_dataframe` принудительно приводила все строковые колонки к нижнему регистру, что приводило к повреждению чувствительных к регистру данных:
- **SMILES** (химические структуры): `CCO` становился `cco`, что изменяет химическую структуру
- **Названия соединений**: `Aspirin` становился `aspirin`
- **Единицы измерения**: `nM`, `uM`, `pM` становились `nm`, `um`, `pm`
- **Названия источников данных**: `ChEMBL` становился `chembl`

## Решение

Начиная с текущей версии, нормализация регистра стала **конфигурируемой** через параметр `lowercase_columns` в настройках `DeterminismSettings`.

### По умолчанию: сохранение регистра

```yaml
determinism:
  lowercase_columns: []  # Пустой список - регистр сохраняется для ВСЕХ колонок
```

### Селективное приведение к нижнему регистру

Если вам нужно привести к нижнему регистру только определённые колонки (например, для нормализации источников данных):

```yaml
determinism:
  lowercase_columns:
    - source
    - journal
```

В этом случае:
- ✅ `smiles`, `target`, `activity_unit` и другие колонки **сохранят** исходный регистр
- ✅ `source` и `journal` будут приведены к нижнему регистру для унификации

## Примеры использования

### Пример 1: Биоактивность с сохранением SMILES

```python
from library.config import DeterminismSettings, SortSettings
from library.etl.load import write_deterministic_csv
import pandas as pd

df = pd.DataFrame({
    'compound_id': ['CHEMBL1', 'CHEMBL2'],
    'smiles': ['CCO', 'c1ccccc1'],  # Важно сохранить регистр!
    'target': ['ProteinA', 'EnzymeB'],
    'activity_unit': ['nM', 'uM'],
    'source': ['ChEMBL', 'PubChem']
})

# Конфигурация: сохраняем регистр для всех колонок кроме source
determinism = DeterminismSettings(
    sort=SortSettings(by=['compound_id'], ascending=[True]),
    column_order=['compound_id', 'smiles', 'target', 'activity_unit', 'source'],
    lowercase_columns=['source']  # Только source приводится к нижнему регистру
)

write_deterministic_csv(df, 'output.csv', determinism=determinism)
```

Результат:
```csv
index,compound_id,smiles,target,activity_unit,source
0,CHEMBL1,CCO,ProteinA,nM,chembl
1,CHEMBL2,c1ccccc1,EnzymeB,uM,pubchem
```

### Пример 2: Полное сохранение регистра

```python
# Конфигурация: сохраняем регистр для ВСЕХ колонок
determinism = DeterminismSettings(
    sort=SortSettings(by=['compound_id'], ascending=[True]),
    column_order=['compound_id', 'smiles', 'target', 'activity_unit', 'source'],
    lowercase_columns=[]  # Пустой список - регистр сохраняется
)

write_deterministic_csv(df, 'output.csv', determinism=determinism)
```

Результат:
```csv
index,compound_id,smiles,target,activity_unit,source
0,CHEMBL1,CCO,ProteinA,nM,ChEMBL
1,CHEMBL2,c1ccccc1,EnzymeB,uM,PubChem
```

## Конфигурация через YAML

В файле `config.yaml`:

```yaml
determinism:
  sort:
    by:
      - compound_id
      - target
    ascending:
      - true
      - true
    na_position: last
  
  column_order:
    - compound_id
    - target
    - smiles
    - activity_value
    - activity_unit
    - source
  
  # ВАЖНО: Оставьте пустым для сохранения регистра во всех колонках
  lowercase_columns: []
  
  # Или укажите конкретные колонки для приведения к нижнему регистру:
  # lowercase_columns:
  #   - source
  #   - journal
```

## Рекомендации

### ✅ Рекомендуется сохранять регистр для:

- **SMILES** - химические структуры чувствительны к регистру
- **Названия соединений** - proper nouns должны сохранять капитализацию
- **Названия белков/генов** - биологическая номенклатура чувствительна к регистру
- **Единицы измерения** - `nM` ≠ `nm`, `uM` ≠ `um`
- **Идентификаторы** - `CHEMBL1` ≠ `chembl1`
- **Заголовки публикаций** - сохранение авторской капитализации

### ⚠️ Можно приводить к нижнему регистру:

- **Источники данных** - для унификации (`ChEMBL`, `chembl`, `CHEMBL` → `chembl`)
- **Названия журналов** - для унификации поиска
- **Теги/категории** - для унификации фильтрации

## Тестирование

Проверьте, что регистр сохраняется правильно:

```python
# Запустите тесты
pytest tests/test_deterministic_output.py::test_case_sensitivity_preservation -v
pytest tests/test_deterministic_output.py::test_selective_lowercase_normalization -v
```

## Миграция с предыдущих версий

Если вы обновляетесь с версии, где все строки приводились к нижнему регистру:

1. **Проверьте ваши данные**: убедитесь, что SMILES и другие чувствительные поля не повреждены
2. **Обновите конфигурацию**: добавьте `lowercase_columns: []` в секцию `determinism`
3. **Пересоздайте выходные файлы**: запустите ETL pipeline заново с новой конфигурацией
4. **Проверьте downstream системы**: убедитесь, что они корректно обрабатывают данные с правильным регистром

## Технические детали

### Функция `_normalize_dataframe`

Логика нормализации (src/library/etl/load.py, строки 128-162):

```python
elif df_normalized[column].dtype == 'object':  # Обычные строковые данные
    # Заменяем None на NA
    df_normalized[column] = df_normalized[column].replace([None], pd.NA)
    
    # Определяем, нужно ли приводить эту колонку к нижнему регистру
    should_lowercase = (
        determinism is not None and 
        determinism.lowercase_columns is not None and 
        column in determinism.lowercase_columns
    )
    
    # Нормализуем все значения
    for idx in df_normalized.index:
        value = df_normalized.loc[idx, column]
        if pd.isna(value):
            continue
        
        # Конвертируем в строку и обрезаем пробелы
        str_value = str(value).strip()
        
        # Приводим к нижнему регистру ТОЛЬКО если колонка указана в конфигурации
        if should_lowercase:
            str_value = str_value.lower()
        
        # Обрабатываем пустые значения
        if str_value in ['', 'nan', 'none', 'null']:
            df_normalized.loc[idx, column] = pd.NA
        else:
            df_normalized.loc[idx, column] = str_value
```

### Параметр конфигурации

В `src/library/config.py`:

```python
class DeterminismSettings(BaseModel):
    """Settings for deterministic, reproducible output."""
    
    lowercase_columns: list[str] = Field(
        default_factory=list,
        description="Список колонок, которые должны быть приведены к нижнему регистру при нормализации. "
                    "По умолчанию пустой - регистр сохраняется."
    )
```

## Поддержка

Если у вас возникли вопросы или проблемы:

1. Проверьте логи: убедитесь, что `lowercase_columns` настроен правильно
2. Запустите тесты: `pytest tests/test_deterministic_output.py -v`
3. Проверьте выходные данные: откройте CSV файл и убедитесь, что регистр сохранён

## История изменений

- **v1.0**: Принудительное приведение всех строк к нижнему регистру (устаревшее поведение)
- **v2.0**: Добавлен параметр `lowercase_columns` для конфигурируемой нормализации регистра
- **v2.1**: По умолчанию регистр сохраняется для всех колонок (`lowercase_columns: []`)

