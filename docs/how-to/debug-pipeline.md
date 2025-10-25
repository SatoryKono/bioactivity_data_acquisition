# Отладка пайплайна

Руководство по диагностике и устранению проблем в ETL пайплайнах.

## Обзор методов отладки

### 1. Логирование

#### Уровни логирования

```bash
# DEBUG - максимальная детализация
LOG_LEVEL=DEBUG make run ENTITY=documents CONFIG=configs/config_documents_full.yaml

# INFO - стандартная информация
LOG_LEVEL=INFO make run ENTITY=documents CONFIG=configs/config_documents_full.yaml

# WARNING - только предупреждения и ошибки
LOG_LEVEL=WARNING make run ENTITY=documents CONFIG=configs/config_documents_full.yaml
```

#### Структурированные логи

```python
import structlog
logger = structlog.get_logger()

# В коде пайплайна
logger.info("Processing batch", batch_id=123, records_count=1000)
logger.error("API request failed", endpoint="/documents", status_code=500)
```

#### Просмотр логов

```bash
# Реальное время
tail -f logs/app.log

# Фильтрация по уровню
grep "ERROR" logs/app.log
grep "WARNING" logs/app.log

# Поиск по контексту
grep "documents" logs/app.log
grep "API" logs/app.log
```

### 2. Проверка конфигурации

#### Валидация конфига

```bash
# Проверка синтаксиса YAML
python -c "
import yaml
with open('configs/config_documents_full.yaml', 'r') as f:
    config = yaml.safe_load(f)
    print('✅ YAML синтаксис корректен')
"

# Проверка схемы конфигурации
bioactivity-data-acquisition validate-config configs/config_documents_full.yaml
```

#### Проверка переменных окружения

```bash
# Просмотр всех переменных
env | grep -E "(LOG_|API_|CACHE_)"

# Проверка конкретных переменных
echo "LOG_LEVEL: $LOG_LEVEL"
echo "OUTPUT_DIR: $OUTPUT_DIR"
```

### 3. Тестирование API подключений

#### Проверка доступности API

```bash
# Тест всех API клиентов
make test-api-connections

# Индивидуальная проверка
python -c "
from library.clients.chembl import ChEMBLClient
client = ChEMBLClient()
result = client.health_check()
print(f'ChEMBL: {result}')
"
```

#### Проверка rate limits

```bash
# Мониторинг использования API
python -c "
from library.clients.chembl import ChEMBLClient
import time

client = ChEMBLClient()
for i in range(5):
    start = time.time()
    result = client.get_documents(limit=1)
    duration = time.time() - start
    print(f'Request {i+1}: {duration:.2f}s')
    time.sleep(1)
"
```

## Диагностика по этапам пайплайна

### 1. Extract (Извлечение данных)

#### Проверка входных данных

```python
# Валидация входного CSV
import pandas as pd
from library.schemas.document_input_schema import DocumentInputSchema

df = pd.read_csv('data/input/documents.csv')
print(f"Записей: {len(df)}")
print(f"Колонки: {list(df.columns)}")
print(f"Пустые значения: {df.isnull().sum()}")

try:
    validated = DocumentInputSchema.validate(df)
    print("✅ Входные данные валидны")
except Exception as e:
    print(f"❌ Ошибка валидации: {e}")
```

#### Проверка API ответов

```python
# Тест API клиента
from library.clients.chembl import ChEMBLClient

client = ChEMBLClient()
try:
    # Тестовый запрос
    result = client.get_documents(limit=5)
    print(f"✅ API работает, получено {len(result)} записей")
    print(f"Пример данных: {result[0] if result else 'Нет данных'}")
except Exception as e:
    print(f"❌ Ошибка API: {e}")
```

### 2. Transform (Трансформация данных)

#### Проверка нормализации

```python
# Тест нормализации данных
from library.etl.transform import normalize_document_data

# Загрузить сырые данные
raw_data = pd.read_csv('data/cache/documents_raw.csv')
print(f"Сырые данные: {len(raw_data)} записей")

# Нормализация
try:
    normalized = normalize_document_data(raw_data)
    print(f"✅ Нормализация успешна: {len(normalized)} записей")
    
    # Проверка качества
    fill_rate = normalized.notna().mean().mean()
    print(f"Fill rate: {fill_rate:.2%}")
except Exception as e:
    print(f"❌ Ошибка нормализации: {e}")
```

#### Проверка валидации

```python
# Тест Pandera схем
from library.schemas.document_output_schema import DocumentOutputSchema

try:
    validated = DocumentOutputSchema.validate(normalized)
    print("✅ Валидация схемы успешна")
except Exception as e:
    print(f"❌ Ошибка валидации: {e}")
    # Детальная информация об ошибках
    if hasattr(e, 'failure_cases'):
        print("Проблемные записи:")
        print(e.failure_cases)
```

### 3. Load (Загрузка данных)

#### Проверка выходных файлов

```bash
# Проверка создания файлов
ls -la data/output/documents_*/

# Проверка размера файлов
du -h data/output/documents_*/*.csv

# Проверка содержимого
head -5 data/output/documents_*/documents_*.csv
```

#### Валидация выходных данных

```python
# Проверка финальных данных
output_df = pd.read_csv('data/output/documents_*/documents_*.csv')
print(f"Выходных записей: {len(output_df)}")
print(f"Колонки: {list(output_df.columns)}")

# Проверка дубликатов
duplicates = output_df.duplicated().sum()
print(f"Дубликаты: {duplicates}")

# Проверка обязательных полей
required_fields = ['document_chembl_id', 'title', 'doi']
for field in required_fields:
    null_count = output_df[field].isnull().sum()
    print(f"{field}: {null_count} пустых значений")
```

## Специфичные проблемы

### 1. Проблемы с API

#### Timeout ошибки

```python
# Диагностика timeout
import requests
import time

def test_timeout(url, timeout=30):
    try:
        start = time.time()
        response = requests.get(url, timeout=timeout)
        duration = time.time() - start
        print(f"✅ {url}: {response.status_code} за {duration:.2f}s")
        return True
    except requests.exceptions.Timeout:
        print(f"❌ {url}: Timeout после {timeout}s")
        return False
    except Exception as e:
        print(f"❌ {url}: {e}")
        return False

# Тест основных API
test_timeout("https://www.ebi.ac.uk/chembl/api/data/document")
test_timeout("https://api.crossref.org/works")
```

#### Rate limit превышен

```python
# Мониторинг rate limits
from library.clients.chembl import ChEMBLClient

client = ChEMBLClient()
for i in range(10):
    try:
        result = client.get_documents(limit=1)
        print(f"✅ Запрос {i+1} успешен")
    except Exception as e:
        if "429" in str(e):
            print(f"❌ Rate limit превышен на запросе {i+1}")
            break
        else:
            print(f"❌ Другая ошибка: {e}")
```

### 2. Проблемы с данными

#### Некорректные типы данных

```python
# Диагностика типов данных
df = pd.read_csv('data/input/documents.csv')
print("Типы данных:")
print(df.dtypes)

# Проверка проблемных колонок
for col in df.columns:
    if df[col].dtype == 'object':
        # Проверка на числовые значения в текстовых полях
        numeric_count = pd.to_numeric(df[col], errors='coerce').notna().sum()
        if numeric_count > 0:
            print(f"⚠️ {col}: {numeric_count} числовых значений в текстовом поле")
```

#### Проблемы с кодировкой

```python
# Проверка кодировки файлов
import chardet

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        return result['encoding']

encoding = detect_encoding('data/input/documents.csv')
print(f"Кодировка файла: {encoding}")

# Перекодировка при необходимости
if encoding != 'utf-8':
    df = pd.read_csv('data/input/documents.csv', encoding=encoding)
    df.to_csv('data/input/documents_utf8.csv', encoding='utf-8', index=False)
```

### 3. Проблемы с памятью

#### Мониторинг использования памяти

```python
import psutil
import pandas as pd

def monitor_memory():
    process = psutil.Process()
    memory_info = process.memory_info()
    print(f"Использование памяти: {memory_info.rss / 1024 / 1024:.1f} MB")

# Проверка перед загрузкой больших файлов
monitor_memory()
df = pd.read_csv('large_file.csv')
monitor_memory()
```

#### Оптимизация памяти

```python
# Загрузка по частям
chunk_size = 10000
chunks = []

for chunk in pd.read_csv('large_file.csv', chunksize=chunk_size):
    # Обработка чанка
    processed_chunk = process_chunk(chunk)
    chunks.append(processed_chunk)
    
    # Мониторинг памяти
    monitor_memory()

# Объединение результатов
result = pd.concat(chunks, ignore_index=True)
```

## Инструменты отладки

### 1. Python debugger

```python
# В коде пайплайна
import pdb; pdb.set_trace()

# Или с помощью breakpoint() (Python 3.7+)
breakpoint()
```

### 2. Профилирование

```python
# Профилирование производительности
import cProfile
import pstats

def profile_pipeline():
    # Запуск пайплайна
    pipeline.run()

# Запуск с профилированием
cProfile.run('profile_pipeline()', 'profile_output.prof')

# Анализ результатов
stats = pstats.Stats('profile_output.prof')
stats.sort_stats('cumulative').print_stats(10)
```

### 3. Визуализация данных

```python
# Быстрая визуализация для отладки
import matplotlib.pyplot as plt

# Распределение значений
df['year'].hist(bins=50)
plt.title('Распределение годов публикации')
plt.show()

# Корреляционная матрица
correlation_matrix = df.select_dtypes(include=[np.number]).corr()
plt.imshow(correlation_matrix, cmap='coolwarm')
plt.colorbar()
plt.show()
```

## Автоматизированная диагностика

### Скрипт полной диагностики

```python
#!/usr/bin/env python3
"""Скрипт полной диагностики пайплайна"""

import sys
import pandas as pd
from pathlib import Path

def run_diagnostics():
    print("🔍 Запуск диагностики пайплайна...")
    
    # 1. Проверка конфигурации
    print("\n1. Проверка конфигурации...")
    try:
        import yaml
        with open('configs/config_documents_full.yaml', 'r') as f:
            config = yaml.safe_load(f)
        print("✅ Конфигурация загружена")
    except Exception as e:
        print(f"❌ Ошибка конфигурации: {e}")
        return False
    
    # 2. Проверка входных данных
    print("\n2. Проверка входных данных...")
    input_file = Path('data/input/documents.csv')
    if input_file.exists():
        df = pd.read_csv(input_file)
        print(f"✅ Входной файл: {len(df)} записей")
    else:
        print("❌ Входной файл не найден")
        return False
    
    # 3. Проверка API
    print("\n3. Проверка API...")
    try:
        from library.clients.chembl import ChEMBLClient
        client = ChEMBLClient()
        result = client.health_check()
        print(f"✅ ChEMBL API: {result}")
    except Exception as e:
        print(f"❌ Ошибка API: {e}")
    
    # 4. Проверка выходных директорий
    print("\n4. Проверка выходных директорий...")
    output_dir = Path('data/output')
    if output_dir.exists():
        print(f"✅ Выходная директория существует")
    else:
        print("❌ Выходная директория не найдена")
        output_dir.mkdir(parents=True, exist_ok=True)
        print("✅ Создана выходная директория")
    
    print("\n🎉 Диагностика завершена!")
    return True

if __name__ == "__main__":
    success = run_diagnostics()
    sys.exit(0 if success else 1)
```

### Makefile команды для отладки

```makefile
# Добавить в Makefile
.PHONY: debug-api debug-config debug-data debug-full

debug-api:
    @echo "🔍 Тестирование API подключений..."
    python -c "from library.clients.chembl import ChEMBLClient; print(ChEMBLClient().health_check())"

debug-config:
    @echo "🔍 Проверка конфигурации..."
    python -c "import yaml; yaml.safe_load(open('configs/config_documents_full.yaml'))"

debug-data:
    @echo "🔍 Проверка входных данных..."
    python -c "import pandas as pd; df=pd.read_csv('data/input/documents.csv'); print(f'Записей: {len(df)}')"

debug-full:
    @echo "🔍 Полная диагностика..."
    python scripts/debug_pipeline.py
```

## Получение помощи

### Логи для поддержки

```bash
# Создание архива с логами для поддержки
tar -czf debug_logs_$(date +%Y%m%d).tar.gz logs/ data/output/ configs/
```

### Информация о системе

```bash
# Сбор информации о системе
python -c "
import sys, platform, pandas as pd, pandera as pa
print(f'Python: {sys.version}')
print(f'Platform: {platform.platform()}')
print(f'Pandas: {pd.__version__}')
print(f'Pandera: {pa.__version__}')
"
```
