# Пайплайн Assay - Извлечение данных ассев из ChEMBL

## 🎯 Обзор

Пайплайн Assay предназначен для извлечения и нормализации данных ассев из ChEMBL API в соответствии со стандартами проекта. Обеспечивает детерминированную обработку данных с полной валидацией и создание измерения `assay_dim` для звёздной схемы.

## ✨ Основные возможности

- **Извлечение по идентификаторам ассев** или **по таргетам**
- **Профили фильтрации** для различных сценариев использования
- **Детерминированная сериализация** для воспроизводимости результатов
- **Строгая валидация** с помощью Pandera схем
- **Graceful degradation** при ошибках API
- **Автоматическое обогащение** данными источников
- **QC отчеты** и метаданные
- **Кэширование** для оптимизации производительности

## 🚀 Быстрый старт

### 1. Установка

```bash
# Установка зависимостей
make -f Makefile.assay install-assay

# Или вручную
pip install -r requirements.txt
```

### 2. Проверка конфигурации

```bash
# Проверка конфигурации
make -f Makefile.assay validate-assay-config

# Проверка статуса ChEMBL API
make -f Makefile.assay assay-status
```

### 3. Запуск с примером данных

```bash
# Запуск с примером данных
make -f Makefile.assay assay-example

# Или вручную
python src/scripts/get_assay_data.py \
    --input data/input/assay_ids_example.csv \
    --config configs/config_assay_full.yaml
```

## 📋 Использование

### CLI команды

#### Извлечение по идентификаторам ассев

```bash
python src/scripts/get_assay_data.py \
    --input data/input/assay_ids.csv \
    --config configs/config_assay_full.yaml
```

#### Извлечение по таргету

```bash
python src/scripts/get_assay_data.py \
    --target CHEMBL231 \
    --config configs/config_assay_full.yaml
```

#### Использование профилей фильтрации

```bash
# Человеческие белки с высокой уверенностью
python src/scripts/get_assay_data.py \
    --target CHEMBL231 \
    --filters human_single_protein \
    --config configs/config_assay_full.yaml

# Только binding ассеи
python src/scripts/get_assay_data.py \
    --target CHEMBL231 \
    --filters binding_assays \
    --config configs/config_assay_full.yaml

# Высококачественные ассеи
python src/scripts/get_assay_data.py \
    --target CHEMBL231 \
    --filters high_quality \
    --config configs/config_assay_full.yaml
```

### Makefile команды

```bash
# Показать все доступные команды
make -f Makefile.assay help

# Быстрый старт
make -f Makefile.assay assay-quick-start

# Извлечение по таргету
make -f Makefile.assay assay-target

# Тестовый запуск
make -f Makefile.assay assay-dry-run

# Запуск тестов
make -f Makefile.assay test-assay
```

## 🏗️ Архитектура

### Этапы пайплайна

1. **S01_status_and_release** - Получение статуса ChEMBL и фиксация релиза
2. **S02_fetch_assay_core** - Извлечение основных данных ассев
3. **S03_enrich_source** - Обогащение данными источников
4. **S04_normalize_fields** - Нормализация полей
5. **S05_validate_schema** - Валидация схемы данных
6. **S06_persist_and_meta** - Сохранение и создание метаданных

### Структура файлов

```text
src/library/assay/
├── __init__.py          # Экспорты модуля
├── config.py            # Конфигурация пайплайна
├── client.py            # Клиент для ChEMBL API
└── pipeline.py          # Основная логика ETL

src/library/schemas/
└── assay_schema.py      # Pandera схемы валидации

src/scripts/
└── get_assay_data.py    # CLI скрипт

configs/
└── config_assay_full.yaml           # Конфигурация пайплайна

tests/
└── test_assay_pipeline.py  # Тесты

docs/
├── assay.md             # Техническая документация
└── assay_usage.md       # Руководство по использованию
```

## 📊 Выходные данные

### Файлы результатов

- **`assay_YYYYMMDD.csv`** - Основные данные ассев
- **`assay_YYYYMMDD_qc.csv`** - Отчет о качестве данных
- **`assay_YYYYMMDD_meta.yaml`** - Метаданные пайплайна

### Схема данных

| Поле | Описание | Тип | Источник |
|------|----------|-----|----------|
| `assay_chembl_id` | ID ассая в ChEMBL | str | assay.assay_chembl_id |
| `src_id` | ID источника | int | assay.src_id |
| `src_name` | Название источника | str | source.src_description |
| `assay_type` | Тип ассая (B/F/P/U) | str | assay.assay_type |
| `relationship_type` | Тип связи с таргетом | str | assay.relationship_type |
| `confidence_score` | Уровень уверенности (0-9) | int | assay.confidence_score |
| `assay_organism` | Организм ассая | str | assay.assay_organism |
| `description` | Описание ассая | str | assay.description |

## ⚙️ Конфигурация

### Основные настройки

```yaml
# HTTP настройки
http:
  global:
    timeout_sec: 60.0
    retries:
      total: 10
      backoff_multiplier: 3.0
    rate_limit:
      max_calls: 3
      period: 15.0

# Входные данные
io:
  input:
    assay_ids_csv: data/input/assay_ids.csv
    target_ids_csv: data/input/target_ids.csv
  output:
    dir: data/output/assay
    format: csv

# Валидация
validation:
  strict: true
  qc:
    max_missing_fraction: 0.02
    max_duplicate_fraction: 0.005
```

### Профили фильтрации

```yaml
filter_profiles:
  human_single_protein:
    target_organism: "Homo sapiens"
    target_type: "SINGLE PROTEIN"
    relationship_type: "D"
    confidence_score__range: "7,9"
    assay_type__in: "B,F"
  
  binding_assays:
    assay_type: "B"
    relationship_type: "D"
    confidence_score__range: "5,9"
  
  high_quality:
    confidence_score__range: "7,9"
    relationship_type: "D"
    assay_type__in: "B,F"
```

## 🧪 Тестирование

### Запуск тестов

```bash
# Все тесты
make -f Makefile.assay test-assay

# С покрытием кода
make -f Makefile.assay test-assay-coverage

# Только unit тесты
make -f Makefile.assay test-assay-unit

# Тесты клиента
make -f Makefile.assay test-assay-client
```

### Типы тестов

- **Unit тесты** - Тестирование отдельных функций
- **Contract тесты** - Проверка контрактов API
- **E2E тесты** - Полный цикл обработки
- **QC тесты** - Проверка качества данных

## 🔧 Разработка

### Программное использование

```python
from library.assay import AssayConfig, load_assay_config, run_assay_etl, write_assay_outputs
from pathlib import Path

# Загрузка конфигурации
config = load_assay_config("configs/config_assay_full.yaml")

# Запуск ETL
result = run_assay_etl(
    config=config,
    assay_ids=["CHEMBL123456", "CHEMBL789012"]
)

# Сохранение результатов
output_paths = write_assay_outputs(
    result=result,
    output_dir=Path("data/output/assay"),
    date_tag="20230101",
    config=config
)
```

### Проверка кода

```bash
# Линтинг
make -f Makefile.assay lint-assay

# Форматирование
make -f Makefile.assay format-assay
```

## 📚 Документация

- **[Техническая документация](docs/assay.md)** - Детальное описание архитектуры
- **[Руководство по использованию](docs/assay_usage.md)** - Примеры и инструкции
- **[API документация](docs/api/)** - Справочник по API

## 🚨 Устранение неполадок

### Частые проблемы

1. **Ошибка "API Error"**

   ```bash
   # Увеличить таймаут
   python src/scripts/get_assay_data.py --target CHEMBL231 --timeout 120
   ```

2. **Медленная работа**

   ```bash
   # Ограничить количество записей
   python src/scripts/get_assay_data.py --target CHEMBL231 --limit 100
   ```

3. **Ошибка валидации**

   ```bash
   # Проверить конфигурацию
   make -f Makefile.assay validate-assay-config
   ```

### Получение помощи

```bash
# Справка CLI
make -f Makefile.assay show-assay-help

# Примеры использования
make -f Makefile.assay show-assay-examples

# Информация о пайплайне
make -f Makefile.assay assay-info
```

## 📈 Производительность

### Рекомендации

- Используйте профили фильтрации для уменьшения объема данных
- Настройте кэширование для повторных запросов
- Ограничивайте количество записей при тестировании
- Используйте Parquet формат для больших объемов данных

### Мониторинг

```bash
# Проверка статуса ChEMBL
make -f Makefile.assay assay-status

# Тестовый запуск
make -f Makefile.assay assay-dry-run
```

## 🤝 Вклад в разработку

1. Форкните репозиторий
2. Создайте ветку для новой функции
3. Добавьте тесты для новой функциональности
4. Убедитесь, что все тесты проходят
5. Создайте Pull Request

## 📄 Лицензия

Проект использует ту же лицензию, что и основной репозиторий.

## 🔗 Ссылки

- [ChEMBL API Documentation](https://chembl.gitbook.io/chembl-interface-documentation/web-services/chembl-data-web-services)
- [ChEMBL Assay Endpoint](https://www.ebi.ac.uk/chembl/api/data/assay)
- [Pandera Documentation](https://pandera.readthedocs.io/)
- [Pydantic Documentation](https://pydantic-docs.helpmanual.io/)

---

**Версия**: 1.0.0  
**Последнее обновление**: 2024-01-01  
**Автор**: Senior Python ETL Engineer
