# Часто задаваемые вопросы (FAQ)

## Установка и настройка

### Как установить проект?

```bash
# Клонирование репозитория
git clone https://github.com/SatoryKono/bioactivity_data_acquisition.git
cd bioactivity_data_acquisition

# Установка зависимостей
pip install .[dev]
```

### Какие версии Python поддерживаются?

Проект поддерживает Python 3.10 и выше. Рекомендуется использовать Python 3.11.

### Как настроить виртуальное окружение?

```bash
# Создание виртуального окружения
python -m venv .venv

# Активация (Windows)
.venv\Scripts\activate

# Активация (Linux/macOS)
source .venv/bin/activate

# Установка зависимостей
pip install .[dev]
```

## Запуск пайплайнов

### Как запустить пайплайн?

Используйте унифицированный интерфейс v2:

```bash
# Документы
make run ENTITY=documents CONFIG=configs/config_documents_v2.yaml

# Мишени
make run ENTITY=targets CONFIG=configs/config_targets_v2.yaml

# Эксперименты
make run ENTITY=assays CONFIG=configs/config_assays_v2.yaml

# Активности
make run ENTITY=activities CONFIG=configs/config_activities_v2.yaml

# Молекулы
make run ENTITY=testitems CONFIG=configs/config_testitems_v2.yaml
```

### Как запустить быстрый старт?

```bash
make quick-start
```

### В чём разница между v1 и v2 конфигурациями?

**v2 конфигурации (рекомендуемые):**

- Унифицированная структура
- Улучшенная валидация
- Стандартизированные артефакты
- Унифицированные CLI флаги

**v1 конфигурации (deprecated):**

- Старая структура
- Менее строгая валидация
- Совместимость сохранена

## Логи и отладка

### Где найти логи?

Логи сохраняются в:

- `data/logs/` — основные логи пайплайнов
- `src/logs/` — логи библиотеки
- `tests/logs/` — логи тестов

### Как включить подробное логирование?

```bash
# Через переменную окружения
export BIOACTIVITY__LOGGING__LEVEL=DEBUG

# Через конфигурацию
logging:
  level: DEBUG
```

### Что делать при ошибках валидации?

1. Проверьте входные данные на соответствие схемам
2. Посмотрите отчёты в `outputs/qa/`
3. Проверьте `outputs/failures/` для деталей ошибок
4. Убедитесь, что все обязательные поля заполнены

## Источники данных

### Какие источники данных поддерживаются?

- **ChEMBL** — основная база биоактивностных данных
- **UniProt** — информация о белках
- **IUPHAR** — фармакологические данные
- **Crossref** — метаданные публикаций
- **PubMed** — медицинские публикации
- **OpenAlex** — открытые научные данные
- **Semantic Scholar** — семантический поиск публикаций
- **PubChem** — химические данные

### Как настроить API ключи?

```bash
# Через переменные окружения
export BIOACTIVITY__SOURCES__CROSSREF__API_KEY=your_key
export BIOACTIVITY__SOURCES__OPENALEX__API_KEY=your_key

# Через конфигурацию
sources:
  crossref:
    api_key: your_key
  openalex:
    api_key: your_key
```

### Какие лимиты API действуют?

| Источник | RPS | Бюджет | TTL кэша |
|----------|-----|--------|----------|
| ChEMBL | ≤5 | ≤100k | 1 день |
| PubMed | ≤3 | ≤50k | 7 дней |
| UniProt | ≤3 | ≤30k | 14 дней |
| Crossref | ≤3 | ≤30k | 7 дней |
| OpenAlex | ≤3 | ≤30k | 7 дней |
| PubChem | ≤5 | ≤50k | 7 дней |
| IUPHAR | ≤2 | ≤10k | 14 дней |

## Выходные данные

### Какие файлы создаются?

**Основные артефакты:**

- `outputs/final/*.csv` — финальные данные
- `outputs/qa/*_quality_report_table.csv` — отчёты качества
- `outputs/qa/*.postprocess.report.json` — отчёты постобработки
- `outputs/failures/*_failure_cases.csv` — случаи ошибок
- `outputs/meta/meta.yaml` — метаданные релиза

### Как проверить качество данных?

```bash
# Запуск QC проверок
make run ENTITY=documents CONFIG=configs/config_documents_v2.yaml

# Просмотр отчётов
cat outputs/qa/documents_quality_report_table.csv
cat outputs/qa/documents.postprocess.report.json
```

### Что означают коды ошибок?

Основные коды ошибок:

- `unit_blacklisted` — запрещённая единица измерения
- `unit_unknown` — неизвестная единица измерения
- `hash_collision` — коллизия хешей
- `orphan_fk` — нарушение ссылочной целостности
- `smiles_invalid` — невалидная SMILES строка
- `doi_invalid` — невалидный DOI
- `uniprot_ambiguous` — неоднозначный UniProt ID

## Разработка

### Как запустить тесты?

```bash
# Все тесты
pytest

# С покрытием
pytest --cov=library

# Конкретный модуль
pytest tests/test_testitem_pipeline.py
```

### Как проверить код?

```bash
# Линтинг
ruff check src/

# Типизация
mypy src/

# Форматирование
black src/
```

### Как добавить новый пайплайн?

1. Создайте модуль в `src/library/`
2. Добавьте конфигурацию в `configs/`
3. Создайте тесты в `tests/`
4. Обновите документацию

## Проблемы и решения

### Ошибка "ModuleNotFoundError"

Убедитесь, что проект установлен в режиме разработки:

```bash
pip install -e .[dev]
```

### Ошибка "Permission denied" при записи файлов

Проверьте права доступа к директории `data/outputs/`:

```bash
# Создайте директорию если не существует
mkdir -p data/outputs
chmod 755 data/outputs
```

### Медленная работа пайплайна

1. Проверьте лимиты API
2. Увеличьте размер батча в конфигурации
3. Используйте кэширование
4. Проверьте сетевое соединение

### Ошибки валидации Pandera

1. Проверьте схемы в `src/library/schemas/`
2. Убедитесь в правильности типов данных
3. Проверьте обязательные поля
4. Посмотрите детали ошибок в логах

## Поддержка

### Где получить помощь?

1. Проверьте [документацию](index.md)
2. Посмотрите [примеры](tutorials/index.md)
3. Изучите [API reference](reference/index.md)
4. Создайте [issue](https://github.com/SatoryKono/bioactivity_data_acquisition/issues)

### Как сообщить об ошибке?

При создании issue укажите:

- Версию Python
- Версию проекта
- Полный текст ошибки
- Шаги для воспроизведения
- Логи (если есть)

### Как предложить улучшение?

1. Создайте issue с описанием предложения
2. Обсудите с командой
3. Создайте pull request
4. Следуйте [руководству по контрибьюшену](how-to/contribute.md)
