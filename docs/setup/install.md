# Установка

Установить и настроить Bioactivity Data Acquisition в вашей среде разработки.

## Предпосылки

Перед выполнением убедитесь, что у вас есть:

- Python 3.10+ установлен
- pip обновлён до последней версии
- Доступ к интернету для загрузки зависимостей

## Установка из репозитория

### Шаг 1: Клонирование репозитория

```bash
git clone https://github.com/SatoryKono/bioactivity_data_acquisition.git
cd bioactivity_data_acquisition
```

### Шаг 2: Создание виртуального окружения

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# или
.venv\Scripts\activate     # Windows
```

### Шаг 3: Установка зависимостей

#### Базовая установка

```bash
pip install .
```

#### Установка с dev зависимостями

```bash
pip install .[dev]
```

#### Установка с документацией

```bash
pip install .[dev,docs]
```

### Шаг 4: Настройка pre-commit hooks

```bash
pre-commit install
pre-commit run --all-files
```

### Шаг 5: Настройка Git LFS

```bash
git lfs install
```

### Шаг 6: Конфигурация

```bash
cp configs/config.example.yaml configs/config.yaml
```

Отредактируйте `configs/config.yaml` в соответствии с вашими потребностями.

### Шаг 7: Проверка установки

```bash
pytest tests/
```

Если все тесты проходят, установка выполнена успешно.

## Установка через pip

### Из GitHub (рекомендуется)

```bash
pip install git+https://github.com/SatoryKono/bioactivity_data_acquisition.git
```

### С dev зависимостями

```bash
pip install git+https://github.com/SatoryKono/bioactivity_data_acquisition.git#egg=bioactivity-data-acquisition[dev]
```

## Docker установка

### Сборка образа

```bash
docker build -t bioactivity-data-acquisition .
```

### Запуск контейнера

```bash
docker run --rm -v $(pwd)/data:/app/data bioactivity-data-acquisition
```

### Docker Compose

```bash
docker-compose up
```

## Проверка работоспособности

### Тестирование CLI

```bash
bioactivity-data-acquisition --help
```

### Тестирование пайплайнов

```bash
# Documents
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --limit 3

# Targets
make -f Makefile.target target-example

# Assays
make -f Makefile.assay assay-example

# Activities
python scripts/get_activity_data.py --config configs/config_activity_full.yaml --limit 10

# Testitems
python -m library.cli testitem-run --config configs/config_testitem_full.yaml --input data/input/testitem.csv
```

## Устранение неполадок

### Проблемы с зависимостями

Если возникают проблемы с установкой зависимостей:

1. Обновите pip: `pip install --upgrade pip`
2. Очистите кэш: `pip cache purge`
3. Переустановите зависимости: `pip install .[dev] --force-reinstall`

### Проблемы с Git LFS

Если Git LFS не работает корректно:

```bash
git lfs pull
git lfs status
```

### Проблемы с pre-commit

Если pre-commit hooks не работают:

```bash
pre-commit uninstall
pre-commit install
pre-commit run --all-files
```

### Проблемы с Docker

Если Docker не запускается:

```bash
# Проверка образа
docker images | grep bioactivity

# Пересборка
docker build --no-cache -t bioactivity-data-acquisition .

# Проверка volumes
docker run --rm -v $(pwd)/data:/app/data -v $(pwd)/configs:/app/configs bioactivity-data-acquisition ls -la /app
```

## Дополнительная настройка

### Настройка IDE

Рекомендуемые расширения для VS Code:

- Python
- Pylance
- Black Formatter
- isort
- Markdown All in One

### Настройка переменных окружения

Создайте файл `.env` в корне проекта на основе `.env.example`:

```bash
# API ключи (опционально)
CHEMBL_API_TOKEN=your_chembl_token_here
PUBMED_API_KEY=your_pubmed_key_here
SEMANTIC_SCHOLAR_API_KEY=your_semantic_scholar_key_here

# Конфигурация окружения
ENV=development
LOG_LEVEL=INFO
```

### Настройка логирования

Отредактируйте `configs/logging.yaml` для настройки логирования под ваши потребности.

### Настройка API ключей

Используйте встроенную утилиту:

```bash
# Windows
make setup-api-keys

# Linux/Mac
python scripts/setup_api_keys.py
```

## Следующие шаги

После успешной установки:

1. Изучите [конфигурацию](config.md)
2. Запустите [быстрый старт](../tutorials/quickstart.md)
3. Ознакомьтесь с [пайплайнами](../pipelines/)
4. Изучите [операции](../operations/)
