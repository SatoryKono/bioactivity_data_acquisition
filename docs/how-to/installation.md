# Установка

Установить и настроить Bioactivity Data Acquisition в вашей среде разработки.

## Предпосылки

Перед выполнением убедитесь, что у вас есть:

- Python 3.11+ установлен
- pip обновлён до последней версии
- Доступ к интернету для загрузки зависимостей

## Решение

### Шаг 1: Клонирование репозитория

```bash
git clone https://github.com/SatoryKono/bioactivity_data_acquisition.git
cd bioactivity_data_acquisition
```

### Шаг 2: Создание виртуального окружения

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# или
venv\Scripts\activate     # Windows
```

### Шаг 3: Установка зависимостей

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
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

## Проверка работоспособности

### Тестирование скриптов

```bash
python -m src.scripts.get_testitem_data --help
```

### Запуск тестового пайплайна

```bash
python -m src.scripts.get_testitem_data --config configs/config_test.yaml --input data/input/testitem.csv --output data/output/
```

### Запуск полного пайплайна

```bash
python -m src.scripts.get_target_data --config configs/config_target_full.yaml --input data/input/target.csv --output data/output/
```

```bash
python -m src.scripts.get_activity_data --config configs/config_activity_full.yaml --input data/input/activity.csv --output data/output/
```

```bash
python -m src.scripts.get_assay_data --config configs/config_assay_full.yaml --input data/input/assay.csv --output data/output/
```

## Устранение неполадок

### Проблемы с зависимостями

Если возникают проблемы с установкой зависимостей:

1. Обновите pip: `pip install --upgrade pip`
2. Очистите кэш: `pip cache purge`
3. Переустановите зависимости: `pip install -r requirements.txt --force-reinstall`

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

## Дополнительная настройка

### Настройка IDE

Рекомендуемые расширения для VS Code:

- Python
- Pylance
- Black Formatter
- isort

### Настройка переменных окружения

Создайте файл `.env` в корне проекта:

```bash
# API ключи
CHEMBL_API_KEY=your_chembl_api_key
PUBCHEM_API_KEY=your_pubchem_api_key

# Настройки
API_TIMEOUT=60
LOG_LEVEL=INFO
```

### Настройка логирования

Отредактируйте `configs/logging.yaml` для настройки логирования под ваши потребности.
