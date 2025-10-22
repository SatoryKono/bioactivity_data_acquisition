# Руководство по участию в разработке

Добро пожаловать в проект Bioactivity Data Acquisition! Мы ценим ваш вклад в развитие проекта.

## Как внести вклад

### 1. Подготовка окружения

```bash
# Клонирование репозитория
git clone https://github.com/your-org/bioactivity-data-acquisition.git
cd bioactivity-data-acquisition

# Создание виртуального окружения
python -m venv venv
source venv/bin/activate  # Linux/Mac
# или
venv\Scripts\activate     # Windows

# Установка зависимостей
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 2. Настройка pre-commit

```bash
# Установка pre-commit hooks
pre-commit install

# Проверка всех файлов
pre-commit run --all-files
```

### 3. Работа с Git LFS

```bash
# Установка Git LFS
git lfs install

# Проверка статуса LFS файлов
git lfs status
```

## Процесс разработки

### Создание ветки

```bash
# Создание новой ветки от main
git checkout main
git pull origin main
git checkout -b feature/your-feature-name

# Или для исправления багов
git checkout -b fix/your-bug-description
```

### Стиль кода

Проект использует следующие инструменты для обеспечения качества кода:

- **Ruff**: Линтинг и форматирование Python кода
- **Black**: Форматирование кода
- **isort**: Сортировка импортов
- **mypy**: Проверка типов

```bash
# Проверка стиля кода
ruff check src/
black src/
isort src/
mypy src/

# Автоисправление
ruff check --fix src/
```

### Структура коммитов

Используйте следующий формат для сообщений коммитов:

```text
тип(область): краткое описание

Подробное описание изменений (если необходимо)

Closes #issue_number
```

Типы коммитов:

- `feat`: новая функциональность
- `fix`: исправление бага
- `docs`: изменения в документации
- `style`: форматирование кода
- `refactor`: рефакторинг кода
- `test`: добавление или изменение тестов
- `chore`: изменения в конфигурации

Примеры:

```text
feat(testitem): add batch processing for molecule extraction
fix(chembl): resolve API timeout issues
docs(api): update client documentation
```

### Тестирование

```bash
# Запуск всех тестов
pytest

# Запуск с покрытием
pytest --cov=src/library --cov-report=html

# Запуск конкретных тестов
pytest tests/test_testitem_pipeline.py -v

# Запуск smoke тестов
pytest tests/test_*_smoke.py
```

### Создание Pull Request

1. Убедитесь, что все тесты проходят
2. Проверьте, что код соответствует стилю проекта
3. Обновите документацию при необходимости
4. Создайте Pull Request с подробным описанием изменений

Шаблон описания PR:

```markdown
## Описание изменений

Краткое описание того, что было изменено.

## Тип изменений

- [ ] Исправление бага
- [ ] Новая функциональность
- [ ] Изменения в документации
- [ ] Рефакторинг

## Чек-лист

- [ ] Код соответствует стилю проекта
- [ ] Все тесты проходят
- [ ] Документация обновлена
- [ ] Добавлены тесты для новой функциональности

## Связанные issues

Closes #issue_number
```

## Работа с данными

### Большие файлы

Для работы с большими файлами данных используется Git LFS:

```bash
# Отслеживание новых типов файлов
git lfs track "*.csv"
git lfs track "*.json"
git lfs track "*.parquet"

# Добавление атрибутов в репозиторий
git add .gitattributes
git commit -m "Add LFS tracking for data files"
```

### Тестовые данные

- Тестовые данные должны быть небольшими (< 1MB)
- Используйте синтетические данные для тестов
- Не коммитьте реальные данные пользователей

## Документация

### Обновление документации

При добавлении новой функциональности:

1. Обновите docstrings в коде
2. Добавьте примеры использования
3. Обновите API документацию
4. При необходимости добавьте руководства

### Формат docstrings

Используйте Google style docstrings:

```python
def process_molecules(molecules: List[str], batch_size: int = 50) -> List[dict]:
    """Обрабатывает список молекул в батчах.
    
    Args:
        molecules: Список идентификаторов молекул
        batch_size: Размер батча для обработки
        
    Returns:
        Список обработанных данных молекул
        
    Raises:
        ValueError: Если список молекул пуст
        APIError: При ошибке обращения к API
    """
```

## Отладка и профилирование

### Логирование

Используйте структурированное логирование:

```python
import structlog

logger = structlog.get_logger()

logger.info("Processing batch", batch_size=len(molecules), batch_id=batch_id)
logger.error("API request failed", error=str(e), retry_count=retry_count)
```

### Профилирование

Для анализа производительности:

```python
import cProfile
import pstats

# Профилирование функции
cProfile.run('your_function()', 'profile_output.prof')

# Анализ результатов
p = pstats.Stats('profile_output.prof')
p.sort_stats('cumulative').print_stats(10)
```

## Работа с API

### Обработка ошибок

```python
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_session_with_retry():
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session
```

### Rate Limiting

```python
import time
from functools import wraps

def rate_limit(calls_per_second: float):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            time.sleep(1.0 / calls_per_second)
            return func(*args, **kwargs)
        return wrapper
    return decorator

@rate_limit(10)  # 10 вызовов в секунду
def api_call():
    pass
```

## Конфигурация

### Переменные окружения

Используйте переменные окружения для конфигурации:

```python
import os
from typing import Optional

def get_api_key() -> Optional[str]:
    return os.getenv('CHEMBL_API_KEY')

def get_timeout() -> int:
    return int(os.getenv('API_TIMEOUT', '60'))
```

### Валидация конфигурации

```python
from pydantic import BaseModel, Field
from typing import Optional

class APIConfig(BaseModel):
    base_url: str = Field(..., description="Base URL for API")
    timeout: int = Field(default=60, ge=1, le=300)
    api_key: Optional[str] = Field(default=None)
    
    class Config:
        env_prefix = "API_"
```

## Дополнительные ресурсы

- Отчёт об очистке репозитория (архивный документ)
- Git LFS Workflow (архивный документ)
- [Pre-commit документация](https://pre-commit.com/)
- [Git LFS документация](https://git-lfs.github.io/)
