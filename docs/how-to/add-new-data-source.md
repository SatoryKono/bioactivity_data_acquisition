# Как добавить новый источник данных

Это руководство описывает процесс добавления нового источника данных в пайплайн.

## 1. Создание API клиента

Сначала необходимо создать новый API клиент для взаимодействия с новым источником данных.

1.  **Создайте новый файл** в директории `src/library/clients/` с названием вашего нового клиента, например, `mynewsource_client.py`.
2.  **Определите класс клиента**, который наследуется от `BaseApiClient`.

```python
# src/library/clients/mynewsource_client.py
from .base import BaseApiClient

class MyNewSourceClient(BaseApiClient):
    def __init__(self, base_url="https://api.mynewsource.com"):
        super().__init__(base_url)

    def get_data(self, some_id: str):
        """
        Пример метода для получения данных.
        """
        endpoint = f"/data/{some_id}"
        return self._get(endpoint)
```

## 2. Интеграция клиента в пайплайн

После создания клиента его необходимо интегрировать в существующий или новый ETL пайплайн.

1.  **Откройте файл пайплайна**, в который вы хотите добавить новый источник, например, `src/library/documents/pipeline.py`.
2.  **Импортируйте ваш новый клиент** и добавьте логику для его использования.

```python
# src/library/documents/pipeline.py
from ..clients.mynewsource_client import MyNewSourceClient

# ... в методе extract ...
def extract(self):
    # ...
    mynewsource_client = MyNewSourceClient()
    new_data = mynewsource_client.get_data("some_id")
    # ...
```

## 3. Обновление конфигурации

Наконец, добавьте новый источник в конфигурацию.

1.  **Откройте `configs/config.yaml`** (или другой релевантный файл конфигурации).
2.  **Добавьте параметры** для нового источника.

```yaml
# configs/config.yaml
sources:
  # ...
  mynewsource:
    enabled: true
    api_key: "${MYNEWSOURCE_API_KEY}" # Пример использования переменной окружения
```

## 4. Запуск пайплайна

Теперь вы можете запустить пайплайн с новым источником данных.

```bash
make run ENTITY=documents CONFIG=configs/config_documents_full.yaml
```
