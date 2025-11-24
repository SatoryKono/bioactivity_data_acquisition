# OpenAI Client Integration

## Обзор

Интеграция OpenAI API (включая Codex) реализована через `OpenAIClient`, который наследует от `UnifiedAPIClient` и обеспечивает:

- Автоматическое управление rate limiting
- Retry логику с exponential backoff
- Circuit breaker для защиты от каскадных сбоев
- Структурированное логирование всех запросов
- Bearer token аутентификацию

## Быстрый старт

### 1. Получение API ключа

1. Зарегистрируйтесь или войдите на [platform.openai.com](https://platform.openai.com)
2. Перейдите в раздел [API Keys](https://platform.openai.com/api-keys)
3. Создайте новый Secret Key
4. Скопируйте ключ (он показывается только один раз!)

### 2. Настройка переменной окружения

Откройте файл `.env` в корне проекта и вставьте ваш ключ:

```bash
OPENAI_API_KEY=sk-proj-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

**ВАЖНО:** Файл `.env` включён в `.gitignore` и никогда не должен коммититься в репозиторий.

### 3. Использование клиента

#### Базовый пример с Codex

```python
import os
from bioetl.clients.client_openai import OpenAIClient
from bioetl.config.loader import load_config

# Загрузка конфигурации из configs/defaults/openai.yaml
config = load_config()
http_config = config.http.profiles["openai"]

# Создание клиента (API ключ берётся из переменной окружения)
client = OpenAIClient(http_config)

# Генерация кода с помощью Codex
response = client.create_completion(
    model="code-davinci-002",
    prompt="def calculate_fibonacci(n):",
    max_tokens=150,
    temperature=0.2,  # Низкая температура для более детерминированного кода
)

print(response["choices"][0]["text"])
```

#### Пример с ChatGPT

```python
from bioetl.clients.client_openai import OpenAIClient
from bioetl.config.loader import load_config

config = load_config()
http_config = config.http.profiles["openai"]
client = OpenAIClient(http_config)

# Чат-запрос
response = client.create_chat_completion(
    model="gpt-3.5-turbo",
    messages=[
        {"role": "system", "content": "You are a helpful bioinformatics assistant."},
        {"role": "user", "content": "What is the difference between EC50 and IC50?"},
    ],
    max_tokens=300,
)

print(response["choices"][0]["message"]["content"])
```

#### Получение списка доступных моделей

```python
from bioetl.clients.client_openai import OpenAIClient
from bioetl.config.loader import load_config

config = load_config()
http_config = config.http.profiles["openai"]
client = OpenAIClient(http_config)

# Получить все доступные модели
models = client.list_models()
for model in models:
    print(f"- {model['id']}")

# Информация о конкретной модели
model_info = client.retrieve_model("gpt-3.5-turbo")
print(model_info)
```

## Конфигурация

### HTTP Profile

Конфигурация для OpenAI API находится в `configs/defaults/openai.yaml`:

```yaml
http:
  profiles:
    openai:
      timeout_sec: 120.0          # Увеличенный таймаут для больших запросов
      rate_limit:
        max_calls: 3              # 3 запроса
        period: 60.0              # в минуту (для free tier)
      retries:
        total: 3                  # 3 повторных попытки
        statuses: [429, 500, 502, 503, 504]
      circuit_breaker:
        failure_threshold: 3
        timeout: 60.0
```

### Настройка Rate Limit

OpenAI использует разные лимиты в зависимости от tier:

| Tier | RPM (Requests Per Minute) | TPM (Tokens Per Minute) |
|------|---------------------------|-------------------------|
| Free | 3 | 40,000 |
| Tier 1 | 60 | 60,000 |
| Tier 2 | 3,500 | 160,000 |
| Tier 3 | 3,500 | 1,000,000 |
| Tier 4 | 10,000 | 10,000,000 |

Для настройки под ваш tier отредактируйте `configs/defaults/openai.yaml`:

```yaml
rate_limit:
  max_calls: 60      # Для Tier 1
  period: 60.0
```

## Доступные модели

### Codex (Code Generation)

- `code-davinci-002` - Наиболее продвинутая модель для генерации кода
- `code-cushman-001` - Быстрее, но менее точная

**Примечание:** OpenAI прекратила обновление моделей Codex. Рекомендуется использовать GPT-4 или GPT-3.5-turbo для генерации кода.

### GPT Models

- `gpt-4` - Самая мощная модель
- `gpt-4-turbo-preview` - Быстрее и дешевле GPT-4
- `gpt-3.5-turbo` - Быстрая и экономичная модель для большинства задач
- `gpt-3.5-turbo-instruct` - Legacy completion endpoint

Актуальный список моделей: [platform.openai.com/docs/models](https://platform.openai.com/docs/models)

## API Methods

### `create_completion()`

Создание text completion (Legacy endpoint, используется для Codex и instruct моделей).

**Параметры:**
- `model` (str) - ID модели
- `prompt` (str | list[str]) - Текстовый промпт
- `max_tokens` (int | None) - Максимум токенов в ответе
- `temperature` (float) - Температура сэмплирования (0.0-2.0)
- `top_p` (float) - Nucleus sampling параметр
- `n` (int) - Количество вариантов ответа
- `stop` (str | list[str] | None) - Стоп-последовательности
- `presence_penalty` (float) - Штраф за повторение тем
- `frequency_penalty` (float) - Штраф за повторение токенов

### `create_chat_completion()`

Создание chat completion (Для ChatGPT моделей).

**Параметры:**
- `model` (str) - ID модели
- `messages` (list[dict]) - Список сообщений с ролями
- Остальные параметры аналогичны `create_completion()`

### `list_models()`

Получение списка доступных моделей.

**Возвращает:** `list[dict]` - Список объектов моделей

### `retrieve_model(model_id: str)`

Получение информации о конкретной модели.

**Возвращает:** `dict` - Объект модели

## Обработка ошибок

Клиент автоматически обрабатывает:

- **429 Too Many Requests** - Rate limit exceeded (ретраит с backoff)
- **500/502/503/504** - Server errors (ретраит с backoff)
- **400/401/403/404** - Client errors (не считаются failure для circuit breaker)

Circuit breaker откроется после 3 consecutive failures и закроется через 60 секунд.

```python
from bioetl.clients.client_openai import OpenAIClient
from bioetl.core.http import CircuitBreakerOpenError
from requests.exceptions import HTTPError

try:
    response = client.create_completion(
        model="code-davinci-002",
        prompt="def hello():",
    )
except CircuitBreakerOpenError:
    print("Circuit breaker is open - too many failures")
except HTTPError as e:
    print(f"HTTP error: {e.response.status_code} - {e.response.text}")
```

## Логирование

Все запросы логируются через `UnifiedLogger`:

```json
{
  "event": "http.request.completed",
  "component": "openai_client",
  "client_name": "openai",
  "endpoint": "/completions",
  "model": "code-davinci-002",
  "prompt_length": 23,
  "status_code": 200,
  "duration_ms": 1234.56
}
```

## Best Practices

1. **Используйте переменные окружения** для API ключей, никогда не хардкодите
2. **Настройте rate limit** под ваш OpenAI tier
3. **Используйте температуру 0-0.3** для детерминированной генерации кода
4. **Добавляйте stop sequences** для контроля длины вывода
5. **Обрабатывайте ошибки** явно (circuit breaker, rate limits, HTTP errors)
6. **Логируйте промпты и ответы** для отладки и аудита

## Troubleshooting

### "OpenAI API key is required"

Убедитесь, что:
1. Переменная `OPENAI_API_KEY` установлена в `.env`
2. Файл `.env` находится в корне проекта
3. Ключ начинается с `sk-` или `sk-proj-`

### Rate limit exceeded (429)

Уменьшите `max_calls` в конфигурации или увеличьте `period`:

```yaml
rate_limit:
  max_calls: 1
  period: 60.0  # 1 запрос в минуту
```

### Circuit breaker открыт

Подождите 60 секунд или увеличьте `failure_threshold` в конфигурации.

## Ссылки

- [OpenAI API Documentation](https://platform.openai.com/docs/api-reference)
- [Rate Limits](https://platform.openai.com/docs/guides/rate-limits)
- [Models Overview](https://platform.openai.com/docs/models)
- [Pricing](https://openai.com/pricing)
