# Правильные настройки Semantic Scholar API

## ✅ Текущие настройки

Semantic Scholar API настроен с правильными лимитами:

### Конфигурация rate limiting:

```

semantic*scholar:
  name: semantic*scholar
  enabled: true  # Всегда включен
  rate*limit:
    max*calls: 1   # 1 запрос в 5 секунд
    period: 5.0    # Период в секундах (5 секунд)

```

## 📊 Официальные лимиты Semantic Scholar

- **Без API ключа**: 1 запрос в 5 секунд

- **С API ключом**: до 100 запросов в минуту

- **Официальная документация**: [https://www.semanticscholar.org/product/api](https://www.semanticscholar.org/product/api)

## 🔧 Техническая реализация

### Rate Limiter работает на уровне:

1. **Конфигурация**:`rate*limit.max*calls: 1`и`rate*limit.period: 5.0`2. **BaseApiClient**:
Автоматическое ограничение скорости
3. **FallbackManager**: Обработка ошибок 429 с улучшенными задержками

### Убраны избыточные задержки:

- ❌`time.sleep(65)`в pipeline (убрано)

- ❌`time.sleep(30)`после обработки (убрано)

- ✅ Только конфигурационные настройки (5 секунд)

## 🚀 Для увеличения лимитов

### Получить API ключ:

1. Перейдите на:
[https://www.semanticscholar.org/product/api#api-key-form](https://www.semanticscholar.org/product/api#api-key-form)
2. Заполните форму запроса API ключа
3. После получения ключа добавьте его в конфигурацию:

```

semantic*scholar:
  enabled: true
  rate*limit:
    max*calls: 100  # С API ключом
    period: 60.0    # 100 запросов в минуту
  http:
    headers:
      x-api-key: "ваш*api*ключ*здесь"

```

## 📈 Мониторинг

Система автоматически логирует:

- ✅ Количество успешных запросов

- ⚠️ Ошибки rate limiting (если возникают)

- 💡 Рекомендации по получению API ключа

- 🔄 Использование fallback данных

## 🎯 Результат

### Текущие настройки:

- ✅ Semantic Scholar всегда включен

- ✅ Правильные лимиты: 1 запрос в 5 секунд

- ✅ Убраны конфликтующие задержки

- ✅ Консистентная обработка rate limiting

### Производительность:

- **Без API ключа**: ~12 запросов в минуту

- **С API ключом**: до 100 запросов в минуту

- **Fallback**: Автоматическое использование резервных данных при ошибках

## 🔧 Конфигурационные файлы

### `configs/config.yaml`:

```

semantic*scholar:
  name: semantic*scholar
  enabled: true
  rate*limit:
    max*calls: 1
    period: 5.0

```

### `configs/config*documents*full.yaml`:

```

semantic*scholar:
  name: semantic*scholar
  enabled: true
  rate*limit:
    max_calls: 1
    period: 5.0

```

## ✅ Заключение

Semantic Scholar API теперь настроен с правильными лимитами (1 запрос в 5
секунд) и всегда включен. Система работает стабильно без ошибок rate limiting
при соблюдении этих настроек.
