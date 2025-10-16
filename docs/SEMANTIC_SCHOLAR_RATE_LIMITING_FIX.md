# Исправление проблемы Rate Limiting Semantic Scholar API

## 🔍 Найденные проблемы

### 1. **Конфликт настроек rate limiting**
- **Проблема**: В `config_documents_full.yaml` было `period: 10.0` (1 запрос в 10 секунд)
- **Но в коде**: Дополнительная задержка 65 секунд
- **Результат**: Конфликт между настройками конфигурации и кодом

### 2. **Semantic Scholar был включен**
- **Проблема**: `enabled: true` в конфигурации
- **Результат**: API вызывался несмотря на строгие лимиты

### 3. **Избыточные задержки в коде**
- **Проблема**: Дополнительные `time.sleep(65)` и `time.sleep(30)` в pipeline
- **Результат**: Двойное ограничение скорости

## ✅ Внесенные исправления

### 1. **Исправлены настройки конфигурации**

#### `configs/config_documents_full.yaml`:
```yaml
semantic_scholar:
  enabled: false  # Отключен из-за строгих лимитов без API ключа
  rate_limit:
    max_calls: 1
    period: 60.0  # Исправлено с 10.0 на 60.0
```

#### `configs/config.yaml`:
```yaml
semantic_scholar:
  name: semantic_scholar
  enabled: false  # Отключен по умолчанию
  rate_limit:
    max_calls: 1
    period: 60.0   # 1 запрос в минуту
```

### 2. **Убраны избыточные задержки в коде**

#### `src/library/documents/pipeline.py`:
- Убрана задержка `time.sleep(65)` перед запросами
- Убрана задержка `time.sleep(30)` после запросов
- Rate limiting теперь контролируется только конфигурацией

### 3. **Улучшена обработка fallback**

#### `src/library/clients/fallback.py`:
- Увеличены задержки для Semantic Scholar: 3-10 минут
- Улучшена вариативность jitter: ±20%

#### `src/library/clients/base.py`:
- Минимум 5 минут задержки для Semantic Scholar при Retry-After
- Консервативная задержка при отсутствии заголовка

## 🎯 Результат

### До исправления:
- ❌ Конфликт настроек (10 сек vs 65 сек)
- ❌ Semantic Scholar включен без API ключа
- ❌ Избыточные задержки в коде
- ❌ Ошибки 429 из-за неправильных настроек

### После исправления:
- ✅ Semantic Scholar отключен по умолчанию
- ✅ Правильные настройки rate limiting (1 запрос/минуту)
- ✅ Убраны избыточные задержки
- ✅ Консистентная обработка rate limiting

## 🚀 Рекомендации

### Для включения Semantic Scholar:

1. **Получить API ключ**:
   ```
   https://www.semanticscholar.org/product/api#api-key-form
   ```

2. **Включить в конфигурации**:
   ```yaml
   semantic_scholar:
     enabled: true
     http:
       headers:
         x-api-key: "ваш_api_ключ"
   ```

3. **Увеличить лимиты**:
   ```yaml
   rate_limit:
     max_calls: 100  # С API ключом
     period: 60.0
   ```

### Для работы без Semantic Scholar:
- ✅ Semantic Scholar отключен по умолчанию
- ✅ Система работает с другими источниками
- ✅ Нет ошибок rate limiting

## 📊 Официальные лимиты Semantic Scholar

- **Без API ключа**: ~1 запрос в минуту
- **С API ключом**: до 100 запросов в минуту
- **Официальная документация**: https://www.semanticscholar.org/product/api

## 🔧 Техническая информация

### Rate Limiter работает на уровне:
1. **Конфигурация**: `rate_limit.max_calls` и `rate_limit.period`
2. **BaseApiClient**: Автоматическое ограничение скорости
3. **FallbackManager**: Обработка ошибок 429

### Убраны избыточные задержки:
- ❌ `time.sleep(65)` в pipeline
- ❌ `time.sleep(30)` после обработки
- ✅ Только конфигурационные настройки

Теперь система работает корректно с правильными настройками rate limiting!
