# Health Checking

Система проверки здоровья API клиентов позволяет мониторить доступность внешних сервисов и выявлять проблемы с подключением.

## Обзор

Health checker выполняет HTTP запросы к настроенным API для проверки их доступности. Поддерживаются различные стратегии проверки в зависимости от типа API.

## Стратегии проверки здоровья

### 1. BASE_URL (по умолчанию)
Используется базовый URL API для проверки. Подходит для внешних API, которые не имеют специального эндпоинта `/health`.

**Особенности:**
- Принимает коды ответа 200, 404, 405 как "здоровый"
- 404 и 405 часто встречаются у внешних API при HEAD запросах
- 4xx ошибки (кроме 404) считаются проблемой
- 5xx ошибки указывают на проблемы сервера

### 2. CUSTOM_ENDPOINT
Используется настраиваемый эндпоинт для проверки здоровья.

**Особенности:**
- Ожидает коды ответа 200-299 для "здорового" состояния
- Любые 4xx и 5xx ошибки считаются проблемой
- Подходит для API с специальными health endpoints

### 3. DEFAULT_HEALTH (legacy)
Использует эндпоинт `/health` (устаревший подход).

## Конфигурация

### Базовый URL (рекомендуется для внешних API)

```yaml
sources:
  crossref:
    name: crossref
    enabled: true
    http:
      base_url: https://api.crossref.org/works
      # health_endpoint не указан - используется base_url
```

### Настраиваемый эндпоинт

```yaml
sources:
  chembl:
    name: chembl
    enabled: true
    http:
      base_url: https://www.ebi.ac.uk/chembl/api/data
      health_endpoint: "status"  # Проверка по /status
```

## Использование CLI

### Проверка здоровья всех API

```bash
python -m library.cli health --config configs/config.yaml
```

### Настройка таймаута

```bash
python -m library.cli health --config configs/config.yaml --timeout 15.0
```

### JSON вывод

```bash
python -m library.cli health --config configs/config.yaml --json
```

## Интерпретация результатов

### Здоровые API
- ✅ **Healthy** - API доступен и отвечает корректно
- Время ответа в миллисекундах
- Состояние circuit breaker (если применимо)

### Нездоровые API
- ❌ **Unhealthy** - API недоступен или возвращает ошибки
- Код ошибки HTTP или описание проблемы
- Состояние circuit breaker

### Возможные ошибки

| Код | Описание | Действие |
|-----|----------|----------|
| 200-299 | Успешный ответ | ✅ Здоровый |
| 404 | Не найдено | ✅ Здоровый (для base_url) / ❌ Нездоровый (для custom) |
| 405 | Метод не разрешен | ✅ Здоровый (для base_url) / ❌ Нездоровый (для custom) |
| 4xx | Клиентская ошибка | ❌ Нездоровый |
| 5xx | Серверная ошибка | ❌ Нездоровый |
| Timeout | Таймаут соединения | ❌ Нездоровый |
| Connection Error | Ошибка соединения | ❌ Нездоровый |

## Circuit Breaker

Health checker интегрирован с circuit breaker паттерном:

- **CLOSED** - Нормальная работа
- **HALF_OPEN** - Тестирование восстановления
- **OPEN** - Сервис недоступен, запросы блокируются

## Примеры конфигурации

### Внешние API (Crossref, OpenAlex)

```yaml
sources:
  crossref:
    name: crossref
    enabled: true
    http:
      base_url: https://api.crossref.org/works
      # Используется base_url для проверки здоровья
      # 404/405 ответы считаются нормальными
```

### API с health endpoint (ChEMBL)

```yaml
sources:
  chembl:
    name: chembl
    enabled: true
    http:
      base_url: https://www.ebi.ac.uk/chembl/api/data
      health_endpoint: "status"
      # Проверка по /status, ожидается 200
```

### API с rate limiting (PubMed, Semantic Scholar)

```yaml
sources:
  pubmed:
    name: pubmed
    enabled: true
    rate_limit:
      max_calls: 2
      period: 1.0
    http:
      base_url: https://eutils.ncbi.nlm.nih.gov/entrez/eutils/
      # Используется base_url для проверки здоровья
```

## Мониторинг и алертинг

Health checker можно интегрировать в системы мониторинга:

1. **Периодические проверки** - запуск health check каждые N минут
2. **Алерты** - уведомления при недоступности критических API
3. **Метрики** - отслеживание времени ответа и доступности
4. **Дашборды** - визуализация состояния API

## Troubleshooting

### Частые проблемы

1. **404 ошибки для внешних API**
   - Решение: не указывайте `health_endpoint`, используйте `base_url`

2. **Таймауты**
   - Решение: увеличьте `timeout_sec` в конфигурации

3. **Rate limiting**
   - Решение: настройте `rate_limit` параметры

4. **Circuit breaker OPEN**
   - Решение: проверьте доступность API и настройки retry

### Отладка

```bash
# Подробный вывод
python -m library.cli health --config configs/config.yaml --timeout 30.0

# JSON для автоматической обработки
python -m library.cli health --config configs/config.yaml --json
```
