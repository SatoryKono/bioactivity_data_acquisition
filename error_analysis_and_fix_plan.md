# Анализ ошибок и план устранения проблем в коде

## Выявленные проблемы

### 1. **КРИТИЧЕСКАЯ: Неправильная обработка rate limiting**
**Проблема**: Rate limiter работает некорректно - при превышении лимита происходит только одно повторное обращение без ожидания.
**Код**: `src/library/clients/base.py:184-200`
```python
def _call_with_rate_limit() -> Response:
    if self.rate_limiter is not None:
        try:
            self.rate_limiter.acquire()
        except RateLimitError as e:
            # ПРОБЛЕМА: После sleep() вызывается acquire() только один раз
            time.sleep(delay)
            self.rate_limiter.acquire()  # Может снова упасть с ошибкой!
```

### 2. **КРИТИЧЕСКАЯ: Отсутствие сохранения промежуточных результатов**
**Проблема**: При прерывании pipeline все данные теряются, так как сохранение происходит только в конце.
**Код**: `src/library/assay/pipeline.py:155-166`
```python
# Process in batches
all_assay_data = {}
for i in range(0, len(assay_ids), batch_size):
    # ПРОБЛЕМА: Нет сохранения промежуточных результатов
    batch_data = chembl_client.fetch_assays_batch(batch_ids)
    all_assay_data.update(batch_data)
```

### 3. **КРИТИЧЕСКАЯ: Неправильная конфигурация rate limiting**
**Проблема**: Rate limit настроен слишком агрессивно (5 запросов за 15 секунд), что приводит к частым блокировкам.
**Конфиг**: `configs/config_assay.yaml:19-21`
```yaml
rate_limit:
  max_calls: 5                    # Слишком мало!
  period: 15.0                    # Слишком долго!
```

### 4. **СРЕДНЯЯ: Отсутствие circuit breaker для ChEMBL**
**Проблема**: Нет защиты от каскадных сбоев при проблемах с API.

### 5. **СРЕДНЯЯ: Неэффективная обработка batch ошибок**
**Проблема**: При ошибке в одном batch теряются все данные из этого batch.

## План устранения

### Этап 1: Исправление rate limiting (КРИТИЧЕСКИЙ)
**Приоритет**: 1
**Время**: 2-3 часа

1. **Исправить логику повторных попыток в rate limiter**
   - Файл: `src/library/clients/base.py`
   - Добавить цикл с повторными попытками
   - Добавить экспоненциальный backoff для rate limit

2. **Обновить конфигурацию rate limiting**
   - Файл: `configs/config_assay.yaml`
   - Увеличить max_calls до 10-15
   - Уменьшить period до 10 секунд

### Этап 2: Добавление промежуточного сохранения (КРИТИЧЕСКИЙ)
**Приоритет**: 1
**Время**: 3-4 часа

1. **Реализовать checkpoint систему**
   - Создать `src/library/common/checkpoint.py`
   - Сохранять прогресс после каждого успешного batch
   - Добавить возможность восстановления с последнего checkpoint

2. **Модифицировать assay pipeline**
   - Файл: `src/library/assay/pipeline.py`
   - Добавить сохранение промежуточных результатов
   - Добавить восстановление при перезапуске

### Этап 3: Улучшение обработки ошибок (ВЫСОКИЙ)
**Приоритет**: 2
**Время**: 2-3 часа

1. **Добавить circuit breaker для ChEMBL**
   - Файл: `src/library/clients/chembl.py`
   - Интегрировать circuit breaker из base client
   - Настроить пороги срабатывания

2. **Улучшить обработку batch ошибок**
   - Файл: `src/library/assay/pipeline.py`
   - Добавить retry для отдельных batches
   - Сохранять частично успешные batches

### Этап 4: Оптимизация производительности (СРЕДНИЙ)
**Приоритет**: 3
**Время**: 2-3 часа

1. **Динамическая настройка batch size**
   - Адаптивный размер batch в зависимости от rate limit
   - Уменьшение batch size при частых ошибках

2. **Параллельная обработка с ограничениями**
   - Добавить worker pool с rate limiting
   - Контролируемый параллелизм

### Этап 5: Мониторинг и логирование (СРЕДНИЙ)
**Приоритет**: 3
**Время**: 1-2 часа

1. **Улучшить логирование**
   - Добавить метрики производительности
   - Детальное логирование rate limiting событий

2. **Добавить мониторинг прогресса**
   - Прогресс-бар для CLI
   - Статистика в реальном времени

## Детальный план исправления rate limiting

### Проблемный код:
```python
def _call_with_rate_limit() -> Response:
    if self.rate_limiter is not None:
        try:
            self.rate_limiter.acquire()
        except RateLimitError as e:
            error_msg = str(e).replace("%", "%%")
            self.logger.warning("Rate limit hit: %s", error_msg)
            if hasattr(self.config, 'rate_limit') and self.config.rate_limit:
                delay = self.config.rate_limit.period + random.uniform(0, 1)
            else:
                delay = 5.0 + random.uniform(0, 1)
            time.sleep(delay)
            self.rate_limiter.acquire()  # ПРОБЛЕМА: Может снова упасть!
    
    return _call()
```

### Исправленный код:
```python
def _call_with_rate_limit() -> Response:
    if self.rate_limiter is not None:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.rate_limiter.acquire()
                break  # Успешно получили разрешение
            except RateLimitError as e:
                if attempt == max_retries - 1:
                    raise  # Последняя попытка, пробрасываем ошибку
                
                error_msg = str(e).replace("%", "%%")
                self.logger.warning("Rate limit hit (attempt %d/%d): %s", 
                                  attempt + 1, max_retries, error_msg)
                
                # Экспоненциальный backoff для rate limiting
                base_delay = self.config.rate_limit.period if hasattr(self.config, 'rate_limit') else 5.0
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                self.logger.info("Waiting %.1f seconds before retry...", delay)
                time.sleep(delay)
    
    return _call()
```

## Ожидаемые результаты

После исправления:
1. **Стабильная работа** - pipeline не будет прерываться из-за rate limiting
2. **Восстановимость** - возможность продолжить с места остановки
3. **Полные данные** - все 100 ассеев будут обработаны
4. **Лучшая производительность** - оптимизированные настройки rate limiting

## Временные рамки

- **Этап 1-2**: 1-2 дня (критические исправления)
- **Этап 3-5**: 3-5 дней (улучшения)
- **Тестирование**: 1-2 дня
- **Общее время**: 1-2 недели
