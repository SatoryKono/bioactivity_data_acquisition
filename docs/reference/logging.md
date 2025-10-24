# Logging & Metrics

## Обзор

Система логирования и метрик для ETL пайплайнов bioactivity_data_acquisition. Обеспечивает структурированное логирование, мониторинг производительности и телеметрию.

## Структурированное логирование

### Уровни логирования
```yaml
log_levels:
  DEBUG: "Детальная отладочная информация"
  INFO: "Общая информация о процессе"
  WARNING: "Предупреждения о потенциальных проблемах"
  ERROR: "Ошибки, не останавливающие выполнение"
  CRITICAL: "Критические ошибки, останавливающие выполнение"
```

### Формат логов
```yaml
log_format:
  timestamp: "%Y-%m-%dT%H:%M:%S.%fZ"
  level: "INFO"
  logger: "library.documents.extract"
  message: "Extracted 1500 documents from ChEMBL"
  context:
    pipeline: "documents"
    source: "chembl"
    records_processed: 1500
    duration_sec: 45.2
    api_calls: 15
    cache_hits: 12
    cache_misses: 3
```

### Примеры логов
```json
{
  "timestamp": "2025-10-24T14:30:22.123Z",
  "level": "INFO",
  "logger": "library.documents.extract",
  "message": "Starting document extraction",
  "context": {
    "pipeline": "documents",
    "config_file": "configs/config_document.yaml",
    "input_file": "data/input/documents.csv",
    "total_records": 1500
  }
}

{
  "timestamp": "2025-10-24T14:30:45.456Z",
  "level": "INFO",
  "logger": "library.clients.chembl",
  "message": "API request completed",
  "context": {
    "source": "chembl",
    "endpoint": "/document/CHEMBL123456",
    "status_code": 200,
    "duration_ms": 1250,
    "response_size_bytes": 2048,
    "cache_hit": false
  }
}

{
  "timestamp": "2025-10-24T14:31:15.789Z",
  "level": "WARNING",
  "logger": "library.clients.crossref",
  "message": "Rate limit approaching",
  "context": {
    "source": "crossref",
    "requests_per_second": 45,
    "limit": 50,
    "remaining_requests": 1000
  }
}

{
  "timestamp": "2025-10-24T14:32:00.012Z",
  "level": "ERROR",
  "logger": "library.clients.semantic_scholar",
  "message": "API request failed",
  "context": {
    "source": "semantic_scholar",
    "endpoint": "/paper/search",
    "status_code": 429,
    "error_message": "Rate limit exceeded",
    "retry_count": 3,
    "max_retries": 5
  }
}
```

## Конфигурация логирования

### YAML конфигурация
```yaml
# configs/logging.yaml
logging:
  level: INFO
  format: json
  handlers:
    console:
      enabled: true
      level: INFO
      format: text
    file:
      enabled: true
      level: DEBUG
      path: logs/app.log
      max_bytes: 10485760  # 10MB
      backup_count: 10
      rotation_strategy: size
    structured:
      enabled: true
      level: INFO
      path: logs/structured.json
      format: json
  loggers:
    library:
      level: DEBUG
      propagate: false
    library.clients:
      level: INFO
    library.normalize:
      level: DEBUG
    library.schemas:
      level: WARNING
```

### Python конфигурация
```python
# src/library/logging/config.py
import logging
import logging.config
from pathlib import Path

def setup_logging(config: dict):
    """Настройка системы логирования"""
    
    # Создание директории для логов
    log_dir = Path(config['logging']['handlers']['file']['path']).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Конфигурация логирования
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'json': {
                'format': '%(asctime)s %(name)s %(levelname)s %(message)s',
                'class': 'pythonjsonlogger.jsonlogger.JsonFormatter'
            },
            'text': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': config['logging']['handlers']['console']['level'],
                'formatter': 'text',
                'stream': 'ext://sys.stdout'
            },
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'level': config['logging']['handlers']['file']['level'],
                'formatter': 'json',
                'filename': config['logging']['handlers']['file']['path'],
                'maxBytes': config['logging']['handlers']['file']['max_bytes'],
                'backupCount': config['logging']['handlers']['file']['backup_count']
            }
        },
        'loggers': {
            'library': {
                'level': config['logging']['loggers']['library']['level'],
                'handlers': ['console', 'file'],
                'propagate': False
            }
        }
    })
```

## Ключевые метрики

### API метрики
```yaml
api_metrics:
  request_count:
    description: "Общее количество API запросов"
    labels: ["source", "endpoint", "status_code"]
    aggregation: "counter"
  
  request_duration:
    description: "Время выполнения API запросов"
    labels: ["source", "endpoint"]
    aggregation: "histogram"
    buckets: [0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
  
  cache_hit_rate:
    description: "Процент попаданий в кэш"
    labels: ["source", "endpoint"]
    aggregation: "gauge"
  
  rate_limit_hits:
    description: "Количество срабатываний rate limit"
    labels: ["source"]
    aggregation: "counter"
  
  error_rate:
    description: "Процент ошибок API"
    labels: ["source", "endpoint", "error_type"]
    aggregation: "gauge"
```

### Pipeline метрики
```yaml
pipeline_metrics:
  records_processed:
    description: "Количество обработанных записей"
    labels: ["pipeline", "stage"]
    aggregation: "counter"
  
  processing_time:
    description: "Время обработки пайплайна"
    labels: ["pipeline"]
    aggregation: "histogram"
  
  memory_usage:
    description: "Использование памяти"
    labels: ["pipeline", "stage"]
    aggregation: "gauge"
  
  quality_score:
    description: "Оценка качества данных"
    labels: ["pipeline", "metric"]
    aggregation: "gauge"
```

### Quality метрики
```yaml
quality_metrics:
  fill_rate:
    description: "Процент заполненных полей"
    labels: ["pipeline", "table", "column"]
    aggregation: "gauge"
  
  duplicate_rate:
    description: "Процент дубликатов"
    labels: ["pipeline", "table"]
    aggregation: "gauge"
  
  validation_errors:
    description: "Количество ошибок валидации"
    labels: ["pipeline", "table", "error_type"]
    aggregation: "counter"
  
  schema_compliance:
    description: "Соответствие схеме данных"
    labels: ["pipeline", "table"]
    aggregation: "gauge"
```

## Мониторинг в реальном времени

### Dashboard метрики
```yaml
dashboard:
  overview:
    - "total_requests_per_minute"
    - "average_response_time"
    - "error_rate"
    - "cache_hit_rate"
  
  pipeline_status:
    - "documents_pipeline_status"
    - "targets_pipeline_status"
    - "assays_pipeline_status"
    - "activities_pipeline_status"
    - "testitems_pipeline_status"
  
  api_health:
    - "chembl_api_status"
    - "crossref_api_status"
    - "openalex_api_status"
    - "pubmed_api_status"
    - "semantic_scholar_api_status"
```

### Алерты
```yaml
alerts:
  high_error_rate:
    condition: "error_rate > 0.1"
    duration: "5m"
    severity: "warning"
    message: "High error rate detected"
  
  api_down:
    condition: "api_status == 'down'"
    duration: "1m"
    severity: "critical"
    message: "API is down"
  
  rate_limit_exceeded:
    condition: "rate_limit_hits > 0"
    duration: "1m"
    severity: "warning"
    message: "Rate limit exceeded"
  
  low_cache_hit_rate:
    condition: "cache_hit_rate < 0.8"
    duration: "10m"
    severity: "warning"
    message: "Low cache hit rate"
```

## Экспорт метрик

### Prometheus формат
```python
# src/library/metrics/prometheus.py
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Счётчики
api_requests_total = Counter(
    'api_requests_total',
    'Total API requests',
    ['source', 'endpoint', 'status_code']
)

# Гистограммы
api_request_duration = Histogram(
    'api_request_duration_seconds',
    'API request duration',
    ['source', 'endpoint'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
)

# Gauge метрики
cache_hit_rate = Gauge(
    'cache_hit_rate',
    'Cache hit rate',
    ['source', 'endpoint']
)

def start_metrics_server(port: int = 8000):
    """Запуск HTTP сервера для метрик"""
    start_http_server(port)
```

### OpenTelemetry интеграция
```python
# src/library/metrics/otel.py
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

def setup_otel_tracing():
    """Настройка OpenTelemetry трейсинга"""
    
    # Создание tracer provider
    trace.set_tracer_provider(TracerProvider())
    tracer = trace.get_tracer(__name__)
    
    # Настройка экспорта
    otlp_exporter = OTLPSpanExporter(
        endpoint="http://jaeger:14250",
        insecure=True
    )
    
    # Добавление span processor
    span_processor = BatchSpanProcessor(otlp_exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)
    
    return tracer
```

## Логирование по компонентам

### API клиенты
```python
# src/library/clients/base.py
import logging

logger = logging.getLogger('library.clients')

class BaseAPIClient:
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(f'library.clients.{self.name}')
    
    def make_request(self, endpoint: str, params: dict = None):
        """Выполнение API запроса с логированием"""
        
        start_time = time.time()
        
        try:
            self.logger.info(
                "Making API request",
                extra={
                    'source': self.name,
                    'endpoint': endpoint,
                    'params': params
                }
            )
            
            response = self._make_request(endpoint, params)
            
            duration = time.time() - start_time
            
            self.logger.info(
                "API request completed",
                extra={
                    'source': self.name,
                    'endpoint': endpoint,
                    'status_code': response.status_code,
                    'duration_ms': duration * 1000,
                    'response_size_bytes': len(response.content),
                    'cache_hit': False
                }
            )
            
            return response
            
        except Exception as e:
            duration = time.time() - start_time
            
            self.logger.error(
                "API request failed",
                extra={
                    'source': self.name,
                    'endpoint': endpoint,
                    'error': str(e),
                    'duration_ms': duration * 1000
                }
            )
            
            raise
```

### Нормализация данных
```python
# src/library/normalize/base.py
import logging

logger = logging.getLogger('library.normalize')

class BaseNormalizer:
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(f'library.normalize.{self.__class__.__name__}')
    
    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Нормализация данных с логированием"""
        
        self.logger.info(
            "Starting data normalization",
            extra={
                'pipeline': self.pipeline_name,
                'input_rows': len(df),
                'input_columns': len(df.columns)
            }
        )
        
        # Нормализация
        normalized_df = self._normalize(df)
        
        # Подсчёт изменений
        changes = self._count_changes(df, normalized_df)
        
        self.logger.info(
            "Data normalization completed",
            extra={
                'pipeline': self.pipeline_name,
                'output_rows': len(normalized_df),
                'output_columns': len(normalized_df.columns),
                'changes': changes
            }
        )
        
        return normalized_df
```

### Валидация схем
```python
# src/library/schemas/base.py
import logging

logger = logging.getLogger('library.schemas')

class BaseSchema:
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(f'library.schemas.{self.__class__.__name__}')
    
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидация данных с логированием"""
        
        self.logger.info(
            "Starting schema validation",
            extra={
                'schema': self.__class__.__name__,
                'rows': len(df),
                'columns': len(df.columns)
            }
        )
        
        try:
            validated_df = self._validate(df)
            
            self.logger.info(
                "Schema validation passed",
                extra={
                    'schema': self.__class__.__name__,
                    'validated_rows': len(validated_df)
                }
            )
            
            return validated_df
            
        except Exception as e:
            self.logger.error(
                "Schema validation failed",
                extra={
                    'schema': self.__class__.__name__,
                    'error': str(e),
                    'error_type': type(e).__name__
                }
            )
            
            raise
```

## Анализ логов

### Структурированные запросы
```python
# src/library/logging/analysis.py
import json
from pathlib import Path

def analyze_logs(log_file: Path) -> dict:
    """Анализ логов для извлечения метрик"""
    
    metrics = {
        'total_requests': 0,
        'successful_requests': 0,
        'failed_requests': 0,
        'average_response_time': 0,
        'cache_hit_rate': 0,
        'error_rate': 0
    }
    
    with open(log_file, 'r') as f:
        for line in f:
            try:
                log_entry = json.loads(line)
                
                # Подсчёт запросов
                if 'context' in log_entry and 'source' in log_entry['context']:
                    metrics['total_requests'] += 1
                    
                    if log_entry['level'] == 'INFO':
                        metrics['successful_requests'] += 1
                    elif log_entry['level'] == 'ERROR':
                        metrics['failed_requests'] += 1
                    
                    # Время ответа
                    if 'duration_ms' in log_entry['context']:
                        metrics['average_response_time'] += log_entry['context']['duration_ms']
                    
                    # Cache hit rate
                    if 'cache_hit' in log_entry['context']:
                        if log_entry['context']['cache_hit']:
                            metrics['cache_hit_rate'] += 1
                
            except json.JSONDecodeError:
                continue
    
    # Нормализация метрик
    if metrics['total_requests'] > 0:
        metrics['average_response_time'] /= metrics['total_requests']
        metrics['cache_hit_rate'] /= metrics['total_requests']
        metrics['error_rate'] = metrics['failed_requests'] / metrics['total_requests']
    
    return metrics
```

### Генерация отчётов
```python
def generate_log_report(log_file: Path, output_file: Path):
    """Генерация отчёта по логам"""
    
    metrics = analyze_logs(log_file)
    
    report = {
        'summary': {
            'total_requests': metrics['total_requests'],
            'success_rate': 1 - metrics['error_rate'],
            'average_response_time_ms': metrics['average_response_time'],
            'cache_hit_rate': metrics['cache_hit_rate']
        },
        'recommendations': []
    }
    
    # Рекомендации
    if metrics['error_rate'] > 0.1:
        report['recommendations'].append(
            "High error rate detected. Check API health and rate limits."
        )
    
    if metrics['cache_hit_rate'] < 0.8:
        report['recommendations'].append(
            "Low cache hit rate. Consider increasing cache TTL."
        )
    
    if metrics['average_response_time'] > 5000:
        report['recommendations'].append(
            "Slow API responses. Consider optimizing requests."
        )
    
    # Сохранение отчёта
    with open(output_file, 'w') as f:
        json.dump(report, f, indent=2)
```

## Лучшие практики

### 1. Структурированное логирование
- Используйте JSON формат для машинного анализа
- Включайте контекстную информацию
- Логируйте на правильном уровне

### 2. Производительность
- Избегайте логирования в горячих путях
- Используйте асинхронное логирование
- Ротируйте файлы логов

### 3. Безопасность
- Не логируйте чувствительные данные
- Маскируйте API ключи
- Ограничьте доступ к логам

### 4. Мониторинг
- Настройте алерты на критические события
- Отслеживайте тренды метрик
- Анализируйте логи регулярно

### 5. Отладка
- Логируйте достаточно информации для отладки
- Используйте корреляционные ID
- Включайте стек-трейсы для ошибок
