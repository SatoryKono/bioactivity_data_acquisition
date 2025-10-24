# Acceptance Criteria

## Обзор

Критерии приемки для ETL пайплайнов bioactivity_data_acquisition. Определяют минимальные требования к качеству данных, производительности и функциональности.

## Общие критерии

### 1. Покрытие пайплайнов
- [ ] **Documents Pipeline** - полностью документирован и протестирован
- [ ] **Targets Pipeline** - полностью документирован и протестирован  
- [ ] **Assays Pipeline** - полностью документирован и протестирован
- [ ] **Activities Pipeline** - полностью документирован и протестирован
- [ ] **Testitems Pipeline** - полностью документирован и протестирован

### 2. Документация
- [ ] **Отсутствие битых ссылок** в mkdocs.yml и markdown файлах
- [ ] **Актуальность контента** - все примеры кода работают
- [ ] **Полнота описаний** - все API endpoints задокументированы
- [ ] **Структурированность** - логичная навигация и группировка

### 3. Воспроизводимость
- [ ] **Детерминированный экспорт** - идентичные checksums при повторных запусках
- [ ] **Стабильная сортировка** - одинаковый порядок строк
- [ ] **Фиксированный порядок колонок** - соответствует конфигурации
- [ ] **Консистентные форматы** - UTF-8, UTC, ISO 8601

## Критерии качества данных

### 1. Fill Rate (Заполненность)
```yaml
fill_rate_criteria:
  documents:
    minimum: 0.95
    critical_fields: ["document_chembl_id", "title"]
    warning_threshold: 0.90
  
  targets:
    minimum: 0.90
    critical_fields: ["target_chembl_id", "pref_name"]
    warning_threshold: 0.85
  
  assays:
    minimum: 0.95
    critical_fields: ["assay_chembl_id", "target_chembl_id"]
    warning_threshold: 0.90
  
  activities:
    minimum: 0.98
    critical_fields: ["activity_chembl_id", "standard_value", "standard_units"]
    warning_threshold: 0.95
  
  testitems:
    minimum: 0.90
    critical_fields: ["molecule_chembl_id", "pref_name"]
    warning_threshold: 0.85
```

### 2. Duplicate Rate (Дубликаты)
```yaml
duplicate_rate_criteria:
  documents:
    maximum: 0.01
    key_fields: ["document_chembl_id", "doi", "pmid"]
  
  targets:
    maximum: 0.005
    key_fields: ["target_chembl_id", "uniprot_accession"]
  
  assays:
    maximum: 0.01
    key_fields: ["assay_chembl_id"]
  
  activities:
    maximum: 0.005
    key_fields: ["activity_chembl_id"]
  
  testitems:
    maximum: 0.01
    key_fields: ["molecule_chembl_id", "pubchem_cid"]
```

### 3. Validation Errors (Ошибки валидации)
```yaml
validation_error_criteria:
  schema_compliance:
    minimum: 1.0
    description: "100% соответствие Pandera схемам"
  
  data_type_errors:
    maximum: 0.01
    description: "Менее 1% ошибок типов данных"
  
  format_errors:
    maximum: 0.05
    description: "Менее 5% ошибок формата"
  
  range_errors:
    maximum: 0.02
    description: "Менее 2% ошибок диапазонов"
```

## Критерии производительности

### 1. Время выполнения
```yaml
performance_criteria:
  documents_pipeline:
    maximum_duration: 1800  # 30 минут
    typical_duration: 900   # 15 минут
    records_per_minute: 1000
  
  targets_pipeline:
    maximum_duration: 600   # 10 минут
    typical_duration: 300   # 5 минут
    records_per_minute: 500
  
  assays_pipeline:
    maximum_duration: 1200  # 20 минут
    typical_duration: 600   # 10 минут
    records_per_minute: 2000
  
  activities_pipeline:
    maximum_duration: 3600  # 60 минут
    typical_duration: 1800  # 30 минут
    records_per_minute: 5000
  
  testitems_pipeline:
    maximum_duration: 900   # 15 минут
    typical_duration: 450   # 7.5 минут
    records_per_minute: 1000
```

### 2. Использование ресурсов
```yaml
resource_criteria:
  memory_usage:
    maximum: 8  # GB
    typical: 4   # GB
    warning_threshold: 6  # GB
  
  cpu_usage:
    maximum: 80  # %
    typical: 50  # %
    warning_threshold: 70  # %
  
  disk_usage:
    maximum: 10  # GB
    typical: 5   # GB
    warning_threshold: 8   # GB
```

### 3. API производительность
```yaml
api_performance_criteria:
  response_time:
    chembl: 5.0      # секунд
    crossref: 2.0    # секунд
    openalex: 3.0    # секунд
    pubmed: 4.0      # секунд
    semantic_scholar: 2.5  # секунд
    uniprot: 3.0     # секунд
    pubchem: 2.0     # секунд
  
  success_rate:
    minimum: 0.95
    warning_threshold: 0.90
  
  cache_hit_rate:
    minimum: 0.80
    warning_threshold: 0.70
```

## Критерии тестирования

### 1. Покрытие тестами
```yaml
test_coverage_criteria:
  unit_tests:
    minimum: 90  # %
    target: 95   # %
  
  integration_tests:
    minimum: 80  # %
    target: 90  # %
  
  e2e_tests:
    minimum: 70  # %
    target: 80  # %
  
  contract_tests:
    minimum: 85  # %
    target: 95  # %
```

### 2. Качество тестов
```yaml
test_quality_criteria:
  test_success_rate:
    minimum: 0.95
    description: "95% тестов проходят успешно"
  
  test_execution_time:
    maximum: 1800  # 30 минут
    description: "Все тесты выполняются за 30 минут"
  
  test_reliability:
    minimum: 0.90
    description: "90% тестов стабильны"
```

## Критерии API интеграции

### 1. Доступность API
```yaml
api_availability_criteria:
  chembl:
    uptime: 0.99
    response_time: 5.0
    error_rate: 0.01
  
  crossref:
    uptime: 0.99
    response_time: 2.0
    error_rate: 0.01
  
  openalex:
    uptime: 0.98
    response_time: 3.0
    error_rate: 0.02
  
  pubmed:
    uptime: 0.99
    response_time: 4.0
    error_rate: 0.01
  
  semantic_scholar:
    uptime: 0.98
    response_time: 2.5
    error_rate: 0.02
  
  uniprot:
    uptime: 0.99
    response_time: 3.0
    error_rate: 0.01
  
  pubchem:
    uptime: 0.99
    response_time: 2.0
    error_rate: 0.01
```

### 2. Rate Limiting
```yaml
rate_limiting_criteria:
  chembl:
    requests_per_second: 5
    daily_limit: 100000
  
  crossref:
    requests_per_second: 50
    daily_limit: 500000
  
  openalex:
    requests_per_second: 10
    daily_limit: 1000000
  
  pubmed:
    requests_per_second: 3
    daily_limit: 50000
  
  semantic_scholar:
    requests_per_5min: 100
    daily_limit: 100000
  
  uniprot:
    requests_per_second: 10
    daily_limit: 1000000
  
  pubchem:
    requests_per_second: 5
    daily_limit: 50000
```

## Критерии детерминизма

### 1. Воспроизводимость
```yaml
determinism_criteria:
  file_checksums:
    md5: "identical"
    sha256: "identical"
    description: "Идентичные контрольные суммы при повторных запусках"
  
  row_order:
    stable: true
    sort_keys: ["id", "title", "date"]
    description: "Стабильный порядок строк"
  
  column_order:
    fixed: true
    config_driven: true
    description: "Фиксированный порядок колонок"
  
  data_types:
    consistent: true
    format: "standardized"
    description: "Консистентные типы данных"
```

### 2. Метаданные
```yaml
metadata_criteria:
  meta_yaml:
    required_fields: ["pipeline", "execution", "data", "sources", "validation", "files", "checksums"]
    format: "yaml"
    validation: "schema_compliant"
  
  checksums:
    md5: "required"
    sha256: "required"
    description: "Обязательные контрольные суммы"
  
  row_counts:
    accuracy: 1.0
    description: "Точный подсчёт строк"
  
  timestamps:
    format: "ISO 8601 UTC"
    precision: "seconds"
    description: "Стандартизированные временные метки"
```

## Критерии мониторинга

### 1. Логирование
```yaml
logging_criteria:
  structured_logs:
    format: "JSON"
    level: "INFO"
    description: "Структурированные логи в JSON формате"
  
  log_completeness:
    api_calls: "logged"
    errors: "logged"
    performance: "logged"
    description: "Полное логирование всех операций"
  
  log_retention:
    duration: "30 days"
    rotation: "daily"
    compression: "enabled"
    description: "30-дневное хранение логов"
```

### 2. Метрики
```yaml
metrics_criteria:
  api_metrics:
    request_count: "tracked"
    response_time: "tracked"
    error_rate: "tracked"
    cache_hit_rate: "tracked"
  
  pipeline_metrics:
    processing_time: "tracked"
    memory_usage: "tracked"
    records_processed: "tracked"
    quality_score: "tracked"
  
  quality_metrics:
    fill_rate: "tracked"
    duplicate_rate: "tracked"
    validation_errors: "tracked"
    schema_compliance: "tracked"
```

## Критерии безопасности

### 1. Аутентификация
```yaml
security_criteria:
  api_keys:
    storage: "environment_variables"
    rotation: "quarterly"
    description: "Безопасное хранение API ключей"
  
  data_privacy:
    sensitive_data: "masked"
    personal_info: "excluded"
    description: "Защита чувствительных данных"
  
  access_control:
    logs_access: "restricted"
    config_access: "restricted"
    description: "Ограниченный доступ к конфигурации"
```

## Критерии развёртывания

### 1. Конфигурация
```yaml
deployment_criteria:
  environment_variables:
    required: ["API_KEYS", "DATABASE_URL", "LOG_LEVEL"]
    optional: ["CACHE_TTL", "TIMEOUT", "RETRIES"]
    description: "Правильная настройка переменных окружения"
  
  configuration_files:
    validation: "schema_compliant"
    format: "yaml"
    description: "Валидные конфигурационные файлы"
  
  dependencies:
    versions: "pinned"
    security: "updated"
    description: "Зафиксированные версии зависимостей"
```

### 2. Мониторинг
```yaml
monitoring_criteria:
  health_checks:
    api_health: "monitored"
    pipeline_health: "monitored"
    database_health: "monitored"
    description: "Мониторинг состояния системы"
  
  alerting:
    error_rate: "alerted"
    performance: "alerted"
    disk_space: "alerted"
    description: "Алерты на критические события"
```

## Критерии документации

### 1. Полнота
```yaml
documentation_criteria:
  api_documentation:
    endpoints: "documented"
    parameters: "documented"
    examples: "provided"
    description: "Полная документация API"
  
  pipeline_documentation:
    purpose: "documented"
    inputs: "documented"
    outputs: "documented"
    configuration: "documented"
    description: "Полная документация пайплайнов"
  
  user_guides:
    installation: "documented"
    configuration: "documented"
    troubleshooting: "documented"
    description: "Руководства пользователя"
```

### 2. Качество
```yaml
documentation_quality_criteria:
  accuracy:
    examples: "working"
    links: "valid"
    descriptions: "accurate"
    description: "Точная и актуальная документация"
  
  clarity:
    language: "clear"
    structure: "logical"
    navigation: "intuitive"
    description: "Понятная и структурированная документация"
  
  completeness:
    coverage: "100%"
    gaps: "none"
    description: "Полное покрытие функциональности"
```

## Критерии приёмки

### 1. Обязательные критерии
- [ ] Все 5 пайплайнов полностью документированы
- [ ] Отсутствие битых ссылок в документации
- [ ] Воспроизводимый экспорт с идентичными checksums
- [ ] Зелёные тесты (схемы, типы, колонки, сеть)
- [ ] QC метрики в допустимых пределах
- [ ] Производительность в пределах SLA
- [ ] Безопасность данных обеспечена

### 2. Желательные критерии
- [ ] Покрытие тестами >90%
- [ ] Время выполнения <50% от SLA
- [ ] Качество данных >95%
- [ ] Документация оценена как отличная
- [ ] Мониторинг настроен и работает
- [ ] Алерты настроены и тестированы

### 3. Критерии отклонения
- [ ] Критические ошибки в тестах
- [ ] Нарушение безопасности данных
- [ ] Невоспроизводимые результаты
- [ ] Производительность ниже SLA
- [ ] Качество данных ниже пороговых значений
- [ ] Отсутствие критической документации
