# Расширенная система оценки качества данных

## Обзор

Система расширенной оценки качества данных предоставляет детальный анализ структуры и содержания данных с использованием множества метрик, которые помогают понять качество, полноту и характеристики каждого столбца в наборе данных.

## Основные возможности

### 1. Базовые метрики полноты данных
- **non_null**: Количество непустых значений
- **non_empty**: Количество непустых и непустых строковых значений  
- **empty_pct**: Процент пустых значений
- **unique_cnt**: Количество уникальных значений
- **unique_pct_of_non_empty**: Процент уникальных значений от общего количества непустых

### 2. Анализ паттернов данных
- **pattern_cov_doi**: Покрытие паттерном DOI (Digital Object Identifier)
- **pattern_cov_issn**: Покрытие паттерном ISSN (International Standard Serial Number)
- **pattern_cov_isbn**: Покрытие паттерном ISBN (International Standard Book Number)
- **pattern_cov_url**: Покрытие паттерном URL
- **pattern_cov_email**: Покрытие паттерном email адресов
- **bool_like_cov**: Покрытие булевыми значениями (true/false, yes/no, 1/0)

### 3. Статистический анализ числовых данных
- **numeric_cov**: Покрытие числовыми значениями
- **numeric_min**: Минимальное значение
- **numeric_p50**: Медиана (50-й процентиль)
- **numeric_p95**: 95-й процентиль
- **numeric_max**: Максимальное значение
- **numeric_mean**: Среднее значение
- **numeric_std**: Стандартное отклонение

### 4. Анализ временных данных
- **date_cov**: Покрытие датами
- **date_min**: Минимальная дата
- **date_p50**: Медианная дата
- **date_max**: Максимальная дата

### 5. Анализ длины текста
- **text_len_min**: Минимальная длина текста
- **text_len_p50**: Медианная длина текста
- **text_len_p95**: 95-й процентиль длины текста
- **text_len_max**: Максимальная длина текста

### 6. Дополнительные метрики
- **guessed_roles**: Автоматически определенные роли столбцов (identifier, datetime, numeric_metric, categorical, text)
- **top_values**: Топ-10 наиболее частых значений с их количеством

## Использование

### Включение расширенной QC в конфигурации

```yaml
postprocess:
  qc:
    enabled: true
    enhanced: true  # Включает расширенную QC отчетность
```

### Программное использование

```python
from library.etl.enhanced_qc import EnhancedTableQualityProfiler, build_enhanced_qc_summary

# Создание профайлера
profiler = EnhancedTableQualityProfiler(logger=logger)

# Анализ данных
quality_report = profiler.consume(dataframe)

# Генерация сводного отчета
summary = profiler.generate_summary_report(quality_report)

# Генерация детальных отчетов
detailed = profiler.generate_detailed_report(quality_report)
```

## Выходные файлы

При включении расширенной QC система создает следующие файлы:

1. **enhanced_qc_summary.csv** - Сводная таблица со всеми метриками для каждого столбца
2. **enhanced_qc_column_summary.csv** - Детальная информация по столбцам
3. **enhanced_qc_top_values.csv** - Топ значения для каждого столбца
4. **enhanced_qc_pattern_coverage.csv** - Покрытие паттернами по столбцам

## Интерпретация результатов

### Качество данных
- **empty_pct < 5%**: Отличное качество данных
- **empty_pct 5-20%**: Хорошее качество данных
- **empty_pct 20-50%**: Приемлемое качество данных
- **empty_pct > 50%**: Плохое качество данных

### Уникальность данных
- **unique_pct_of_non_empty > 90%**: Высокая уникальность (возможно, идентификаторы)
- **unique_pct_of_non_empty 10-90%**: Средняя уникальность (обычные данные)
- **unique_pct_of_non_empty < 10%**: Низкая уникальность (категориальные данные)

### Паттерны данных
- **pattern_cov_doi > 80%**: Столбец содержит в основном DOI
- **pattern_cov_email > 50%**: Столбец содержит email адреса
- **pattern_cov_url > 50%**: Столбец содержит URL

### Роли данных
Система автоматически определяет роли столбцов:
- **identifier**: Уникальные идентификаторы
- **datetime**: Временные данные
- **numeric_metric**: Числовые метрики
- **categorical**: Категориальные данные
- **text**: Текстовые данные

## Примеры использования

### Анализ качества научных публикаций
```python
# Анализ DOI и ISSN
doi_coverage = summary[summary['column'] == 'doi']['pattern_cov_doi'].iloc[0]
issn_coverage = summary[summary['column'] == 'issn']['pattern_cov_issn'].iloc[0]

print(f"DOI покрытие: {doi_coverage:.1f}%")
print(f"ISSN покрытие: {issn_coverage:.1f}%")
```

### Проверка полноты данных
```python
# Найти столбцы с высоким процентом пустых значений
incomplete_columns = summary[summary['empty_pct'] > 20]['column'].tolist()
print(f"Столбцы с неполными данными: {incomplete_columns}")
```

### Анализ числовых метрик
```python
# Анализ активности соединений
activity_stats = summary[summary['column'] == 'activity_value']
print(f"Средняя активность: {activity_stats['numeric_mean'].iloc[0]:.2f}")
print(f"Стандартное отклонение: {activity_stats['numeric_std'].iloc[0]:.2f}")
```

## Производительность

Система оптимизирована для работы с большими наборами данных:
- Использует векторизованные операции pandas
- Минимальное использование памяти
- Параллельная обработка паттернов
- Кэширование результатов анализа

## Расширение системы

Для добавления новых метрик можно расширить класс `EnhancedTableQualityProfiler`:

```python
def _analyze_custom_pattern(self, series: pd.Series) -> Dict[str, float]:
    """Анализ пользовательского паттерна."""
    # Реализация пользовательского анализа
    pass
```

## Совместимость

- Python 3.8+
- pandas 1.3+
- numpy 1.20+
- structlog (для логирования)
