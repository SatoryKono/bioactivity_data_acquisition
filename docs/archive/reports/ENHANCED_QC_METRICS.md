# Расширенная система оценки качества данных

## Обзор

Система расширенной оценки качества данных предоставляет детальный анализ
структуры и содержания данных с использованием множества метрик, которые
помогают понять качество, полноту и характеристики каждого столбца в наборе
данных.

## Основные возможности

### 1. Базовые метрики полноты данных

- **non*null**: Количество непустых значений

- **non*empty**: Количество непустых и непустых строковых значений

- **empty*pct**: Процент пустых значений

- **unique*cnt**: Количество уникальных значений

- **unique*pct*of*non*empty**: Процент уникальных значений от общего количества непустых

### 2. Анализ паттернов данных

- **pattern*cov*doi**: Покрытие паттерном DOI (Digital Object Identifier)

- **pattern*cov*issn**: Покрытие паттерном ISSN (International Standard Serial Number)

- **pattern*cov*isbn**: Покрытие паттерном ISBN (International Standard Book Number)

- **pattern*cov*url**: Покрытие паттерном URL

- **pattern*cov*email**: Покрытие паттерном email адресов

- **bool*like*cov**: Покрытие булевыми значениями (true/false, yes/no, 1/0)

### 3. Статистический анализ числовых данных

- **numeric*cov**: Покрытие числовыми значениями

- **numeric*min**: Минимальное значение

- **numeric*p50**: Медиана (50-й процентиль)

- **numeric*p95**: 95-й процентиль

- **numeric*max**: Максимальное значение

- **numeric*mean**: Среднее значение

- **numeric*std**: Стандартное отклонение

### 4. Анализ временных данных

- **date*cov**: Покрытие датами

- **date*min**: Минимальная дата

- **date*p50**: Медианная дата

- **date*max**: Максимальная дата

### 5. Анализ длины текста

- **text*len*min**: Минимальная длина текста

- **text*len*p50**: Медианная длина текста

- **text*len*p95**: 95-й процентиль длины текста

- **text*len*max**: Максимальная длина текста

### 6. Дополнительные метрики

- **guessed*roles**: Автоматически определенные роли столбцов (identifier, datetime, numeric*metric, categorical, text)

- **top*values**: Топ-10 наиболее частых значений с их количеством

## Использование

### Включение расширенной QC в конфигурации

```

postprocess:
  qc:
    enabled: true
    enhanced: true  # Включает расширенную QC отчетность

```

### Программное использование

```

from library.etl.enhanced*qc import EnhancedTableQualityProfiler,
build*enhanced*qc*summary

## Создание профайлера

profiler = EnhancedTableQualityProfiler(logger=logger)

## Анализ данных

quality*report = profiler.consume(dataframe)

## Генерация сводного отчета

summary = profiler.generate*summary*report(quality*report)

## Генерация детальных отчетов

detailed = profiler.generate*detailed*report(quality*report)

```

## Выходные файлы

При включении расширенной QC система создает следующие файлы:

1. **enhanced*qc*summary.csv**- Сводная таблица со всеми метриками для каждого
столбца
2.**enhanced*qc*column*summary.csv**- Детальная информация по столбцам
3.**enhanced*qc*top*values.csv**- Топ значения для каждого столбца
4.**enhanced*qc*pattern*coverage.csv**- Покрытие паттернами по столбцам

## Интерпретация результатов

### Качество данных

-**empty*pct < 5%**: Отличное качество данных

- **empty*pct 5-20%**: Хорошее качество данных

- **empty*pct 20-50%**: Приемлемое качество данных

- **empty*pct > 50%**: Плохое качество данных

### Уникальность данных

- **unique*pct*of*non*empty > 90%**: Высокая уникальность (возможно, идентификаторы)

- **unique*pct*of*non*empty 10-90%**: Средняя уникальность (обычные данные)

- **unique*pct*of*non*empty < 10%**: Низкая уникальность (категориальные данные)

### Паттерны данных

- **pattern*cov*doi > 80%**: Столбец содержит в основном DOI

- **pattern*cov*email > 50%**: Столбец содержит email адреса

- **pattern*cov*url > 50%**: Столбец содержит URL

### Роли данных

Система автоматически определяет роли столбцов:

- **identifier**: Уникальные идентификаторы

- **datetime**: Временные данные

- **numeric*metric**: Числовые метрики

- **categorical**: Категориальные данные

- **text**: Текстовые данные

## Примеры использования

### Анализ качества научных публикаций

```

## Анализ DOI и ISSN

doi*coverage = summary[summary['column'] == 'doi']['pattern*cov*doi'].iloc[0]
issn*coverage = summary[summary['column'] == 'issn']['pattern*cov*issn'].iloc[0]

print(f"DOI покрытие: {doi*coverage:.1f}%")
print(f"ISSN покрытие: {issn*coverage:.1f}%")

```

### Проверка полноты данных

```

## Найти столбцы с высоким процентом пустых значений

incomplete*columns = summary[summary['empty*pct'] > 20]['column'].tolist()
print(f"Столбцы с неполными данными: {incomplete*columns}")

```

### Анализ числовых метрик

```

## Анализ активности соединений

activity*stats = summary[summary['column'] == 'activity*value']
print(f"Средняя активность: {activity*stats['numeric*mean'].iloc[0]:.2f}")
print(f"Стандартное отклонение: {activity*stats['numeric*std'].iloc[0]:.2f}")

```

## Производительность

Система оптимизирована для работы с большими наборами данных:

- Использует векторизованные операции pandas

- Минимальное использование памяти

- Параллельная обработка паттернов

- Кэширование результатов анализа

## Расширение системы

Для добавления новых метрик можно расширить класс`EnhancedTableQualityProfiler`:

```

def *analyze*custom_pattern(self, series: pd.Series) -> Dict[str, float]:
    """Анализ пользовательского паттерна."""

## Реализация пользовательского анализа

    pass

```

## Совместимость

- Python 3.8+

- pandas 1.3+

- numpy 1.20+

- structlog (для логирования)
