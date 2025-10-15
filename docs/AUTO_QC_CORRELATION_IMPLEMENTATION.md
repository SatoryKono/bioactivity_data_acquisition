# Реализация автоматического сохранения QC и корреляционных отчетов

## Обзор

В проект добавлена функциональность автоматического сохранения таблиц корреляций и таблиц по качеству данных при каждом сохранении итоговой таблицы с данными. Это обеспечивает комплексный анализ качества данных и взаимосвязей между переменными без необходимости дополнительной настройки.

## Функциональность

### Автоматическая генерация отчетов

При каждом сохранении итоговых данных через функцию `write_deterministic_csv` автоматически создаются:

#### 1. Базовые отчеты

- **QC отчет**: Базовая оценка качества данных с основными метриками

- **Корреляционный отчет**: Базовая корреляционная матрица для числовых данных

#### 2. Расширенные QC отчеты

- **Расширенный QC отчет**: Детальные метрики качества данных

- **Детальные QC отчеты**: Разбивка по категориям (summary, patterns, top values)

#### 3. Расширенные корреляционные отчеты

- **Корреляционные матрицы**: Pearson, Spearman, Covariance

- **Категориальные корреляции**: Cramer's V для категориальных данных

- **Смешанные корреляции**: Eta-squared, Point-Biserial

- **Детальные отчеты**: JSON анализ и CSV инсайты

### Структура создаваемых файлов

Для файла данных `example_data.csv` создаются следующие отчеты:

```text
example_data.csv                                    # Основной файл данных
example_data_quality_report.csv                     # Базовый QC отчет
example_data_correlation_report.csv                 # Базовая корреляционная матрица
example_data_quality_report_enhanced.csv            # Расширенный QC отчет
example_data_quality_report_detailed/               # Детальные QC отчеты
├── column_summary.csv
├── pattern_coverage.csv
└── top_values.csv
example_data_correlation_report_enhanced/           # Расширенные корреляционные отчеты
├── numeric_pearson.csv
├── numeric_spearman.csv
├── numeric_covariance.csv
├── categorical_cramers_v.csv
├── mixed_eta_squared.csv
├── mixed_point_biserial.csv
└── correlation_summary.csv
example_data_correlation_report_detailed/           # Детальные корреляционные отчеты
├── correlation_analysis.json
└── correlation_insights.csv
```

## Реализация

### Функция `_auto_generate_qc_and_correlation_reports`

**Файл:** `src/library/etl/load.py`

```python
def _auto_generate_qc_and_correlation_reports(
    df: pd.DataFrame,
    data_path: Path,
    output: OutputSettings | None = None,
    logger: BoundLogger | None = None,
) -> None:
    """Автоматически генерирует и сохраняет QC и корреляционные отчеты."""
```

### Интеграция в пайплайн

Функция автоматически вызывается в `write_deterministic_csv`:

```python
# Автоматически генерируем и сохраняем QC и корреляционные таблицы

if not df.empty:
    _auto_generate_qc_and_correlation_reports(
        df_to_write,
        destination,
        output,
        logger=logger
    )
```

### Логирование

Функция поддерживает детальное логирование:

```python
logger.info("auto_qc_corr_start", qc_path=str(qc_path), corr_path=str(corr_path))
logger.info("auto_qc_basic_saved", path=str(qc_path))
logger.info("auto_corr_basic_saved", path=str(corr_path))
logger.info("auto_qc_enhanced_saved", enhanced_path=str(enhanced_qc_path))
logger.info("auto_corr_enhanced_saved", enhanced_path=str(enhanced_corr_path))
logger.info("auto_qc_corr_complete", qc_files=[...], corr_files=[...])
```

## Типы создаваемых отчетов

### Базовые отчеты

#### QC отчет (`*_quality_report.csv`)

- `metric`: Название метрики

- `value`: Значение метрики

- `threshold`: Пороговое значение

- `ratio`: Соотношение

- `status`: Статус (pass/fail)

#### Корреляционный отчет (`*_correlation_report.csv`)

- Базовая корреляционная матрица для числовых данных

- Включает столбец index для ссылочной целостности

### Расширенные QC отчеты

#### Расширенный QC отчет (`*_quality_report_enhanced.csv`)

Содержит 30+ метрик качества данных:

- `non_null`, `non_empty`, `empty_pct`

- `unique_cnt`, `unique_pct_of_non_empty`

- `pattern_cov_doi`, `pattern_cov_issn`, `pattern_cov_isbn`

- `pattern_cov_url`, `pattern_cov_email`

- `bool_like_cov`, `numeric_cov`

- `numeric_min`, `numeric_p50`, `numeric_p95`, `numeric_max`

- `numeric_mean`, `numeric_std`

- `date_cov`, `date_min`, `date_p50`, `date_max`

- `text_len_min`, `text_len_p50`, `text_len_p95`, `text_len_max`

- `guessed_roles`, `top_values`

#### Детальные QC отчеты (`*_quality_report_detailed/`)

- `column_summary.csv`: Сводка по колонкам

- `pattern_coverage.csv`: Покрытие паттернов

- `top_values.csv`: Топ значения

### Расширенные корреляционные отчеты

#### Корреляционные матрицы (`*_correlation_report_enhanced/`)

- `numeric_pearson.csv`: Корреляция Пирсона

- `numeric_spearman.csv`: Корреляция Спирмена

- `numeric_covariance.csv`: Ковариация

- `categorical_cramers_v.csv`: Cramer's V для категориальных данных

- `mixed_eta_squared.csv`: Eta-squared для смешанных данных

- `mixed_point_biserial.csv`: Point-Biserial корреляция

- `correlation_summary.csv`: Сводка корреляций

#### Детальные корреляционные отчеты (`*_correlation_report_detailed/`)

- `correlation_analysis.json`: Полный анализ в JSON формате

- `correlation_insights.csv`: Человекочитаемые инсайты и рекомендации

## Обработка ошибок

Функция включает обработку ошибок:

```python
try:
    # Генерация отчетов
    ...
except Exception as e:
    if logger is not None:
        logger.error("auto_qc_corr_error", error=str(e), error_type=type(e).__name__)
    # Не прерываем основной процесс, только логируем ошибку
```

## Совместимость

### Обратная совместимость

✅ **Полная обратная совместимость**:

- Все существующие функции работают без изменений

- Не требуется модификация конфигурационных файлов

- Поддерживаются все форматы вывода (CSV, Parquet)

- Автоматическое сохранение работает прозрачно

### Условия работы

- **Непустые данные**: Отчеты создаются только для непустых DataFrame

- **Автоматичность**: Не требует дополнительной настройки

- **Формат данных**: Поддерживает любые типы данных в DataFrame

- **Размер данных**: Работает с данными любого размера

## Преимущества

### 1. Автоматичность

- Не требует вмешательства пользователя

- Интегрировано в существующий процесс сохранения

- Работает для всех типов данных

### 2. Комплексность

- Полный набор метрик качества данных

- Множественные типы корреляционного анализа

- Детальные инсайты и рекомендации

### 3. Удобство использования

- Предсказуемая структура файлов

- Логирование процесса

- Обработка ошибок без прерывания основного процесса

### 4. Масштабируемость

- Поддержка больших объемов данных

- Эффективная генерация отчетов

- Оптимизированное использование ресурсов

## Примеры использования

### Базовое использование

```python
from library.etl.load import write_deterministic_csv

# Простое сохранение данных - отчеты создаются автоматически

write_deterministic_csv(df, "output/data.csv")
```

### С настройками

```python
from library.config import OutputSettings, CsvFormatSettings

output_settings = OutputSettings(
    data_path=Path("output/data.csv"),
    qc_report_path=Path("output/qc.csv"),
    correlation_path=Path("output/corr.csv"),
    format="csv",
    csv=CsvFormatSettings()
)

write_deterministic_csv(df, Path("output/data.csv"), output=output_settings)
```

### Анализ созданных отчетов

```python
import pandas as pd
import json

# Загрузка QC отчета

qc_report = pd.read_csv("output/data_quality_report.csv")
print("QC метрики:", qc_report)

# Загрузка расширенного QC отчета

enhanced_qc = pd.read_csv("output/data_quality_report_enhanced.csv")
print("Расширенные метрики:", enhanced_qc)

# Загрузка корреляционных инсайтов

insights = pd.read_csv("output/data_correlation_report_detailed/correlation_insights.csv")
print("Корреляционные инсайты:", insights)

# Загрузка JSON анализа

with open("output/data_correlation_report_detailed/correlation_analysis.json", 'r') as f:
    analysis = json.load(f)
print("Полный анализ:", analysis)
```

## Заключение

Автоматическое сохранение QC и корреляционных отчетов обеспечивает комплексный анализ качества данных и взаимосвязей между переменными без необходимости дополнительной настройки. Функциональность полностью интегрирована в существующий пайплайн и работает прозрачно для пользователя, предоставляя богатый набор аналитических отчетов для каждого сохранения данных.
