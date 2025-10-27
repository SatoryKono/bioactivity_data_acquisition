# QC отчеты

## Обзор

Данный документ описывает структуру и содержание отчетов о качестве данных (QC - Quality Control), генерируемых пайплайном Bioactivity Data Acquisition.

## Типы QC отчетов

### Базовые QC отчеты

**Назначение**: Основные метрики качества данных  
**Формат**: JSON  
**Расположение**: `data/output/qc_report.json`

### Расширенные QC отчеты

**Назначение**: Детальный анализ качества данных  
**Формат**: JSON  
**Расположение**: `data/output/enhanced_qc_report.json`

## Структура базового QC отчета

### Основные метрики

```json
{
  "pipeline_info": {
    "version": "1.0.0",
    "timestamp": "2024-01-15T10:30:00Z",
    "config_hash": "abc123...",
    "input_files": ["data/input/activities.csv"],
    "output_files": ["data/output/activities.csv"]
  },
  "data_overview": {
    "total_rows": 12345,
    "total_columns": 15,
    "processing_time_seconds": 45.2,
    "memory_usage_mb": 256.7
  },
  "completeness": {
    "overall_completeness": 0.95,
    "column_completeness": {
      "assay_id": 1.0,
      "molecule_id": 1.0,
      "activity_value": 0.98,
      "activity_type": 0.99,
      "activity_units": 0.97
    }
  },
  "uniqueness": {
    "overall_uniqueness": 0.89,
    "column_uniqueness": {
      "activity_id": 1.0,
      "assay_id": 0.85,
      "molecule_id": 0.92,
      "target_id": 0.78
    }
  },
  "validation_results": {
    "pandera_validation": {
      "passed": true,
      "errors": 0,
      "warnings": 5
    },
    "schema_compliance": 0.99
  },
  "data_distribution": {
    "activity_types": {
      "IC50": 4567,
      "EC50": 2345,
      "Ki": 1234,
      "Kd": 567
    },
    "activity_units": {
      "nM": 6789,
      "uM": 2345,
      "mM": 123
    }
  }
}
```

### Детальное описание метрик

#### Полнота данных (Completeness)

**Определение**: Процент заполненных полей

**Расчет**:
```python
def calculate_completeness(dataframe: pd.DataFrame) -> dict:
    """Вычисляет полноту данных по колонкам."""
    
    completeness = {}
    total_rows = len(dataframe)
    
    for column in dataframe.columns:
        non_null_count = dataframe[column].count()
        completeness[column] = non_null_count / total_rows
    
    return completeness
```

**Интерпретация**:
- `1.0`: Все значения заполнены
- `0.8-0.99`: Хорошая полнота
- `0.5-0.79`: Средняя полнота
- `<0.5`: Низкая полнота

#### Уникальность данных (Uniqueness)

**Определение**: Процент уникальных значений

**Расчет**:
```python
def calculate_uniqueness(dataframe: pd.DataFrame) -> dict:
    """Вычисляет уникальность данных по колонкам."""
    
    uniqueness = {}
    total_rows = len(dataframe)
    
    for column in dataframe.columns:
        unique_count = dataframe[column].nunique()
        uniqueness[column] = unique_count / total_rows
    
    return uniqueness
```

**Интерпретация**:
- `1.0`: Все значения уникальны
- `0.8-0.99`: Высокая уникальность
- `0.5-0.79`: Средняя уникальность
- `<0.5`: Низкая уникальность

#### Валидация схем

**Pandera валидация**:
```python
def validate_with_pandera(dataframe: pd.DataFrame, schema) -> dict:
    """Валидирует данные с помощью Pandera."""
    
    try:
        validated_df = schema.validate(dataframe, lazy=True)
        return {
            "passed": True,
            "errors": 0,
            "warnings": 0
        }
    except pa.errors.SchemaError as e:
        return {
            "passed": False,
            "errors": len(e.failure_cases),
            "warnings": 0,
            "failure_cases": e.failure_cases.to_dict()
        }
```

## Структура расширенного QC отчета

### Дополнительные метрики

```json
{
  "pipeline_info": {
    "version": "1.0.0",
    "timestamp": "2024-01-15T10:30:00Z",
    "config_hash": "abc123...",
    "sources_used": ["chembl", "pubchem"],
    "transforms_applied": ["normalization", "enrichment"]
  },
  "data_overview": {
    "total_rows": 12345,
    "total_columns": 15,
    "processing_time_seconds": 45.2,
    "memory_usage_mb": 256.7,
    "disk_usage_mb": 12.3
  },
  "completeness": {
    "overall_completeness": 0.95,
    "column_completeness": {
      "assay_id": 1.0,
      "molecule_id": 1.0,
      "activity_value": 0.98,
      "activity_type": 0.99,
      "activity_units": 0.97
    },
    "completeness_trends": {
      "improved_columns": ["activity_value", "activity_type"],
      "degraded_columns": ["activity_units"],
      "stable_columns": ["assay_id", "molecule_id"]
    }
  },
  "uniqueness": {
    "overall_uniqueness": 0.89,
    "column_uniqueness": {
      "activity_id": 1.0,
      "assay_id": 0.85,
      "molecule_id": 0.92,
      "target_id": 0.78
    },
    "duplicate_analysis": {
      "exact_duplicates": 23,
      "near_duplicates": 45,
      "duplicate_rate": 0.005
    }
  },
  "validation_results": {
    "pandera_validation": {
      "passed": true,
      "errors": 0,
      "warnings": 5,
      "error_details": []
    },
    "schema_compliance": 0.99,
    "business_rules": {
      "activity_value_range": {
        "passed": true,
        "violations": 0,
        "min_value": 0.001,
        "max_value": 1000000
      },
      "molecule_weight_range": {
        "passed": true,
        "violations": 0,
        "min_value": 50,
        "max_value": 2000
      }
    }
  },
  "data_distribution": {
    "activity_types": {
      "IC50": 4567,
      "EC50": 2345,
      "Ki": 1234,
      "Kd": 567
    },
    "activity_units": {
      "nM": 6789,
      "uM": 2345,
      "mM": 123
    },
    "statistical_summary": {
      "activity_value": {
        "mean": 1234.56,
        "median": 567.89,
        "std": 2345.67,
        "min": 0.001,
        "max": 100000
      }
    }
  },
  "source_analysis": {
    "chembl": {
      "rows_contributed": 8000,
      "completeness": 0.96,
      "quality_score": 0.94
    },
    "pubchem": {
      "rows_contributed": 4345,
      "completeness": 0.93,
      "quality_score": 0.91
    }
  },
  "transformation_analysis": {
    "normalization": {
      "fields_normalized": 8,
      "normalization_errors": 0,
      "improvement_rate": 0.15
    },
    "enrichment": {
      "fields_enriched": 5,
      "enrichment_success_rate": 0.87,
      "new_data_added": 2345
    }
  },
  "quality_trends": {
    "completeness_trend": "improving",
    "uniqueness_trend": "stable",
    "validation_trend": "improving"
  }
}
```

## Генерация QC отчетов

### Базовый QC отчет

```python
def generate_basic_qc_report(
    dataframe: pd.DataFrame,
    pipeline_info: dict
) -> dict:
    """Генерирует базовый QC отчет."""
    
    report = {
        "pipeline_info": pipeline_info,
        "data_overview": {
            "total_rows": len(dataframe),
            "total_columns": len(dataframe.columns),
            "processing_time_seconds": pipeline_info.get("processing_time", 0),
            "memory_usage_mb": pipeline_info.get("memory_usage", 0)
        },
        "completeness": calculate_completeness(dataframe),
        "uniqueness": calculate_uniqueness(dataframe),
        "validation_results": validate_with_pandera(dataframe),
        "data_distribution": analyze_data_distribution(dataframe)
    }
    
    return report
```

### Расширенный QC отчет

```python
def generate_enhanced_qc_report(
    dataframe: pd.DataFrame,
    pipeline_info: dict,
    previous_report: dict = None
) -> dict:
    """Генерирует расширенный QC отчет."""
    
    report = generate_basic_qc_report(dataframe, pipeline_info)
    
    # Дополнительные метрики
    report.update({
        "duplicate_analysis": analyze_duplicates(dataframe),
        "statistical_summary": calculate_statistical_summary(dataframe),
        "source_analysis": analyze_sources(dataframe),
        "transformation_analysis": analyze_transformations(dataframe),
        "quality_trends": calculate_quality_trends(report, previous_report)
    })
    
    return report
```

## Анализ дубликатов

### Точные дубликаты

```python
def find_exact_duplicates(dataframe: pd.DataFrame) -> dict:
    """Находит точные дубликаты."""
    
    duplicates = dataframe.duplicated()
    duplicate_count = duplicates.sum()
    
    return {
        "exact_duplicates": int(duplicate_count),
        "duplicate_rate": float(duplicate_count / len(dataframe)),
        "duplicate_rows": dataframe[duplicates].to_dict('records')
    }
```

### Близкие дубликаты

```python
def find_near_duplicates(dataframe: pd.DataFrame, threshold: float = 0.95) -> dict:
    """Находит близкие дубликаты."""
    
    # Использование fuzzy matching для поиска похожих строк
    near_duplicates = []
    
    for i in range(len(dataframe)):
        for j in range(i + 1, len(dataframe)):
            similarity = calculate_row_similarity(dataframe.iloc[i], dataframe.iloc[j])
            if similarity >= threshold:
                near_duplicates.append({
                    "row1": i,
                    "row2": j,
                    "similarity": similarity
                })
    
    return {
        "near_duplicates": len(near_duplicates),
        "near_duplicate_rate": len(near_duplicates) / len(dataframe),
        "duplicate_pairs": near_duplicates
    }
```

## Статистический анализ

### Описательная статистика

```python
def calculate_statistical_summary(dataframe: pd.DataFrame) -> dict:
    """Вычисляет описательную статистику."""
    
    summary = {}
    
    for column in dataframe.select_dtypes(include=[np.number]).columns:
        summary[column] = {
            "mean": float(dataframe[column].mean()),
            "median": float(dataframe[column].median()),
            "std": float(dataframe[column].std()),
            "min": float(dataframe[column].min()),
            "max": float(dataframe[column].max()),
            "q25": float(dataframe[column].quantile(0.25)),
            "q75": float(dataframe[column].quantile(0.75))
        }
    
    return summary
```

### Анализ распределений

```python
def analyze_distributions(dataframe: pd.DataFrame) -> dict:
    """Анализирует распределения данных."""
    
    distributions = {}
    
    for column in dataframe.columns:
        if dataframe[column].dtype == 'object':
            # Категориальные данные
            value_counts = dataframe[column].value_counts()
            distributions[column] = {
                "type": "categorical",
                "unique_values": len(value_counts),
                "top_values": value_counts.head(10).to_dict()
            }
        elif dataframe[column].dtype in ['int64', 'float64']:
            # Числовые данные
            distributions[column] = {
                "type": "numerical",
                "distribution": "normal" if is_normal_distribution(dataframe[column]) else "non-normal",
                "skewness": float(dataframe[column].skew()),
                "kurtosis": float(dataframe[column].kurtosis())
            }
    
    return distributions
```

## Анализ источников

### Вклад источников

```python
def analyze_sources(dataframe: pd.DataFrame) -> dict:
    """Анализирует вклад различных источников."""
    
    source_analysis = {}
    
    # Анализ по полям, указывающим на источник
    if 'source' in dataframe.columns:
        for source in dataframe['source'].unique():
            source_data = dataframe[dataframe['source'] == source]
            source_analysis[source] = {
                "rows_contributed": len(source_data),
                "completeness": calculate_completeness(source_data),
                "quality_score": calculate_quality_score(source_data)
            }
    
    return source_analysis
```

### Качество по источникам

```python
def calculate_quality_score(dataframe: pd.DataFrame) -> float:
    """Вычисляет общий балл качества."""
    
    completeness = calculate_completeness(dataframe)
    uniqueness = calculate_uniqueness(dataframe)
    
    # Средневзвешенный балл
    quality_score = (
        0.4 * np.mean(list(completeness.values())) +
        0.3 * np.mean(list(uniqueness.values())) +
        0.3 * 1.0  # Предполагаем, что валидация прошла
    )
    
    return float(quality_score)
```

## Тренды качества

### Сравнение с предыдущими отчетами

```python
def calculate_quality_trends(current_report: dict, previous_report: dict = None) -> dict:
    """Вычисляет тренды качества."""
    
    if previous_report is None:
        return {
            "completeness_trend": "unknown",
            "uniqueness_trend": "unknown",
            "validation_trend": "unknown"
        }
    
    trends = {}
    
    # Тренд полноты
    current_completeness = current_report["completeness"]["overall_completeness"]
    previous_completeness = previous_report["completeness"]["overall_completeness"]
    
    if current_completeness > previous_completeness + 0.01:
        trends["completeness_trend"] = "improving"
    elif current_completeness < previous_completeness - 0.01:
        trends["completeness_trend"] = "degrading"
    else:
        trends["completeness_trend"] = "stable"
    
    # Аналогично для других метрик
    trends["uniqueness_trend"] = calculate_trend(
        current_report["uniqueness"]["overall_uniqueness"],
        previous_report["uniqueness"]["overall_uniqueness"]
    )
    
    trends["validation_trend"] = calculate_trend(
        current_report["validation_results"]["schema_compliance"],
        previous_report["validation_results"]["schema_compliance"]
    )
    
    return trends
```

## Сохранение QC отчетов

### Запись в JSON

```python
def save_qc_report(report: dict, output_path: Path) -> None:
    """Сохраняет QC отчет в JSON файл."""
    
    # Создание директории
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Запись в файл
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    logger.info(f"QC отчет сохранен: {output_path}")
```

### Архивирование отчетов

```python
def archive_qc_report(report: dict, archive_path: Path) -> None:
    """Архивирует QC отчет с временной меткой."""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_file = archive_path / f"qc_report_{timestamp}.json"
    
    save_qc_report(report, archive_file)
```

## Интерпретация QC отчетов

### Критерии качества

**Отличное качество**:
- Полнота > 0.95
- Уникальность > 0.90
- Валидация: 100% прохождение
- Дубликаты < 1%

**Хорошее качество**:
- Полнота > 0.85
- Уникальность > 0.80
- Валидация: > 95% прохождение
- Дубликаты < 5%

**Требует внимания**:
- Полнота < 0.85
- Уникальность < 0.80
- Валидация: < 95% прохождение
- Дубликаты > 5%

### Рекомендации по улучшению

```python
def generate_quality_recommendations(report: dict) -> list:
    """Генерирует рекомендации по улучшению качества."""
    
    recommendations = []
    
    # Рекомендации по полноте
    completeness = report["completeness"]["overall_completeness"]
    if completeness < 0.9:
        recommendations.append(
            f"Низкая полнота данных ({completeness:.2%}). "
            "Рекомендуется проверить источники данных и процессы извлечения."
        )
    
    # Рекомендации по уникальности
    uniqueness = report["uniqueness"]["overall_uniqueness"]
    if uniqueness < 0.8:
        recommendations.append(
            f"Низкая уникальность данных ({uniqueness:.2%}). "
            "Рекомендуется проверить процессы дедупликации."
        )
    
    # Рекомендации по валидации
    validation_errors = report["validation_results"]["pandera_validation"]["errors"]
    if validation_errors > 0:
        recommendations.append(
            f"Найдены ошибки валидации ({validation_errors}). "
            "Рекомендуется исправить данные или обновить схемы валидации."
        )
    
    return recommendations
```
