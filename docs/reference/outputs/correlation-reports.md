# Корреляционные отчеты

## Обзор

Данный документ описывает структуру и содержание корреляционных отчетов, генерируемых пайплайном Bioactivity Data Acquisition для анализа связей между различными параметрами данных.

## Типы корреляционных отчетов

### Базовые корреляционные матрицы

**Назначение**: Основные корреляции между числовыми параметрами  
**Формат**: JSON  
**Расположение**: `data/output/correlation_matrix.json`

### Расширенные корреляционные отчеты

**Назначение**: Детальный анализ корреляций с визуализацией  
**Формат**: JSON + HTML  
**Расположение**: `data/output/enhanced_correlation_report.json`

## Структура базового корреляционного отчета

### Основные компоненты

```json
{
  "pipeline_info": {
    "version": "1.0.0",
    "timestamp": "2024-01-15T10:30:00Z",
    "config_hash": "abc123...",
    "correlation_method": "pearson",
    "significance_threshold": 0.05
  },
  "data_overview": {
    "total_rows": 12345,
    "numeric_columns": 8,
    "categorical_columns": 7,
    "correlation_pairs": 28
  },
  "correlation_matrix": {
    "activity_value": {
      "molecular_weight": 0.234,
      "heavy_atom_count": 0.456,
      "logp": -0.123,
      "tpsa": 0.345
    },
    "molecular_weight": {
      "heavy_atom_count": 0.789,
      "logp": 0.234,
      "tpsa": 0.567
    },
    "heavy_atom_count": {
      "logp": 0.123,
      "tpsa": 0.456
    },
    "logp": {
      "tpsa": -0.234
    }
  },
  "significant_correlations": {
    "strong_positive": [
      {
        "variable1": "molecular_weight",
        "variable2": "heavy_atom_count",
        "correlation": 0.789,
        "p_value": 0.001
      }
    ],
    "strong_negative": [
      {
        "variable1": "activity_value",
        "variable2": "logp",
        "correlation": -0.123,
        "p_value": 0.045
      }
    ],
    "moderate_positive": [
      {
        "variable1": "activity_value",
        "variable2": "molecular_weight",
        "correlation": 0.234,
        "p_value": 0.012
      }
    ]
  },
  "correlation_summary": {
    "strong_correlations": 5,
    "moderate_correlations": 12,
    "weak_correlations": 11,
    "no_correlation": 0
  }
}
```

## Методы корреляционного анализа

### Коэффициент корреляции Пирсона

**Назначение**: Линейные корреляции между числовыми переменными

**Расчет**:
```python
def calculate_pearson_correlations(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Вычисляет корреляции Пирсона."""
    
    # Выбор только числовых колонок
    numeric_columns = dataframe.select_dtypes(include=[np.number]).columns
    
    # Расчет корреляционной матрицы
    correlation_matrix = dataframe[numeric_columns].corr(method='pearson')
    
    return correlation_matrix
```

**Интерпретация**:
- `0.7-1.0`: Сильная положительная корреляция
- `0.3-0.7`: Умеренная положительная корреляция
- `0.1-0.3`: Слабая положительная корреляция
- `-0.1-0.1`: Отсутствие корреляции
- `-0.3--0.1`: Слабая отрицательная корреляция
- `-0.7--0.3`: Умеренная отрицательная корреляция
- `-1.0--0.7`: Сильная отрицательная корреляция

### Коэффициент корреляции Спирмена

**Назначение**: Монотонные корреляции (не обязательно линейные)

**Расчет**:
```python
def calculate_spearman_correlations(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Вычисляет корреляции Спирмена."""
    
    numeric_columns = dataframe.select_dtypes(include=[np.number]).columns
    correlation_matrix = dataframe[numeric_columns].corr(method='spearman')
    
    return correlation_matrix
```

### Коэффициент корреляции Кендалла

**Назначение**: Ранговые корреляции для небольших выборок

**Расчет**:
```python
def calculate_kendall_correlations(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Вычисляет корреляции Кендалла."""
    
    numeric_columns = dataframe.select_dtypes(include=[np.number]).columns
    correlation_matrix = dataframe[numeric_columns].corr(method='kendall')
    
    return correlation_matrix
```

## Статистическая значимость

### P-значения

**Расчет**:
```python
def calculate_correlation_significance(
    dataframe: pd.DataFrame,
    method: str = 'pearson'
) -> pd.DataFrame:
    """Вычисляет статистическую значимость корреляций."""
    
    from scipy.stats import pearsonr, spearmanr, kendalltau
    
    numeric_columns = dataframe.select_dtypes(include=[np.number]).columns
    p_values = pd.DataFrame(index=numeric_columns, columns=numeric_columns)
    
    for col1 in numeric_columns:
        for col2 in numeric_columns:
            if col1 == col2:
                p_values.loc[col1, col2] = 0.0
            else:
                if method == 'pearson':
                    _, p_val = pearsonr(dataframe[col1], dataframe[col2])
                elif method == 'spearman':
                    _, p_val = spearmanr(dataframe[col1], dataframe[col2])
                elif method == 'kendall':
                    _, p_val = kendalltau(dataframe[col1], dataframe[col2])
                
                p_values.loc[col1, col2] = p_val
    
    return p_values
```

### Доверительные интервалы

**Расчет**:
```python
def calculate_confidence_intervals(
    dataframe: pd.DataFrame,
    confidence_level: float = 0.95
) -> dict:
    """Вычисляет доверительные интервалы для корреляций."""
    
    from scipy.stats import pearsonr
    from scipy.stats import fisher_transform
    
    numeric_columns = dataframe.select_dtypes(include=[np.number]).columns
    confidence_intervals = {}
    
    for col1 in numeric_columns:
        for col2 in numeric_columns:
            if col1 != col2:
                corr, _ = pearsonr(dataframe[col1], dataframe[col2])
                
                # Преобразование Фишера
                z = fisher_transform(corr)
                se = 1 / np.sqrt(len(dataframe) - 3)
                
                # Доверительный интервал для z
                alpha = 1 - confidence_level
                z_critical = stats.norm.ppf(1 - alpha/2)
                z_lower = z - z_critical * se
                z_upper = z + z_critical * se
                
                # Обратное преобразование
                ci_lower = np.tanh(z_lower)
                ci_upper = np.tanh(z_upper)
                
                confidence_intervals[f"{col1}_{col2}"] = {
                    "correlation": corr,
                    "ci_lower": ci_lower,
                    "ci_upper": ci_upper,
                    "confidence_level": confidence_level
                }
    
    return confidence_intervals
```

## Классификация корреляций

### По силе связи

```python
def classify_correlations(correlation_matrix: pd.DataFrame) -> dict:
    """Классифицирует корреляции по силе."""
    
    classifications = {
        "strong_positive": [],
        "moderate_positive": [],
        "weak_positive": [],
        "no_correlation": [],
        "weak_negative": [],
        "moderate_negative": [],
        "strong_negative": []
    }
    
    for col1 in correlation_matrix.columns:
        for col2 in correlation_matrix.columns:
            if col1 != col2:
                corr = correlation_matrix.loc[col1, col2]
                
                if corr >= 0.7:
                    classifications["strong_positive"].append({
                        "variable1": col1,
                        "variable2": col2,
                        "correlation": corr
                    })
                elif corr >= 0.3:
                    classifications["moderate_positive"].append({
                        "variable1": col1,
                        "variable2": col2,
                        "correlation": corr
                    })
                elif corr >= 0.1:
                    classifications["weak_positive"].append({
                        "variable1": col1,
                        "variable2": col2,
                        "correlation": corr
                    })
                elif corr >= -0.1:
                    classifications["no_correlation"].append({
                        "variable1": col1,
                        "variable2": col2,
                        "correlation": corr
                    })
                elif corr >= -0.3:
                    classifications["weak_negative"].append({
                        "variable1": col1,
                        "variable2": col2,
                        "correlation": corr
                    })
                elif corr >= -0.7:
                    classifications["moderate_negative"].append({
                        "variable1": col1,
                        "variable2": col2,
                        "correlation": corr
                    })
                else:
                    classifications["strong_negative"].append({
                        "variable1": col1,
                        "variable2": col2,
                        "correlation": corr
                    })
    
    return classifications
```

### По статистической значимости

```python
def filter_significant_correlations(
    correlation_matrix: pd.DataFrame,
    p_values: pd.DataFrame,
    significance_threshold: float = 0.05
) -> dict:
    """Фильтрует статистически значимые корреляции."""
    
    significant_correlations = {
        "significant": [],
        "not_significant": []
    }
    
    for col1 in correlation_matrix.columns:
        for col2 in correlation_matrix.columns:
            if col1 != col2:
                corr = correlation_matrix.loc[col1, col2]
                p_val = p_values.loc[col1, col2]
                
                correlation_info = {
                    "variable1": col1,
                    "variable2": col2,
                    "correlation": corr,
                    "p_value": p_val
                }
                
                if p_val < significance_threshold:
                    significant_correlations["significant"].append(correlation_info)
                else:
                    significant_correlations["not_significant"].append(correlation_info)
    
    return significant_correlations
```

## Расширенный корреляционный анализ

### Частичные корреляции

**Назначение**: Корреляции с учетом влияния других переменных

**Расчет**:
```python
def calculate_partial_correlations(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Вычисляет частичные корреляции."""
    
    from sklearn.covariance import GraphicalLassoCV
    
    numeric_columns = dataframe.select_dtypes(include=[np.number]).columns
    data = dataframe[numeric_columns].dropna()
    
    # Оценка ковариационной матрицы
    model = GraphicalLassoCV()
    model.fit(data)
    precision_matrix = model.precision_
    
    # Преобразование в корреляционную матрицу
    partial_corr = -precision_matrix / np.sqrt(np.outer(np.diag(precision_matrix), np.diag(precision_matrix)))
    
    return pd.DataFrame(partial_corr, index=numeric_columns, columns=numeric_columns)
```

### Кросс-корреляции

**Назначение**: Корреляции с временными задержками

**Расчет**:
```python
def calculate_cross_correlations(
    dataframe: pd.DataFrame,
    max_lags: int = 5
) -> dict:
    """Вычисляет кросс-корреляции."""
    
    from scipy.signal import correlate
    
    numeric_columns = dataframe.select_dtypes(include=[np.number]).columns
    cross_correlations = {}
    
    for col1 in numeric_columns:
        for col2 in numeric_columns:
            if col1 != col2:
                series1 = dataframe[col1].dropna()
                series2 = dataframe[col2].dropna()
                
                # Нормализация данных
                series1_norm = (series1 - series1.mean()) / series1.std()
                series2_norm = (series2 - series2.mean()) / series2.std()
                
                # Кросс-корреляция
                correlation = correlate(series1_norm, series2_norm, mode='full')
                lags = np.arange(-max_lags, max_lags + 1)
                
                cross_correlations[f"{col1}_{col2}"] = {
                    "correlations": correlation.tolist(),
                    "lags": lags.tolist(),
                    "max_correlation": np.max(correlation),
                    "max_lag": lags[np.argmax(correlation)]
                }
    
    return cross_correlations
```

## Визуализация корреляций

### Тепловая карта

```python
def create_correlation_heatmap(
    correlation_matrix: pd.DataFrame,
    output_path: Path
) -> None:
    """Создает тепловую карту корреляций."""
    
    import matplotlib.pyplot as plt
    import seaborn as sns
    
    plt.figure(figsize=(12, 10))
    
    # Создание маски для верхнего треугольника
    mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))
    
    # Построение тепловой карты
    sns.heatmap(
        correlation_matrix,
        mask=mask,
        annot=True,
        cmap='RdBu_r',
        center=0,
        square=True,
        fmt='.3f',
        cbar_kws={"shrink": .8}
    )
    
    plt.title('Корреляционная матрица')
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
```

### Диаграмма рассеяния

```python
def create_scatter_plot(
    dataframe: pd.DataFrame,
    x_col: str,
    y_col: str,
    output_path: Path
) -> None:
    """Создает диаграмму рассеяния с линией тренда."""
    
    import matplotlib.pyplot as plt
    import seaborn as sns
    
    plt.figure(figsize=(10, 8))
    
    # Диаграмма рассеяния
    sns.scatterplot(data=dataframe, x=x_col, y=y_col, alpha=0.6)
    
    # Линия тренда
    sns.regplot(data=dataframe, x=x_col, y=y_col, scatter=False, color='red')
    
    # Расчет корреляции
    corr = dataframe[x_col].corr(dataframe[y_col])
    plt.title(f'{x_col} vs {y_col}\nКорреляция: {corr:.3f}')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
```

## Генерация корреляционных отчетов

### Базовый отчет

```python
def generate_correlation_report(
    dataframe: pd.DataFrame,
    pipeline_info: dict
) -> dict:
    """Генерирует базовый корреляционный отчет."""
    
    # Расчет корреляций
    correlation_matrix = calculate_pearson_correlations(dataframe)
    p_values = calculate_correlation_significance(dataframe)
    
    # Классификация корреляций
    classifications = classify_correlations(correlation_matrix)
    significant_correlations = filter_significant_correlations(
        correlation_matrix, p_values
    )
    
    report = {
        "pipeline_info": pipeline_info,
        "data_overview": {
            "total_rows": len(dataframe),
            "numeric_columns": len(correlation_matrix.columns),
            "correlation_pairs": len(correlation_matrix.columns) * (len(correlation_matrix.columns) - 1) // 2
        },
        "correlation_matrix": correlation_matrix.to_dict(),
        "significant_correlations": significant_correlations,
        "correlation_classifications": classifications,
        "correlation_summary": {
            "strong_correlations": len(classifications["strong_positive"]) + len(classifications["strong_negative"]),
            "moderate_correlations": len(classifications["moderate_positive"]) + len(classifications["moderate_negative"]),
            "weak_correlations": len(classifications["weak_positive"]) + len(classifications["weak_negative"]),
            "no_correlation": len(classifications["no_correlation"])
        }
    }
    
    return report
```

### Расширенный отчет

```python
def generate_enhanced_correlation_report(
    dataframe: pd.DataFrame,
    pipeline_info: dict
) -> dict:
    """Генерирует расширенный корреляционный отчет."""
    
    # Базовый отчет
    report = generate_correlation_report(dataframe, pipeline_info)
    
    # Дополнительные анализы
    report.update({
        "partial_correlations": calculate_partial_correlations(dataframe).to_dict(),
        "cross_correlations": calculate_cross_correlations(dataframe),
        "confidence_intervals": calculate_confidence_intervals(dataframe),
        "correlation_insights": generate_correlation_insights(report)
    })
    
    return report
```

## Интерпретация корреляций

### Биологическая значимость

```python
def interpret_biological_correlations(correlations: dict) -> list:
    """Интерпретирует корреляции с биологической точки зрения."""
    
    insights = []
    
    # Корреляция молекулярного веса и количества атомов
    if "molecular_weight" in correlations and "heavy_atom_count" in correlations:
        corr = correlations["molecular_weight"]["heavy_atom_count"]
        if corr > 0.7:
            insights.append(
                "Сильная корреляция между молекулярным весом и количеством тяжелых атомов "
                "ожидаема и подтверждает качество данных."
            )
    
    # Корреляция активности и липофильности
    if "activity_value" in correlations and "logp" in correlations:
        corr = correlations["activity_value"]["logp"]
        if corr < -0.3:
            insights.append(
                "Отрицательная корреляция между активностью и липофильностью "
                "может указывать на влияние растворимости на активность."
            )
    
    return insights
```

### Статистические рекомендации

```python
def generate_statistical_recommendations(report: dict) -> list:
    """Генерирует статистические рекомендации."""
    
    recommendations = []
    
    # Рекомендации по сильным корреляциям
    strong_correlations = (
        report["correlation_classifications"]["strong_positive"] +
        report["correlation_classifications"]["strong_negative"]
    )
    
    if len(strong_correlations) > 5:
        recommendations.append(
            "Обнаружено много сильных корреляций. "
            "Рекомендуется проверить на мультиколлинеарность."
        )
    
    # Рекомендации по размеру выборки
    sample_size = report["data_overview"]["total_rows"]
    if sample_size < 100:
        recommendations.append(
            "Небольшой размер выборки может влиять на надежность корреляций. "
            "Рекомендуется увеличить объем данных."
        )
    
    return recommendations
```

## Сохранение отчетов

### JSON отчет

```python
def save_correlation_report(report: dict, output_path: Path) -> None:
    """Сохраняет корреляционный отчет в JSON."""
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Корреляционный отчет сохранен: {output_path}")
```

### HTML отчет с визуализацией

```python
def generate_html_correlation_report(
    report: dict,
    dataframe: pd.DataFrame,
    output_path: Path
) -> None:
    """Генерирует HTML отчет с визуализацией."""
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Корреляционный отчет</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .section {{ margin-bottom: 30px; }}
            .correlation-table {{ border-collapse: collapse; width: 100%; }}
            .correlation-table th, .correlation-table td {{ border: 1px solid #ddd; padding: 8px; text-align: center; }}
            .correlation-table th {{ background-color: #f2f2f2; }}
            .strong {{ background-color: #ffcccc; }}
            .moderate {{ background-color: #ffffcc; }}
            .weak {{ background-color: #ccffcc; }}
        </style>
    </head>
    <body>
        <h1>Корреляционный отчет</h1>
        
        <div class="section">
            <h2>Обзор данных</h2>
            <p>Общее количество строк: {report['data_overview']['total_rows']}</p>
            <p>Количество числовых колонок: {report['data_overview']['numeric_columns']}</p>
            <p>Количество пар корреляций: {report['data_overview']['correlation_pairs']}</p>
        </div>
        
        <div class="section">
            <h2>Сводка корреляций</h2>
            <p>Сильные корреляции: {report['correlation_summary']['strong_correlations']}</p>
            <p>Умеренные корреляции: {report['correlation_summary']['moderate_correlations']}</p>
            <p>Слабые корреляции: {report['correlation_summary']['weak_correlations']}</p>
        </div>
        
        <div class="section">
            <h2>Статистически значимые корреляции</h2>
            <ul>
    """
    
    for corr in report['significant_correlations']['significant']:
        html_content += f"""
                <li>{corr['variable1']} ↔ {corr['variable2']}: 
                    {corr['correlation']:.3f} (p = {corr['p_value']:.3f})</li>
        """
    
    html_content += """
            </ul>
        </div>
    </body>
    </html>
    """
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.info(f"HTML корреляционный отчет сохранен: {output_path}")
```
