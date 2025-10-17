# Артефакты выгрузки

## CSV датасет

- Детерминированная сортировка: ключи и порядок столбцов задаются `determinism.sort` и `determinism.column_order` в конфиге
- Кодировка UTF-8, формат чисел по конфигу (`io.output.csv.float_format`)
- Контроль версий: фиксируйте версию пакета и конфиг (в теге файла/отчёте)

## QC-отчёты

- Метрики полноты, дубликатов, распределений
- Пример таблицы (читает CSV напрямую):

```table
:header-rows: 1
:file: docs/qc/enhanced_qc_summary.csv
```

## Корреляции

- Числовые/категориальные корреляции, экспорт в CSV
- Пример (файл из `docs/qc/` можно отобразить аналогично через table-reader)

## Воспроизводимость

- Фиксируйте `runtime.date_tag` для артефактов
- Используйте стабильную сортировку и явный `column_order`
- Храните копию конфига рядом с артефактами

## Структура создаваемых файлов (полная)

Ниже отражена фактическая структура, создаваемая при записи итоговых данных и включённых пост‑обработках (QC/Correlation).

```text
<output_dir>/
  data.csv                                    # Основной файл данных
  data_quality_report.csv                     # Базовый QC отчёт
  data_correlation_report.csv                 # Базовая корреляционная матрица
  data_quality_report_enhanced.csv            # Расширённый QC отчёт (если включено)
  data_quality_report_detailed/               # Детальные QC отчёты
    column_summary.csv
    pattern_coverage.csv
    top_values.csv
  data_correlation_report_enhanced/           # Расширенные корреляции (если включено)
    numeric_pearson.csv
    numeric_spearman.csv
    numeric_covariance.csv
    categorical_cramers_v.csv
    mixed_eta_squared.csv
    mixed_point_biserial.csv
    correlation_summary.csv
  data_correlation_report_detailed/           # Детальные корреляционные отчёты
    correlation_analysis.json
    correlation_insights.csv
```

См. подробные пояснения: `docs/AUTO_QC_CORRELATION_IMPLEMENTATION.md`.

## Мини‑тест детерминизма

Проверьте стабильность артефактов на одинаковом вводе.

```bash
# два последовательных запуска с фиксированным конфигом/seed
bioactivity-data-acquisition pipeline --config configs/config.yaml
cp data/output/bioactivities.csv run1.csv
bioactivity-data-acquisition pipeline --config configs/config.yaml
cp data/output/bioactivities.csv run2.csv

# сравнение байт-в-байт
python - <<'PY'
from pathlib import Path
assert Path('run1.csv').read_bytes() == Path('run2.csv').read_bytes()
print('Deterministic: OK')
PY
```

См. автотест: `tests/test_deterministic_output.py`.
