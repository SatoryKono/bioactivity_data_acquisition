"""Loading stage utilities for the ETL pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from structlog.stdlib import BoundLogger

from library.etl.qc import (
    build_correlation_matrix, 
    build_qc_report,
    build_enhanced_qc_report,
    build_enhanced_qc_detailed_reports,
    build_enhanced_correlation_analysis_report,
    build_enhanced_correlation_reports_df,
    build_correlation_insights_report
)
from library.io_.normalize import normalize_doi_advanced

if TYPE_CHECKING:  # pragma: no cover - type checking helpers
    from library.config import (
        CsvFormatSettings,
        DeterminismSettings,
        OutputSettings,
        ParquetFormatSettings,
        PostprocessSettings,
        QCValidationSettings,
    )


def _deterministic_order(
    df: pd.DataFrame,
    determinism: DeterminismSettings,
) -> pd.DataFrame:
    # Если это QC отчет, не применяем фильтрацию колонок
    if _is_qc_report(df):
        return df.reset_index(drop=True)
    
    # Сохраняем только колонки, указанные в column_order
    desired_order = [col for col in determinism.column_order if col in df.columns]
    # Исключаем лишние колонки - сохраняем только те, что указаны в конфигурации
    ordered = df[desired_order]
    
    # Фильтруем колонки для сортировки, оставляя только существующие
    sort_by = [col for col in (determinism.sort.by or ordered.columns.tolist()) if col in df.columns]
    
    # Если нет колонок для сортировки, возвращаем DataFrame как есть
    if not sort_by:
        return ordered
    
    from library.etl.transform import _resolve_ascending

    ascending = _resolve_ascending(sort_by, determinism.sort.ascending)
    return ordered.sort_values(
        sort_by,
        ascending=ascending,
        na_position=determinism.sort.na_position,
    )


def _normalize_dataframe(df: pd.DataFrame, determinism: DeterminismSettings | None = None, logger: BoundLogger | None = None) -> pd.DataFrame:
    """
    Нормализует DataFrame перед сохранением:
    - DOI-столбцы: специальная нормализация DOI
    - Строковые переменные: обрезка пробелов, приведение к нижнему регистру только для колонок из determinism.lowercase_columns
    - Пустые ячейки: заполнение NA
    - Числовые данные: нормализация
    - Логические данные: нормализация
    """
    if df.empty:
        return df.copy()
    
    df_normalized = df.copy()
    
    if logger is not None:
        logger.info("normalize_start", columns=list(df.columns), rows=len(df))
    
    # Определяем DOI-столбцы (case-insensitive), исключая служебные флаги валидации
    lower_names = {col: col.lower() for col in df_normalized.columns}
    doi_columns: list[str] = []
    for col, low in lower_names.items():
        # Не нормализуем булев флаг валидации DOI
        if low == 'invalid_doi':
            continue
        # Явные DOI-колонки и канонические имена
        if low in {
            'doi', 'document_doi', 'doi_key', 'valid_doi',
            'chembl_doi', 'crossref_doi', 'openalex_doi', 'pubmed_doi', 'semantic_scholar_doi'
        }:
            doi_columns.append(col)
        # Общий случай: поля, заканчивающиеся на _doi (кроме invalid_doi, уже исключили)
        elif low.endswith('_doi'):
            doi_columns.append(col)
    
    if logger is not None and doi_columns:
        logger.info("doi_normalization_detected", doi_columns=doi_columns)
    
    for column in df_normalized.columns:
        # Пропускаем столбец index - он не должен нормализоваться
        if column == 'index':
            continue
        
        # Специальная обработка DOI-столбцов
        if column in doi_columns:
            if logger is not None:
                logger.info("normalizing_doi_column", column=column)
            
            # Заменяем None на NA
            df_normalized[column] = df_normalized[column].replace([None], pd.NA)
            
            # Применяем продвинутую нормализацию DOI
            for idx in df_normalized.index:
                value = df_normalized.loc[idx, column]
                if pd.isna(value):
                    continue  # Пропускаем уже NA значения
                
                # Нормализуем DOI
                normalized_doi = normalize_doi_advanced(value)
                # Если DOI невалидный, устанавливаем pd.NA
                if normalized_doi is None:
                    df_normalized.loc[idx, column] = pd.NA
                else:
                    df_normalized.loc[idx, column] = normalized_doi
                
        elif df_normalized[column].dtype == 'object':  # Обычные строковые данные
            # Заменяем None на NA
            df_normalized[column] = df_normalized[column].replace([None], pd.NA)
            
            # Определяем, нужно ли приводить эту колонку к нижнему регистру
            should_lowercase = (
                determinism is not None and 
                determinism.lowercase_columns is not None and 
                column in determinism.lowercase_columns
            )
            
            # Нормализуем все значения (включая пустые строки)
            for idx in df_normalized.index:
                value = df_normalized.loc[idx, column]
                
                # Проверяем на NA только для скалярных значений
                try:
                    if pd.isna(value):
                        continue  # Пропускаем уже NA значения
                except (TypeError, ValueError):
                    # Если pd.isna не может обработать тип (например, список), продолжаем
                    pass
                
                # Конвертируем в строку и нормализуем
                str_value = str(value).strip()
                
                # Приводим к нижнему регистру только если колонка указана в конфигурации
                if should_lowercase:
                    str_value = str_value.lower()
                
                # Проверяем, является ли результат пустой строкой или специальным значением
                if str_value in ['', 'nan', 'none', 'null']:
                    df_normalized.loc[idx, column] = pd.NA
                else:
                    df_normalized.loc[idx, column] = str_value
            
        elif pd.api.types.is_numeric_dtype(df_normalized[column]):
            # Числовые данные - заменяем NaN на pd.NA для консистентности
            df_normalized[column] = df_normalized[column].replace([np.nan, np.inf, -np.inf], pd.NA)
            
        elif pd.api.types.is_bool_dtype(df_normalized[column]):
            # Логические данные - пропускаем нормализацию для всех булевых колонок
            # так как они уже содержат правильные значения
            pass
    
    if logger is not None:
        logger.info("normalize_complete", columns=list(df_normalized.columns))
    
    return df_normalized


def _csv_options(settings: CsvFormatSettings) -> dict[str, object]:
    options: dict[str, object] = {
        "index": False,
        "encoding": settings.encoding,
    }
    if settings.float_format is not None:
        options["float_format"] = settings.float_format
    if settings.date_format is not None:
        options["date_format"] = settings.date_format
    if settings.na_rep is not None:
        options["na_rep"] = settings.na_rep
    if settings.line_terminator is not None:
        options["line_terminator"] = settings.line_terminator
    return options


def _apply_qc_thresholds(
    qc_report: pd.DataFrame,
    df: pd.DataFrame,
    validation: QCValidationSettings,
) -> pd.DataFrame:
    row_count = len(df)
    thresholds: list[float | None] = []
    ratios: list[float | None] = []
    statuses: list[str] = []

    for metric, value in zip(qc_report["metric"], qc_report["value"], strict=False):
        threshold: float | None = None
        ratio: float | None = None
        status = "pass"
        numeric_value = float(value)

        if metric in {"missing_document_chembl_id", "missing_doi", "missing_title"}:
            threshold = validation.max_missing_fraction
            ratio = numeric_value / row_count if row_count else 0.0
            if ratio > threshold:
                status = "fail"
        elif metric == "duplicates":
            threshold = validation.max_duplicate_fraction
            ratio = numeric_value / row_count if row_count else 0.0
            if ratio > threshold:
                status = "fail"

        thresholds.append(threshold)
        ratios.append(ratio)
        statuses.append(status)

    qc_with_thresholds = qc_report.assign(
        threshold=thresholds,
        ratio=ratios,
        status=statuses,
    )

    summary_status = (
        "fail"
        if any(
            status == "fail" and threshold is not None
            for status, threshold in zip(statuses, thresholds, strict=False)
        )
        else "pass"
    )
    summary = pd.DataFrame(
        [
            {
                "metric": "qc_passed",
                "value": summary_status == "pass",
                "threshold": None,
                "ratio": None,
                "status": summary_status,
            }
        ]
    )
    return pd.concat([qc_with_thresholds, summary], ignore_index=True)


def write_deterministic_csv(
    df: pd.DataFrame,
    destination: Path,
    logger: BoundLogger | None = None,
    *,
    determinism: DeterminismSettings | None = None,
    output: OutputSettings | None = None,
    postprocess: PostprocessSettings | None = None,
) -> Path:
    """Persist data to disk in a deterministic order using atomic writes."""

    from library.config import (
        CsvFormatSettings as _CsvFormatSettings,
    )
    from library.config import (
        DeterminismSettings as _DeterminismSettings,
    )
    from library.config import (
        ParquetFormatSettings as _ParquetFormatSettings,
    )
    from library.io_.atomic_writes import atomic_write_context

    determinism = determinism or _DeterminismSettings()
    csv_settings: CsvFormatSettings
    parquet_settings: ParquetFormatSettings
    file_format = "csv"

    if output is not None:
        csv_settings = output.csv
        parquet_settings = output.parquet
        file_format = output.format
    else:  # pragma: no cover - fallback for direct usage
        csv_settings = _CsvFormatSettings()
        parquet_settings = _ParquetFormatSettings()

    if df.empty:
        df_to_write = df.copy()
    else:
        # Добавляем столбец index с порядковыми номерами строк (начиная с 0)
        # Но только если это не QC отчет (который уже имеет структуру metric/value)
        df_to_write = df.copy()
        if not _is_qc_report(df_to_write):
            # Проверяем, существует ли уже колонка index
            if 'index' not in df_to_write.columns:
                df_to_write.insert(0, 'index', range(len(df_to_write)))
                if logger is not None:
                    logger.info("index_column_added", columns_before=len(df.columns), columns_after=len(df_to_write.columns), first_columns=list(df_to_write.columns[:5]))
            else:
                if logger is not None:
                    logger.info("index_column_exists", columns=len(df_to_write.columns), first_columns=list(df_to_write.columns[:5]))
        
        # Применяем детерминистический порядок колонок после добавления index
        df_to_write = _deterministic_order(df_to_write, determinism)
        if logger is not None:
            logger.info("deterministic_order_applied", columns_after=len(df_to_write.columns), first_columns=list(df_to_write.columns[:5]))
        
        # Нормализуем данные перед сохранением
        df_to_write = _normalize_dataframe(df_to_write, determinism=determinism, logger=logger)

    # Очищаем старые backup файлы перед записью
    from library.io_.atomic_writes import cleanup_backups
    backup_count = cleanup_backups(destination.parent)
    if backup_count > 0 and logger is not None:
        logger.info("cleaned_up_backups", count=backup_count, directory=str(destination.parent))

    # Use atomic writes for safe file operations
    with atomic_write_context(destination, logger=logger) as temp_path:
        if file_format == "parquet":
            df_to_write.to_parquet(temp_path, index=False, compression=parquet_settings.compression)
        else:
            options = _csv_options(csv_settings)
            df_to_write.to_csv(temp_path, **options)

    if logger is not None:
        logger.info("load_complete", path=str(destination), rows=len(df_to_write))
    
    # Автоматически генерируем и сохраняем QC и корреляционные таблицы
    # Но только для основных данных, не для самих QC отчетов и корреляций
    if not df.empty and not _is_report_file(destination):
        _auto_generate_qc_and_correlation_reports(
            df_to_write, 
            destination, 
            output, 
            postprocess,
            logger=logger
        )
    
    return destination


def _is_qc_report(df: pd.DataFrame) -> bool:
    """Проверяет, является ли DataFrame QC отчетом."""
    # QC отчеты имеют структуру с колонками metric и value
    return (
        len(df.columns) == 2 and 
        'metric' in df.columns and 
        'value' in df.columns
    )


def _is_report_file(file_path: Path) -> bool:
    """Проверяет, является ли файл отчетом (QC или корреляционным)."""
    file_name = file_path.name.lower()
    
    # Проверяем паттерны имен файлов отчетов
    report_patterns = [
        '_quality_report.csv',
        '_quality_report_enhanced.csv',
        '_quality_report_detailed',
        '_correlation_report.csv',
        '_correlation_analysis.csv',
        '_correlation_insights.csv',
        '_qc.csv',
        '_corr.csv'
    ]
    
    return any(pattern in file_name for pattern in report_patterns)


def _auto_generate_qc_and_correlation_reports(
    df: pd.DataFrame,
    data_path: Path,
    output: OutputSettings | None = None,
    postprocess: PostprocessSettings | None = None,
    logger: BoundLogger | None = None,
) -> None:
    """Автоматически генерирует и сохраняет QC и корреляционные отчеты."""
    
    if df.empty:
        return
    
    # Не генерируем отчеты для самих QC отчетов и корреляций
    if _is_qc_report(df) or _is_report_file(data_path):
        if logger is not None:
            logger.info("skip_report_generation", reason="input_is_report", path=str(data_path))
        return
    
    # Проверяем настройки постобработки
    from library.config import PostprocessSettings as _PostprocessSettings
    postprocess = postprocess or _PostprocessSettings()
    
    # Если ни QC, ни корреляция не включены, пропускаем генерацию отчетов
    if not postprocess.qc.enabled and not postprocess.correlation.enabled:
        if logger is not None:
            logger.info("skip_report_generation", reason="postprocess_disabled", 
                       qc_enabled=postprocess.qc.enabled, 
                       correlation_enabled=postprocess.correlation.enabled)
        return
    
    # Импортируем функции для генерации отчетов
    from .qc import (
        build_qc_report,
        build_correlation_matrix,
        build_enhanced_qc_report,
        build_enhanced_qc_detailed_reports,
        build_enhanced_correlation_analysis_report,
        build_enhanced_correlation_reports_df,
        build_correlation_insights_report
    )
    
    # Настройки по умолчанию
    from library.config import (
        CsvFormatSettings as _CsvFormatSettings,
        ParquetFormatSettings as _ParquetFormatSettings,
        QCValidationSettings as _QCValidationSettings,
    )
    
    csv_settings: CsvFormatSettings
    parquet_settings: ParquetFormatSettings
    file_format = "csv"
    
    if output is not None:
        csv_settings = output.csv
        parquet_settings = output.parquet
        file_format = output.format
    else:
        csv_settings = _CsvFormatSettings()
        parquet_settings = _ParquetFormatSettings()
    
    validation = _QCValidationSettings()
    
    # Создаем пути для QC и корреляционных отчетов
    data_dir = data_path.parent
    data_stem = data_path.stem
    
    # Базовые пути для QC и корреляций
    qc_path = data_dir / f"{data_stem}_quality_report.csv"
    corr_path = data_dir / f"{data_stem}_correlation_report.csv"
    
    if logger is not None:
        logger.info("auto_qc_corr_start", 
                   qc_path=str(qc_path), 
                   corr_path=str(corr_path),
                   data_rows=len(df))
    
    try:
        # 1. Генерируем базовый QC отчет (только если включен)
        if postprocess.qc.enabled:
            qc_report = build_qc_report(df)
            qc_report = _apply_qc_thresholds(qc_report, df, validation)
            
            qc_path.parent.mkdir(parents=True, exist_ok=True)
            if file_format == "parquet":
                qc_report.to_parquet(qc_path.with_suffix('.parquet'), index=False, compression=parquet_settings.compression)
            else:
                qc_report.to_csv(qc_path, **_csv_options(csv_settings))
            
            if logger is not None:
                logger.info("auto_qc_basic_saved", path=str(qc_path))
        
        # 2. Генерируем базовую корреляционную матрицу (только если включена)
        if postprocess.correlation.enabled:
            correlation = build_correlation_matrix(df)
            
            corr_path.parent.mkdir(parents=True, exist_ok=True)
            if file_format == "parquet":
                correlation.to_parquet(corr_path.with_suffix('.parquet'), index=False, compression=parquet_settings.compression)
            else:
                correlation.to_csv(corr_path, **_csv_options(csv_settings))
            
            if logger is not None:
                logger.info("auto_corr_basic_saved", path=str(corr_path))
        
        # 3. Генерируем расширенные QC отчеты (только если включены)
        if postprocess.qc.enabled and hasattr(postprocess.qc, 'enhanced') and postprocess.qc.enhanced:
            enhanced_qc_path = data_dir / f"{data_stem}_quality_report_enhanced.csv"
            detailed_qc_path = data_dir / f"{data_stem}_quality_report_detailed"
            
            enhanced_report = build_enhanced_qc_report(df, logger=logger)
            if file_format == "parquet":
                enhanced_report.to_parquet(enhanced_qc_path.with_suffix('.parquet'), index=False, compression=parquet_settings.compression)
            else:
                enhanced_report.to_csv(enhanced_qc_path, **_csv_options(csv_settings))
            
            # Детальные отчеты
            detailed_reports = build_enhanced_qc_detailed_reports(df, logger=logger)
            detailed_qc_path.mkdir(parents=True, exist_ok=True)
            
            for report_name, report_df in detailed_reports.items():
                report_path = detailed_qc_path / f"{report_name}.csv"
                if file_format == "parquet":
                    report_df.to_parquet(report_path.with_suffix('.parquet'), index=False, compression=parquet_settings.compression)
                else:
                    report_df.to_csv(report_path, **_csv_options(csv_settings))
            
            if logger is not None:
                logger.info("auto_qc_enhanced_saved", 
                           enhanced_path=str(enhanced_qc_path),
                           detailed_path=str(detailed_qc_path))
        
        # 4. Генерируем расширенные корреляционные отчеты (только если включены)
        if postprocess.correlation.enabled and hasattr(postprocess.correlation, 'enhanced') and postprocess.correlation.enhanced:
            enhanced_corr_path = data_dir / f"{data_stem}_correlation_report_enhanced"
            detailed_corr_path = data_dir / f"{data_stem}_correlation_report_detailed"
            
            # Расширенные корреляционные отчеты
            enhanced_corr_reports = build_enhanced_correlation_reports_df(df, logger=logger)
            enhanced_corr_path.mkdir(parents=True, exist_ok=True)
            
            for report_name, report_df in enhanced_corr_reports.items():
                if not report_df.empty:
                    report_path = enhanced_corr_path / f"{report_name}.csv"
                    if file_format == "parquet":
                        report_df.to_parquet(report_path.with_suffix('.parquet'), index=True, compression=parquet_settings.compression)
                    else:
                        report_df.to_csv(report_path, **_csv_options(csv_settings))
            
            # Детальные корреляционные отчеты
            detailed_corr_analysis = build_enhanced_correlation_analysis_report(df, logger=logger)
            detailed_corr_path.mkdir(parents=True, exist_ok=True)
            
            # Сохраняем анализ в JSON формате
            import json
            analysis_path = detailed_corr_path / "correlation_analysis.json"
            with open(analysis_path, 'w', encoding='utf-8') as f:
                # Преобразуем numpy типы в JSON-совместимые
                def convert_numpy(obj):
                    if isinstance(obj, np.integer):
                        return int(obj)
                    elif isinstance(obj, np.floating):
                        return float(obj)
                    elif isinstance(obj, np.ndarray):
                        return obj.tolist()
                    elif isinstance(obj, pd.DataFrame):
                        return obj.to_dict()
                    return obj
                
                json_analysis = json.loads(json.dumps(detailed_corr_analysis, default=convert_numpy, indent=2))
                json.dump(json_analysis, f, ensure_ascii=False, indent=2)
            
            # Создаем отчет с инсайтами
            insights = build_correlation_insights_report(df, logger=logger)
            if insights:
                insights_df = pd.DataFrame(insights)
                insights_path = detailed_corr_path / "correlation_insights.csv"
                if file_format == "parquet":
                    insights_df.to_parquet(insights_path.with_suffix('.parquet'), index=False, compression=parquet_settings.compression)
                else:
                    insights_df.to_csv(insights_path, **_csv_options(csv_settings))
            
            if logger is not None:
                logger.info("auto_corr_enhanced_saved",
                           enhanced_path=str(enhanced_corr_path),
                           detailed_path=str(detailed_corr_path))
        
        if logger is not None:
            # Собираем список созданных файлов для логирования
            created_files = []
            if postprocess.qc.enabled:
                created_files.append(str(qc_path))
                if hasattr(postprocess.qc, 'enhanced') and postprocess.qc.enhanced:
                    created_files.extend([str(enhanced_qc_path), str(detailed_qc_path)])
            if postprocess.correlation.enabled:
                created_files.append(str(corr_path))
                if hasattr(postprocess.correlation, 'enhanced') and postprocess.correlation.enhanced:
                    created_files.extend([str(enhanced_corr_path), str(detailed_corr_path)])
            
            logger.info("auto_qc_corr_complete", created_files=created_files)
    
    except Exception as e:
        if logger is not None:
            logger.error("auto_qc_corr_error", error=str(e), error_type=type(e).__name__)
        # Не прерываем основной процесс, только логируем ошибку


def write_qc_artifacts(
    df: pd.DataFrame,
    qc_path: Path,
    corr_path: Path,
    *,
    output: OutputSettings | None = None,
    validation: QCValidationSettings | None = None,
    postprocess: PostprocessSettings | None = None,
    logger: BoundLogger | None = None,
) -> None:
    """Write QC and correlation reports according to configuration."""

    from library.config import (
        CsvFormatSettings as _CsvFormatSettings,
    )
    from library.config import (
        ParquetFormatSettings as _ParquetFormatSettings,
    )
    from library.config import (
        PostprocessSettings as _PostprocessSettings,
    )
    from library.config import (
        QCValidationSettings as _QCValidationSettings,
    )

    csv_settings: CsvFormatSettings
    parquet_settings: ParquetFormatSettings
    file_format = "csv"

    if output is not None:
        csv_settings = output.csv
        parquet_settings = output.parquet
        file_format = output.format
    else:  # pragma: no cover - fallback for direct usage
        csv_settings = _CsvFormatSettings()
        parquet_settings = _ParquetFormatSettings()

    validation = validation or _QCValidationSettings()
    postprocess = postprocess or _PostprocessSettings()

    if not postprocess.qc.enabled:
        return

    # Базовая QC отчетность
    qc_report = build_qc_report(df)
    qc_report = _apply_qc_thresholds(qc_report, df, validation)
    qc_path.parent.mkdir(parents=True, exist_ok=True)

    if file_format == "parquet":
        qc_report.to_parquet(qc_path, index=False, compression=parquet_settings.compression)
    else:
        qc_report.to_csv(qc_path, **_csv_options(csv_settings))

    # Расширенная QC отчетность
    if hasattr(postprocess.qc, 'enhanced') and postprocess.qc.enhanced:
        enhanced_qc_path = qc_path.parent / f"{qc_path.stem}_enhanced.csv"
        detailed_qc_path = qc_path.parent / f"{qc_path.stem}_detailed"
        
        # Создаем расширенный отчет
        enhanced_report = build_enhanced_qc_report(df, logger=logger)
        if file_format == "parquet":
            enhanced_report.to_parquet(enhanced_qc_path.with_suffix('.parquet'), index=False, compression=parquet_settings.compression)
        else:
            enhanced_report.to_csv(enhanced_qc_path, **_csv_options(csv_settings))
        
        # Создаем детальные отчеты
        detailed_reports = build_enhanced_qc_detailed_reports(df, logger=logger)
        detailed_qc_path.mkdir(parents=True, exist_ok=True)
        
        for report_name, report_df in detailed_reports.items():
            report_path = detailed_qc_path / f"{report_name}.csv"
            if file_format == "parquet":
                report_df.to_parquet(report_path.with_suffix('.parquet'), index=False, compression=parquet_settings.compression)
            else:
                report_df.to_csv(report_path, **_csv_options(csv_settings))

    if not postprocess.correlation.enabled:
        return

    # Базовая корреляционная матрица
    correlation = build_correlation_matrix(df)
    corr_path.parent.mkdir(parents=True, exist_ok=True)
    if file_format == "parquet":
        correlation.to_parquet(corr_path, index=False, compression=parquet_settings.compression)
    else:
        correlation.to_csv(corr_path, **_csv_options(csv_settings))

    # Расширенный корреляционный анализ
    if hasattr(postprocess.correlation, 'enhanced') and postprocess.correlation.enhanced:
        enhanced_corr_path = corr_path.parent / f"{corr_path.stem}_enhanced"
        detailed_corr_path = corr_path.parent / f"{corr_path.stem}_detailed"
        
        # Создаем расширенные корреляционные отчеты
        enhanced_corr_reports = build_enhanced_correlation_reports_df(df, logger=logger)
        enhanced_corr_path.mkdir(parents=True, exist_ok=True)
        
        for report_name, report_df in enhanced_corr_reports.items():
            if not report_df.empty:
                report_path = enhanced_corr_path / f"{report_name}.csv"
                if file_format == "parquet":
                    report_df.to_parquet(report_path.with_suffix('.parquet'), index=True, compression=parquet_settings.compression)
                else:
                    report_df.to_csv(report_path, **_csv_options(csv_settings))
        
        # Создаем детальные корреляционные отчеты
        detailed_corr_analysis = build_enhanced_correlation_analysis_report(df, logger=logger)
        detailed_corr_path.mkdir(parents=True, exist_ok=True)
        
        # Сохраняем анализ в JSON формате для сложных структур
        import json
        analysis_path = detailed_corr_path / "correlation_analysis.json"
        with open(analysis_path, 'w', encoding='utf-8') as f:
            # Преобразуем numpy типы в JSON-совместимые
            def convert_numpy(obj):
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif isinstance(obj, pd.DataFrame):
                    return obj.to_dict()
                return obj
            
            json_analysis = json.loads(json.dumps(detailed_corr_analysis, default=convert_numpy, indent=2))
            json.dump(json_analysis, f, ensure_ascii=False, indent=2)
        
        # Создаем отчет с инсайтами
        insights = build_correlation_insights_report(df, logger=logger)
        if insights:
            insights_df = pd.DataFrame(insights)
            insights_path = detailed_corr_path / "correlation_insights.csv"
            if file_format == "parquet":
                insights_df.to_parquet(insights_path.with_suffix('.parquet'), index=False, compression=parquet_settings.compression)
            else:
                insights_df.to_csv(insights_path, **_csv_options(csv_settings))


__all__ = ["write_deterministic_csv", "write_qc_artifacts"]
