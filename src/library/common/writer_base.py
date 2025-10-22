"""Base writer for standardized artifact generation."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from library.io_.atomic_writes import atomic_write_context

logger = logging.getLogger(__name__)


class BaseWriter:
    """Базовый writer для всех пайплайнов.
    
    Обеспечивает единообразное именование и запись артефактов
    для всех ETL пайплайнов согласно стандарту:
    
    - <stem>_<date_tag>.csv              # Основной результат
    - <stem>_<date_tag>.meta.yaml        # Метаданные
    - <stem>_<date_tag>_qc_summary.csv   # QC summary
    - <stem>_<date_tag>_qc_detailed.csv  # QC detailed (опционально)
    - <stem>_<date_tag>_rejected.csv     # Отклоненные записи (опционально)
    - <stem>_<date_tag>_correlation.csv  # Корреляционный анализ (опционально)
    """
    
    @staticmethod
    def write_outputs(
        result: Any,  # ETLResult from pipeline_base
        output_dir: Path,
        stem: str,
        date_tag: str,
        config: Any,
    ) -> dict[str, Path]:
        """Запись всех артефактов пайплайна.
        
        Args:
            result: Результат ETL процесса (ETLResult)
            output_dir: Директория для записи
            stem: Базовое имя файлов (documents, targets, assays, activities, testitems)
            date_tag: Тег даты (YYYYMMDD)
            config: Конфигурация пайплайна
            
        Returns:
            Словарь с именами артефактов и путями к файлам
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Writing outputs to {output_dir} with stem '{stem}' and date_tag '{date_tag}'")
        
        outputs = {}
        
        # Основной CSV
        main_path = output_dir / f"{stem}_{date_tag}.csv"
        BaseWriter._write_csv_atomic(
            result.data,
            main_path,
            config,
            description="main data"
        )
        outputs['main'] = main_path
        logger.info(f"Written main data: {main_path}")
        
        # QC summary
        qc_summary_path = output_dir / f"{stem}_{date_tag}_qc_summary.csv"
        BaseWriter._write_csv_atomic(
            result.qc_summary,
            qc_summary_path,
            config,
            description="QC summary"
        )
        outputs['qc_summary'] = qc_summary_path
        logger.info(f"Written QC summary: {qc_summary_path}")
        
        # QC detailed (опционально)
        if result.qc_detailed is not None and not result.qc_detailed.empty:
            qc_detailed_path = output_dir / f"{stem}_{date_tag}_qc_detailed.csv"
            BaseWriter._write_csv_atomic(
                result.qc_detailed,
                qc_detailed_path,
                config,
                description="QC detailed"
            )
            outputs['qc_detailed'] = qc_detailed_path
            logger.info(f"Written QC detailed: {qc_detailed_path}")
        
        # Rejected (опционально)
        if result.rejected is not None and not result.rejected.empty:
            rejected_path = output_dir / f"{stem}_{date_tag}_rejected.csv"
            BaseWriter._write_csv_atomic(
                result.rejected,
                rejected_path,
                config,
                description="rejected data"
            )
            outputs['rejected'] = rejected_path
            logger.info(f"Written rejected data: {rejected_path}")
        
        # Metadata YAML
        meta_path = output_dir / f"{stem}_{date_tag}.meta.yaml"
        BaseWriter._write_yaml_atomic(
            result.meta,
            meta_path,
            description="metadata"
        )
        outputs['metadata'] = meta_path
        logger.info(f"Written metadata: {meta_path}")
        
        # Correlation (опционально)
        if result.correlation_analysis is not None:
            corr_path = output_dir / f"{stem}_{date_tag}_correlation.csv"
            BaseWriter._write_correlation_analysis(
                result.correlation_analysis,
                result.correlation_reports,
                corr_path,
                config
            )
            outputs['correlation'] = corr_path
            logger.info(f"Written correlation analysis: {corr_path}")
        
        logger.info(f"Successfully wrote {len(outputs)} artifacts")
        return outputs
    
    @staticmethod
    def _write_csv_atomic(
        df: pd.DataFrame,
        path: Path,
        config: Any,
        description: str = "data"
    ) -> None:
        """Атомарная запись CSV файла с применением настроек детерминизма.
        
        Args:
            df: DataFrame для записи
            path: Путь к файлу
            config: Конфигурация с настройками детерминизма
            description: Описание данных для логирования
        """
        if df.empty:
            logger.warning(f"Writing empty DataFrame to {path}")
        
        # Применяем настройки детерминизма если доступны
        if hasattr(config, 'determinism') and config.determinism is not None:
            df = BaseWriter._apply_determinism(df, config.determinism)
        
        # Настройки CSV из конфигурации
        csv_options = BaseWriter._get_csv_options(config)
        
        with atomic_write_context(path) as temp_path:
            df.to_csv(temp_path, index=False, **csv_options)
    
    @staticmethod
    def _write_yaml_atomic(
        data: dict[str, Any],
        path: Path,
        description: str = "metadata"
    ) -> None:
        """Атомарная запись YAML файла.
        
        Args:
            data: Данные для записи
            path: Путь к файлу
            description: Описание данных для логирования
        """
        with atomic_write_context(path) as temp_path:
            with open(temp_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
    
    @staticmethod
    def _write_correlation_analysis(
        correlation_analysis: dict[str, Any],
        correlation_reports: dict[str, pd.DataFrame] | None,
        path: Path,
        config: Any
    ) -> None:
        """Запись корреляционного анализа.
        
        Args:
            correlation_analysis: Результаты корреляционного анализа
            correlation_reports: Корреляционные отчеты
            path: Путь к файлу
            config: Конфигурация
        """
        # Объединяем все корреляционные данные в один DataFrame
        correlation_data = []
        
        # Добавляем метрики корреляционного анализа
        if correlation_analysis:
            for metric, value in correlation_analysis.items():
                correlation_data.append({
                    'metric': metric,
                    'value': value,
                    'type': 'analysis'
                })
        
        # Добавляем данные из отчетов
        if correlation_reports:
            for report_name, report_df in correlation_reports.items():
                if not report_df.empty:
                    # Добавляем информацию о типе отчета
                    report_df_copy = report_df.copy()
                    report_df_copy['report_type'] = report_name
                    correlation_data.append(report_df_copy)
        
        if correlation_data:
            # Объединяем все данные
            if len(correlation_data) == 1 and isinstance(correlation_data[0], pd.DataFrame):
                combined_df = correlation_data[0]
            else:
                # Создаем DataFrame из словарей и объединяем с DataFrame'ами
                dict_data = [item for item in correlation_data if isinstance(item, dict)]
                df_data = [item for item in correlation_data if isinstance(item, pd.DataFrame)]
                
                if dict_data:
                    dict_df = pd.DataFrame(dict_data)
                    if df_data:
                        combined_df = pd.concat([dict_df] + df_data, ignore_index=True)
                    else:
                        combined_df = dict_df
                elif df_data:
                    combined_df = pd.concat(df_data, ignore_index=True)
                else:
                    combined_df = pd.DataFrame()
            
            BaseWriter._write_csv_atomic(combined_df, path, config, "correlation analysis")
        else:
            # Создаем пустой файл
            BaseWriter._write_csv_atomic(pd.DataFrame(), path, config, "correlation analysis")
    
    @staticmethod
    def _apply_determinism(df: pd.DataFrame, determinism_config: Any) -> pd.DataFrame:
        """Применение настроек детерминизма к DataFrame.
        
        Args:
            df: DataFrame для обработки
            determinism_config: Конфигурация детерминизма
            
        Returns:
            Обработанный DataFrame
        """
        if df.empty:
            return df
        
        result_df = df.copy()
        
        # Применяем сортировку
        if hasattr(determinism_config, 'sort') and determinism_config.sort:
            sort_config = determinism_config.sort
            
            if hasattr(sort_config, 'by') and sort_config.by:
                sort_columns = [col for col in sort_config.by if col in result_df.columns]
                if sort_columns:
                    ascending = getattr(sort_config, 'ascending', [True] * len(sort_columns))
                    if len(ascending) != len(sort_columns):
                        ascending = [True] * len(sort_columns)
                    
                    na_position = getattr(sort_config, 'na_position', 'last')
                    result_df = result_df.sort_values(
                        sort_columns,
                        ascending=ascending,
                        na_position=na_position
                    ).reset_index(drop=True)
        
        # Применяем порядок колонок
        if hasattr(determinism_config, 'column_order') and determinism_config.column_order:
            available_columns = [col for col in determinism_config.column_order if col in result_df.columns]
            if available_columns:
                # Добавляем колонки, которых нет в column_order, но есть в DataFrame
                missing_columns = [col for col in result_df.columns if col not in available_columns]
                final_columns = available_columns + missing_columns
                result_df = result_df[final_columns]
        
        return result_df
    
    @staticmethod
    def _get_csv_options(config: Any) -> dict[str, Any]:
        """Получение настроек CSV из конфигурации.
        
        Args:
            config: Конфигурация пайплайна
            
        Returns:
            Словарь с настройками для pandas.to_csv()
        """
        options = {}
        
        # Настройки из io.output.csv
        if hasattr(config, 'io') and hasattr(config.io, 'output') and hasattr(config.io.output, 'csv'):
            csv_config = config.io.output.csv
            
            if hasattr(csv_config, 'encoding'):
                options['encoding'] = csv_config.encoding
            
            if hasattr(csv_config, 'float_format'):
                options['float_format'] = csv_config.float_format
            
            if hasattr(csv_config, 'date_format'):
                options['date_format'] = csv_config.date_format
            
            if hasattr(csv_config, 'na_rep'):
                options['na_rep'] = csv_config.na_rep
            
            if hasattr(csv_config, 'line_terminator'):
                options['lineterminator'] = csv_config.line_terminator
        
        return options
