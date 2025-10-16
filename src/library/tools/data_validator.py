"""Утилита для валидации данных из различных источников."""

from __future__ import annotations

import pandas as pd
from typing import Any


def _is_empty_value(value: Any) -> bool:
    """Проверяет, является ли значение пустым (None, NaN, пустая строка)."""
    if value is None:
        return True
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _normalize_value(value: Any) -> str | None:
    """Нормализует значение для сравнения."""
    if _is_empty_value(value):
        return None
    return str(value).strip().lower()


def validate_doi_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Валидирует DOI из различных источников.
    
    Args:
        df: DataFrame с данными документов
        
    Returns:
        DataFrame с добавленными колонками invalid_doi и valid_doi
    """
    df_copy = df.copy()
    
    # Поля DOI из разных источников (только те, что есть в данных)
    doi_fields = ['chembl_doi', 'openalex_doi', 'pubmed_doi', 'semantic_scholar_doi']
    
    invalid_doi = []
    valid_doi = []
    
    for _, row in df_copy.iterrows():
        # Собираем непустые DOI значения
        non_empty_dois = []
        for field in doi_fields:
            if field in row and not _is_empty_value(row[field]):
                non_empty_dois.append(_normalize_value(row[field]))
        
        if len(non_empty_dois) == 0:
            # Нет ни одного DOI
            invalid_doi.append(True)
            valid_doi.append(pd.NA)
        elif len(non_empty_dois) == 1:
            # Только один DOI - считаем валидным
            invalid_doi.append(False)
            valid_doi.append(row['chembl_doi'] if not _is_empty_value(row['chembl_doi']) else non_empty_dois[0])
        else:
            # Несколько DOI - проверяем совпадения
            chembl_doi = _normalize_value(row['chembl_doi']) if not _is_empty_value(row['chembl_doi']) else None
            
            matches = 0
            mismatches = 0
            
            for doi in non_empty_dois:
                if chembl_doi is not None and doi == chembl_doi:
                    matches += 1
                elif chembl_doi is not None:
                    mismatches += 1
            
            # Если количество совпадений <= количества несовпадений - DOI невалидный
            if matches <= mismatches:
                invalid_doi.append(True)
                valid_doi.append(pd.NA)
            else:
                invalid_doi.append(False)
                valid_doi.append(row['chembl_doi'] if not _is_empty_value(row['chembl_doi']) else chembl_doi)
    
    df_copy['invalid_doi'] = invalid_doi
    df_copy['valid_doi'] = valid_doi
    
    return df_copy


def validate_journal_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Валидирует journal из различных источников.
    
    Args:
        df: DataFrame с данными документов
        
    Returns:
        DataFrame с добавленными колонками invalid_journal и valid_journal
    """
    df_copy = df.copy()
    
    # Поля journal из разных источников
    journal_fields = ['chembl_journal', 'pubmed_journal', 'semantic_scholar_journal']
    
    invalid_journal = []
    valid_journal = []
    
    for _, row in df_copy.iterrows():
        # Собираем непустые journal значения
        non_empty_journals = []
        for field in journal_fields:
            if field in row and not _is_empty_value(row[field]):
                non_empty_journals.append(_normalize_value(row[field]))
        
        if len(non_empty_journals) == 0:
            # Нет ни одного journal
            invalid_journal.append(True)
            valid_journal.append(pd.NA)
        elif len(non_empty_journals) == 1:
            # Только один journal - считаем валидным
            invalid_journal.append(False)
            valid_journal.append(row['chembl_journal'] if not _is_empty_value(row['chembl_journal']) else non_empty_journals[0])
        else:
            # Несколько journal - проверяем совпадения
            chembl_journal = _normalize_value(row['chembl_journal']) if not _is_empty_value(row['chembl_journal']) else None
            
            matches = 0
            mismatches = 0
            
            for journal in non_empty_journals:
                if chembl_journal is not None and journal == chembl_journal:
                    matches += 1
                elif chembl_journal is not None:
                    mismatches += 1
            
            # Если количество совпадений <= количества несовпадений - journal невалидный
            if matches <= mismatches:
                invalid_journal.append(True)
                valid_journal.append(pd.NA)
            else:
                invalid_journal.append(False)
                valid_journal.append(row['chembl_journal'] if not _is_empty_value(row['chembl_journal']) else chembl_journal)
    
    df_copy['invalid_journal'] = invalid_journal
    df_copy['valid_journal'] = valid_journal
    
    return df_copy


def validate_year_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Валидирует year из различных источников.
    
    Args:
        df: DataFrame с данными документов
        
    Returns:
        DataFrame с добавленными колонками invalid_year и valid_year
    """
    df_copy = df.copy()
    
    # Поля year из разных источников
    year_fields = ['chembl_year', 'openalex_year']
    
    invalid_year = []
    valid_year = []
    
    for _, row in df_copy.iterrows():
        # Собираем непустые year значения
        non_empty_years = []
        for field in year_fields:
            if field in row and not _is_empty_value(row[field]):
                try:
                    year_val = int(row[field])
                    non_empty_years.append(year_val)
                except (ValueError, TypeError):
                    pass
        
        if len(non_empty_years) == 0:
            # Нет ни одного year
            invalid_year.append(True)
            valid_year.append(pd.NA)
        elif len(non_empty_years) == 1:
            # Только один year - считаем валидным
            invalid_year.append(False)
            valid_year.append(row['chembl_year'] if not _is_empty_value(row['chembl_year']) else non_empty_years[0])
        else:
            # Несколько year - проверяем совпадения
            chembl_year = None
            if not _is_empty_value(row['chembl_year']):
                try:
                    chembl_year = int(row['chembl_year'])
                except (ValueError, TypeError):
                    pass
            
            matches = 0
            mismatches = 0
            
            for year in non_empty_years:
                if chembl_year is not None and year == chembl_year:
                    matches += 1
                elif chembl_year is not None:
                    mismatches += 1
            
            # Если количество совпадений <= количества несовпадений - year невалидный
            if matches <= mismatches:
                invalid_year.append(True)
                valid_year.append(pd.NA)
            else:
                invalid_year.append(False)
                valid_year.append(row['chembl_year'] if not _is_empty_value(row['chembl_year']) else chembl_year)
    
    df_copy['invalid_year'] = invalid_year
    df_copy['valid_year'] = valid_year
    
    return df_copy


def validate_volume_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Валидирует volume из различных источников.
    
    Args:
        df: DataFrame с данными документов
        
    Returns:
        DataFrame с добавленными колонками invalid_volume и valid_volume
    """
    df_copy = df.copy()
    
    # Поля volume из разных источников
    volume_fields = ['chembl_volume', 'pubmed_volume']
    
    invalid_volume = []
    valid_volume = []
    
    for _, row in df_copy.iterrows():
        # Собираем непустые volume значения
        non_empty_volumes = []
        for field in volume_fields:
            if field in row and not _is_empty_value(row[field]):
                non_empty_volumes.append(_normalize_value(row[field]))
        
        if len(non_empty_volumes) == 0:
            # Нет ни одного volume
            invalid_volume.append(True)
            valid_volume.append(pd.NA)
        elif len(non_empty_volumes) == 1:
            # Только один volume - считаем валидным
            invalid_volume.append(False)
            valid_volume.append(row['chembl_volume'] if not _is_empty_value(row['chembl_volume']) else non_empty_volumes[0])
        else:
            # Несколько volume - проверяем совпадения
            chembl_volume = _normalize_value(row['chembl_volume']) if not _is_empty_value(row['chembl_volume']) else None
            
            matches = 0
            mismatches = 0
            
            for volume in non_empty_volumes:
                if chembl_volume is not None and volume == chembl_volume:
                    matches += 1
                elif chembl_volume is not None:
                    mismatches += 1
            
            # Если количество совпадений <= количества несовпадений - volume невалидный
            if matches <= mismatches:
                invalid_volume.append(True)
                valid_volume.append(pd.NA)
            else:
                invalid_volume.append(False)
                valid_volume.append(row['chembl_volume'] if not _is_empty_value(row['chembl_volume']) else chembl_volume)
    
    df_copy['invalid_volume'] = invalid_volume
    df_copy['valid_volume'] = valid_volume
    
    return df_copy


def validate_issue_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Валидирует issue из различных источников.
    
    Args:
        df: DataFrame с данными документов
        
    Returns:
        DataFrame с добавленными колонками invalid_issue и valid_issue
    """
    df_copy = df.copy()
    
    # Поля issue из разных источников
    issue_fields = ['chembl_issue', 'pubmed_issue']
    
    invalid_issue = []
    valid_issue = []
    
    for _, row in df_copy.iterrows():
        # Собираем непустые issue значения
        non_empty_issues = []
        for field in issue_fields:
            if field in row and not _is_empty_value(row[field]):
                non_empty_issues.append(_normalize_value(row[field]))
        
        if len(non_empty_issues) == 0:
            # Нет ни одного issue
            invalid_issue.append(True)
            valid_issue.append(pd.NA)
        elif len(non_empty_issues) == 1:
            # Только один issue - считаем валидным
            invalid_issue.append(False)
            valid_issue.append(row['chembl_issue'] if not _is_empty_value(row['chembl_issue']) else non_empty_issues[0])
        else:
            # Несколько issue - проверяем совпадения
            chembl_issue = _normalize_value(row['chembl_issue']) if not _is_empty_value(row['chembl_issue']) else None
            
            matches = 0
            mismatches = 0
            
            for issue in non_empty_issues:
                if chembl_issue is not None and issue == chembl_issue:
                    matches += 1
                elif chembl_issue is not None:
                    mismatches += 1
            
            # Если количество совпадений <= количества несовпадений - issue невалидный
            if matches <= mismatches:
                invalid_issue.append(True)
                valid_issue.append(pd.NA)
            else:
                invalid_issue.append(False)
                valid_issue.append(row['chembl_issue'] if not _is_empty_value(row['chembl_issue']) else chembl_issue)
    
    df_copy['invalid_issue'] = invalid_issue
    df_copy['valid_issue'] = valid_issue
    
    return df_copy


def validate_all_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Валидирует все поля из различных источников.
    
    Args:
        df: DataFrame с данными документов
        
    Returns:
        DataFrame с добавленными колонками валидации
    """
    df_validated = df.copy()
    
    # Валидируем все поля по очереди
    df_validated = validate_doi_fields(df_validated)
    df_validated = validate_journal_fields(df_validated)
    df_validated = validate_year_fields(df_validated)
    df_validated = validate_volume_fields(df_validated)
    df_validated = validate_issue_fields(df_validated)
    
    return df_validated


__all__ = [
    "validate_doi_fields",
    "validate_journal_fields", 
    "validate_year_fields",
    "validate_volume_fields",
    "validate_issue_fields",
    "validate_all_fields",
]
