"""Демонстрация работы обеих функций: нормализации журналов и формирования ссылок."""

import sys
from pathlib import Path

# Добавляем src в путь для импорта
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

import pandas as pd
from library.tools.citation_formatter import add_citation_column
from library.tools.journal_normalizer import normalize_journal_columns


def test_combined_workflow():
    """Демонстрация полного workflow: нормализация + формирование ссылок."""
    
    print("=" * 80)
    print("ДЕМОНСТРАЦИЯ ПОЛНОГО WORKFLOW")
    print("=" * 80)
    print("1. Нормализация названий журналов")
    print("2. Формирование литературных ссылок")
    print("=" * 80)
    
    # Создаем тестовый DataFrame с различными форматами названий журналов
    df = pd.DataFrame({
        'document_chembl_id': ['CHEMBL1', 'CHEMBL2', 'CHEMBL3', 'CHEMBL4', 'CHEMBL5'],
        'journal': [
            'The Journal of Chemical Physics',  # С артиклем
            'J. Chem. Phys.',                   # Сокращения
            'Proceedings of the IEEE',          # Proceedings
            'Annales de Physique',              # Французский предлог
            'Nature'                            # Простое название
        ],
        'volume': ['612', '123', '45', '78', '580'],
        'issue': ['7940', '', '3', '12', ''],
        'first_page': ['100', '50', '200', '15', '300'],
        'last_page': ['105', '60', '250', '25', '350'],
        'pubmed_journal_title': [
            'IEEE Transactions on Pattern Analysis',
            'Bulletin-of Mathematical Biology',
            'Letters in Applied Microbiology',
            'Communications of the ACM',
            'Reports on Progress in Physics'
        ]
    })
    
    print("\nИСХОДНЫЕ ДАННЫЕ:")
    print("-" * 80)
    print(df[['document_chembl_id', 'journal', 'volume', 'issue', 'first_page', 'last_page']].to_string(index=False))
    
    # Шаг 1: Нормализация названий журналов
    print("\nШАГ 1: НОРМАЛИЗАЦИЯ НАЗВАНИЙ ЖУРНАЛОВ")
    print("-" * 80)
    df_normalized = normalize_journal_columns(df)
    
    print("После нормализации:")
    print(df_normalized[['document_chembl_id', 'journal', 'pubmed_journal_title']].to_string(index=False))
    
    # Шаг 2: Формирование литературных ссылок
    print("\nШАГ 2: ФОРМИРОВАНИЕ ЛИТЕРАТУРНЫХ ССЫЛОК")
    print("-" * 80)
    df_final = add_citation_column(df_normalized)
    
    print("Итоговый результат с литературными ссылками:")
    print(df_final[['document_chembl_id', 'journal', 'citation']].to_string(index=False))
    
    # Показываем примеры нормализации
    print("\nПРИМЕРЫ НОРМАЛИЗАЦИИ:")
    print("-" * 80)
    examples = [
        ("The Journal of Chemical Physics", "journal of chemical physics"),
        ("J. Chem. Phys.", "journal chemical physics"),
        ("Proceedings of the IEEE", "proceedings of the ieee"),
        ("Annales de Physique", "annales of physique"),
        ("IEEE Transactions on Pattern Analysis", "ieee transactions on pattern analysis"),
        ("Bulletin-of Mathematical Biology", "bulletin-of mathematical biology"),
        ("Letters in Applied Microbiology", "letters in applied microbiology"),
        ("Communications of the ACM", "communications of the acm"),
        ("Reports on Progress in Physics", "reports on progress in physics")
    ]
    
    for original, normalized in examples:
        print(f"'{original}' -> '{normalized}'")
    
    # Показываем примеры формирования ссылок
    print("\nПРИМЕРЫ ФОРМИРОВАНИЯ ССЫЛОК:")
    print("-" * 80)
    citation_examples = [
        ("journal of chemical physics", "612", "7940", "100", "105", "journal of chemical physics, 612 (7940). p. 100-105"),
        ("journal chemical physics", "123", "", "50", "60", "journal chemical physics, 123. p. 50-60"),
        ("proceedings of the ieee", "45", "3", "200", "250", "proceedings of the ieee, 45 (3). p. 200-250"),
        ("annales of physique", "78", "12", "15", "25", "annales of physique, 78 (12). p. 15-25"),
        ("nature", "580", "", "300", "350", "nature, 580. p. 300-350")
    ]
    
    for journal, volume, issue, first_page, last_page, expected in citation_examples:
        print(f"'{journal}' + vol:{volume} + iss:{issue} + p:{first_page}-{last_page}")
        print(f"  -> '{expected}'")
        print()
    
    print("=" * 80)
    print("WORKFLOW ЗАВЕРШЕН УСПЕШНО!")
    print("=" * 80)
    print("Результат:")
    print("- Названия журналов нормализованы для единообразия")
    print("- Сформированы литературные ссылки согласно стандартам")
    print("- Данные готовы для сохранения в CSV")
    print("=" * 80)


if __name__ == "__main__":
    test_combined_workflow()
