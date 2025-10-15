"""Демонстрация работы функции нормализации названий журналов."""

import sys
from pathlib import Path

# Добавляем src в путь для импорта
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

import pandas as pd
from library.tools.journal_normalizer import (
    normalize_journal_name, 
    normalize_journal_columns,
    get_journal_columns
)


def test_normalize_journal_name():
    """Тестирование функции normalize_journal_name."""
    
    print("=" * 80)
    print("Тестирование функции normalize_journal_name")
    print("=" * 80)
    
    test_cases = [
        # (input, expected, description)
        ("The Journal of Chemical Physics", "journal of chemical physics", "Базовый пример с артиклем"),
        ("J. Chem. Phys.", "journal chemical physics", "Сокращения с точками"),
        ("Proceedings of the IEEE", "proceedings of the ieee", "Proceedings"),
        ("IEEE Transactions on Pattern Analysis", "ieee transactions on pattern analysis", "Transactions"),
        ("Annales de Physique", "annales of physique", "Французский предлог"),
        ("Revista de Biologia", "revista of biologia", "Испанский предлог"),
        ("Letters in Applied Microbiology", "letters in applied microbiology", "Letters"),
        ("Bulletin-of Mathematical Biology", "bulletin-of mathematical biology", "Дефис"),
        ("Journal of the American Chemical Society", "journal of the american chemical society", "С артиклем в середине"),
        ("Nature Reviews Drug Discovery", "nature reviews drug discovery", "Reviews"),
        ("Communications of the ACM", "communications of the acm", "Communications"),
        ("Reports on Progress in Physics", "reports on progress in physics", "Reports"),
        ("Annals of Mathematics", "annals of mathematics", "Annals"),
        ("Magazine of Concrete Research", "magazine of concrete research", "Magazine"),
        ("Newsletter of the Royal Society", "newsletter of the royal society", "Newsletter"),
        ("Notes and Records", "notes and records", "Notes"),
        ("  Nature  ", "nature", "Пробелы"),
        ("\"Science\"", "science", "Кавычки"),
        ("Cell.", "cell", "Точка в конце"),
        ("Journal & Science", "journal and science", "Амперсанд"),
        ("Journal + Science", "journal and science", "Плюс"),
        ("resume", "resume", "Без диакритики"),
        ("Part II", "part 2", "Римские цифры"),
        ("", None, "Пустая строка"),
        (None, None, "None"),
        ("   ", None, "Только пробелы"),
    ]
    
    passed = 0
    failed = 0
    
    for i, (input_text, expected, description) in enumerate(test_cases, 1):
        result = normalize_journal_name(input_text)
        status = "PASS" if result == expected else "FAIL"
        
        if status == "PASS":
            passed += 1
        else:
            failed += 1
        
        print(f"\nТест {i}: {description}")
        print(f"Вход:      '{input_text}'")
        print(f"Результат: '{result}'")
        print(f"Ожидается: '{expected}'")
        print(f"Статус:    {status}")
    
    print(f"\n" + "=" * 80)
    print(f"ИТОГО: {passed} PASS, {failed} FAIL")
    print("=" * 80)


def test_normalize_journal_columns():
    """Тестирование функции normalize_journal_columns."""
    
    print("\n" + "=" * 80)
    print("Тестирование функции normalize_journal_columns")
    print("=" * 80)
    
    # Создаем тестовый DataFrame
    df = pd.DataFrame({
        'document_chembl_id': ['CHEMBL1', 'CHEMBL2', 'CHEMBL3', 'CHEMBL4'],
        'journal': ['The Journal of Chemical Physics', 'J. Chem. Phys.', 'Nature', 'Science'],
        'pubmed_journal_title': ['Proceedings of the IEEE', 'IEEE Transactions', 'Cell', 'Nature Reviews'],
        'chembl_journal': ['Annales de Physique', 'Revista de Biologia', 'Letters in Applied Microbiology', 'Bulletin-of Mathematical Biology'],
        'other_column': ['value1', 'value2', 'value3', 'value4']  # Не должна нормализоваться
    })
    
    print("\nИсходный DataFrame:")
    print(df[['document_chembl_id', 'journal', 'pubmed_journal_title', 'chembl_journal']])
    
    # Нормализуем колонки
    result = normalize_journal_columns(df)
    
    print("\nDataFrame после нормализации:")
    print(result[['document_chembl_id', 'journal', 'pubmed_journal_title', 'chembl_journal']])
    
    # Проверяем, что колонка other_column не изменилась
    other_unchanged = (df['other_column'] == result['other_column']).all()
    print(f"\nКолонка 'other_column' не изменилась: {'PASS' if other_unchanged else 'FAIL'}")


def test_get_journal_columns():
    """Тестирование функции get_journal_columns."""
    
    print("\n" + "=" * 80)
    print("Тестирование функции get_journal_columns")
    print("=" * 80)
    
    df = pd.DataFrame({
        'document_chembl_id': ['CHEMBL1'],
        'journal': ['Nature'],
        'pubmed_journal_title': ['Science'],
        'chembl_journal': ['Cell'],
        'other_column': ['value'],
        'zhurnal': ['Zhurnal'],  # Русское название в латинице
        'JOURNAL_NAME': ['Test'],  # Верхний регистр
    })
    
    journal_cols = get_journal_columns(df)
    expected_cols = ['journal', 'pubmed_journal_title', 'chembl_journal', 'JOURNAL_NAME']
    
    print(f"Найденные колонки журналов: {journal_cols}")
    print(f"Ожидаемые колонки: {expected_cols}")
    
    # Проверяем, что все ожидаемые колонки найдены
    found_all = all(col in journal_cols for col in expected_cols)
    print(f"Все колонки найдены: {'PASS' if found_all else 'FAIL'}")
    
    # Проверяем, что other_column не включена
    other_not_included = 'other_column' not in journal_cols
    print(f"other_column не включена: {'PASS' if other_not_included else 'FAIL'}")


if __name__ == "__main__":
    test_normalize_journal_name()
    test_normalize_journal_columns()
    test_get_journal_columns()
    print("\n" + "=" * 80)
    print("Все тесты завершены!")
    print("=" * 80)
