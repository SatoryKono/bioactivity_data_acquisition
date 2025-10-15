"""Демонстрация работы функции формирования литературных ссылок."""

import sys
from pathlib import Path

# Добавляем src в путь для импорта
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

import pandas as pd
from library.tools.citation_formatter import format_citation, add_citation_column


def test_format_citation():
    """Тестирование функции format_citation."""
    
    print("=" * 60)
    print("Тестирование функции format_citation")
    print("=" * 60)
    
    # Тест 1: Базовая ссылка с issue
    result = format_citation(
        journal="Nature",
        volume="612",
        issue="7940",
        first_page="100",
        last_page="105"
    )
    expected = "Nature, 612 (7940). p. 100-105"
    print(f"\nТест 1: С issue")
    print(f"Результат:  {result}")
    print(f"Ожидается:  {expected}")
    print(f"Статус: {'PASS' if result == expected else 'FAIL'}")
    
    # Тест 2: Без issue
    result = format_citation(
        journal="Nature",
        volume="612",
        issue="",
        first_page="100",
        last_page="100"
    )
    expected = "Nature, 612. 100"
    print(f"\nТест 2: Без issue, одна страница")
    print(f"Результат:  {result}")
    print(f"Ожидается:  {expected}")
    print(f"Статус: {'PASS' if result == expected else 'FAIL'}")
    
    # Тест 3: Нечисловая страница
    result = format_citation(
        journal="JAMA",
        volume="327",
        issue="12",
        first_page="e221234",
        last_page="e221234"
    )
    expected = "JAMA, 327 (12). e221234"
    print(f"\nТест 3: Нечисловая страница")
    print(f"Результат:  {result}")
    print(f"Ожидается:  {expected}")
    print(f"Статус: {'PASS' if result == expected else 'FAIL'}")
    
    # Тест 4: Большой last_page
    result = format_citation(
        journal="Cell",
        volume="",
        issue="",
        first_page="50",
        last_page="200000"
    )
    expected = "Cell. 50"
    print(f"\nТест 4: Большой last_page")
    print(f"Результат:  {result}")
    print(f"Ожидается:  {expected}")
    print(f"Статус: {'PASS' if result == expected else 'FAIL'}")
    
    # Тест 5: Без страниц
    result = format_citation(
        journal="Science",
        volume="380",
        issue="*",
        first_page="",
        last_page=""
    )
    expected = "Science, 380 (*)."
    print(f"\nТест 5: Без страниц")
    print(f"Результат:  {result}")
    print(f"Ожидается:  {expected}")
    print(f"Статус: {'PASS' if result == expected else 'FAIL'}")


def test_add_citation_column():
    """Тестирование функции add_citation_column."""
    
    print("\n" + "=" * 60)
    print("Тестирование функции add_citation_column")
    print("=" * 60)
    
    # Создаем тестовый DataFrame
    df = pd.DataFrame({
        'document_chembl_id': ['CHEMBL1', 'CHEMBL2', 'CHEMBL3'],
        'journal': ['Nature', 'Science', 'Cell'],
        'volume': ['612', '380', ''],
        'issue': ['7940', '', ''],
        'first_page': ['100', '50', ''],
        'last_page': ['105', '200000', '']
    })
    
    print("\nИсходный DataFrame:")
    print(df)
    
    # Добавляем колонку с цитатами
    result = add_citation_column(df)
    
    print("\nDataFrame с добавленной колонкой 'citation':")
    print(result[['document_chembl_id', 'citation']])
    
    # Проверяем результаты
    expected_citations = [
        "Nature, 612 (7940). p. 100-105",
        "Science, 380. 50",
        "Cell."
    ]
    
    print("\nПроверка результатов:")
    for i, (actual, expected) in enumerate(zip(result['citation'], expected_citations)):
        status = 'PASS' if actual == expected else 'FAIL'
        print(f"Zapic {i+1}: {status}")
        print(f"  Результат:  {actual}")
        print(f"  Ожидается:  {expected}")


if __name__ == "__main__":
    test_format_citation()
    test_add_citation_column()
    print("\n" + "=" * 60)
    print("Все тесты завершены!")
    print("=" * 60)
