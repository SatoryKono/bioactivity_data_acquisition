"""
Тест исправления пустых колонок в пайплайне documents.

Проверяет что после обновления клиентов API колонки заполняются данными.
"""


# Импортируем обновленные клиенты
from src.library.clients.crossref import CrossrefClient
from src.library.clients.openalex import OpenAlexClient
from src.library.clients.pubmed import PubMedClient
from src.library.clients.semantic_scholar import SemanticScholarClient
from src.library.config import APIClientConfig
from src.library.config import RateLimitSettings
from src.library.config import RetrySettings


def test_crossref_client():
    """Тест CrossrefClient - проверяем извлечение новых полей."""
    print("=" * 80)
    print("ТЕСТ 1: CrossrefClient извлекает новые поля")
    print("=" * 80)
    
    # Создаем мок конфиг
    config = APIClientConfig(
        name="crossref",
        base_url="https://api.crossref.org",
        timeout_sec=30,
        retries=RetrySettings(total=3, backoff_multiplier=2.0, backoff_max=60.0),
        rate_limit=RateLimitSettings(max_calls=10, period=1.0),
        headers={},
        verify_ssl=True,
        follow_redirects=True,
    )
    
    client = CrossrefClient(config)
    
    # Тестовые данные Crossref work
    test_work = {
        "DOI": "10.1021/jm00123a001",
        "title": ["Test Article"],
        "type": "journal-article",
        "published-print": {
            "date-parts": [[2023, 1, 15]]
        },
        "volume": "45",
        "issue": "3",
        "page": "123-145",
        "container-title": ["Journal of Medicinal Chemistry"],
        "author": [
            {"given": "John", "family": "Doe"},
            {"given": "Jane", "family": "Smith"}
        ]
    }
    
    # Парсим work
    result = client._parse_work(test_work)
    
    # Проверяем новые поля
    new_fields = [
        "crossref_year", "crossref_volume", "crossref_issue", 
        "crossref_first_page", "crossref_last_page"
    ]
    
    print("Результат парсинга Crossref work:")
    for field in new_fields:
        value = result.get(field)
        if value is not None:
            print(f"✅ {field}: {value}")
        else:
            print(f"❌ {field}: None")
    
    return all(result.get(field) is not None for field in new_fields)

def test_openalex_client():
    """Тест OpenAlexClient - проверяем извлечение новых полей."""
    print("\n" + "=" * 80)
    print("ТЕСТ 2: OpenAlexClient извлекает новые поля")
    print("=" * 80)
    
    # Создаем мок конфиг
    config = APIClientConfig(
        name="openalex",
        base_url="https://api.openalex.org",
        timeout_sec=30,
        retries=RetrySettings(total=3, backoff_multiplier=2.0, backoff_max=60.0),
        rate_limit=RateLimitSettings(max_calls=10, period=1.0),
        headers={},
        verify_ssl=True,
        follow_redirects=True,
    )
    
    client = OpenAlexClient(config)
    
    # Тестовые данные OpenAlex work
    test_work = {
        "id": "https://openalex.org/W1234567890",
        "doi": "https://doi.org/10.1021/jm00123a001",
        "title": "Test Article",
        "type": "journal-article",
        "publication_date": "2023-01-15",
        "biblio": {
            "volume": "45",
            "issue": "3",
            "first_page": "123",
            "last_page": "145"
        },
        "authorships": [
            {"author": {"display_name": "John Doe"}},
            {"author": {"display_name": "Jane Smith"}}
        ]
    }
    
    # Парсим work
    result = client._parse_work(test_work)
    
    # Проверяем новые поля
    new_fields = [
        "openalex_volume", "openalex_issue", 
        "openalex_first_page", "openalex_last_page"
    ]
    
    print("Результат парсинга OpenAlex work:")
    for field in new_fields:
        value = result.get(field)
        if value is not None:
            print(f"✅ {field}: {value}")
        else:
            print(f"❌ {field}: None")
    
    return all(result.get(field) is not None for field in new_fields)

def test_pubmed_client():
    """Тест PubMedClient - проверяем извлечение MeSH данных."""
    print("\n" + "=" * 80)
    print("ТЕСТ 3: PubMedClient извлекает MeSH данные")
    print("=" * 80)
    
    # Создаем мок конфиг
    config = APIClientConfig(
        name="pubmed",
        base_url="https://eutils.ncbi.nlm.nih.gov",
        timeout_sec=30,
        retries=RetrySettings(total=3, backoff_multiplier=2.0, backoff_max=60.0),
        rate_limit=RateLimitSettings(max_calls=10, period=1.0),
        headers={},
        verify_ssl=True,
        follow_redirects=True,
    )
    
    client = PubMedClient(config)
    
    # Тестовые данные PubMed record
    test_record = {
        "pmid": "12345678",
        "title": "Test Article",
        "abstract": "Test abstract",
        "meshdescriptors": ["Drug Discovery", "Medicinal Chemistry"],
        "meshqualifiers": ["therapeutic use", "pharmacology"],
        "chemicals": ["Aspirin", "Ibuprofen"],
        "pubtype": ["Journal Article"],
        "year": 2023,
        "month": 1,
        "day": 15
    }
    
    # Парсим record
    result = client._normalise_record(test_record)
    
    # Проверяем MeSH поля
    mesh_fields = [
        "pubmed_mesh_descriptors", "pubmed_mesh_qualifiers", "pubmed_chemical_list"
    ]
    
    print("Результат парсинга PubMed record:")
    for field in mesh_fields:
        value = result.get(field)
        if value is not None:
            print(f"✅ {field}: {value}")
        else:
            print(f"❌ {field}: None")
    
    return all(result.get(field) is not None for field in mesh_fields)

def test_semantic_scholar_client():
    """Тест SemanticScholarClient - проверяем извлечение DOI и ISSN."""
    print("\n" + "=" * 80)
    print("ТЕСТ 4: SemanticScholarClient извлекает DOI и ISSN")
    print("=" * 80)
    
    # Создаем мок конфиг
    config = APIClientConfig(
        name="semantic_scholar",
        base_url="https://api.semanticscholar.org",
        timeout_sec=30,
        retries=RetrySettings(total=3, backoff_multiplier=2.0, backoff_max=60.0),
        rate_limit=RateLimitSettings(max_calls=10, period=1.0),
        headers={},
        verify_ssl=True,
        follow_redirects=True,
    )
    
    client = SemanticScholarClient(config)
    
    # Тестовые данные Semantic Scholar paper
    test_paper = {
        "paperId": "1234567890",
        "title": "Test Article",
        "year": 2023,
        "externalIds": {
            "DOI": "10.1021/jm00123a001",
            "PMID": "12345678"
        },
        "publicationVenue": {
            "name": "Journal of Medicinal Chemistry",
            "issn": "0022-2623"
        },
        "authors": [
            {"name": "John Doe"},
            {"name": "Jane Smith"}
        ]
    }
    
    # Парсим paper
    result = client._parse_paper(test_paper)
    
    # Проверяем DOI и ISSN поля
    doi_issn_fields = [
        "semantic_scholar_doi", "semantic_scholar_issn"
    ]
    
    print("Результат парсинга Semantic Scholar paper:")
    for field in doi_issn_fields:
        value = result.get(field)
        if value is not None:
            print(f"✅ {field}: {value}")
        else:
            print(f"❌ {field}: None")
    
    return all(result.get(field) is not None for field in doi_issn_fields)

def main():
    """Запуск всех тестов."""
    print("ТЕСТ ИСПРАВЛЕНИЯ ПУСТЫХ КОЛОНОК")
    print("=" * 80)
    
    results = []
    
    # Тест 1: CrossrefClient
    results.append(test_crossref_client())
    
    # Тест 2: OpenAlexClient
    results.append(test_openalex_client())
    
    # Тест 3: PubMedClient
    results.append(test_pubmed_client())
    
    # Тест 4: SemanticScholarClient
    results.append(test_semantic_scholar_client())
    
    # Итоговый результат
    print("\n" + "=" * 80)
    print("ИТОГОВЫЙ РЕЗУЛЬТАТ")
    print("=" * 80)
    
    passed = sum(results)
    total = len(results)
    
    print(f"Пройдено тестов: {passed}/{total}")
    
    if passed == total:
        print("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ! Клиенты API теперь извлекают все необходимые поля.")
    else:
        print("⚠️  Некоторые тесты не пройдены. Требуется дополнительная работа.")
    
    return passed == total

if __name__ == "__main__":
    main()
