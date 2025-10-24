# Testing Reference

## Обзор

Документация по тестированию ETL пайплайнов bioactivity_data_acquisition. Включает unit тесты, интеграционные тесты, contract тесты и E2E тесты.

## Структура тестов

### Каталоги тестов
```
tests/
├── unit/                    # Unit тесты
│   ├── test_api_clients.py
│   ├── test_normalize.py
│   ├── test_schemas.py
│   └── test_utils.py
├── integration/             # Интеграционные тесты
│   ├── test_api_integration.py
│   ├── test_pipeline_integration.py
│   └── test_database_integration.py
├── e2e/                     # End-to-end тесты
│   ├── test_documents_pipeline.py
│   ├── test_targets_pipeline.py
│   ├── test_assays_pipeline.py
│   ├── test_activities_pipeline.py
│   └── test_testitems_pipeline.py
├── fixtures/                # Тестовые данные
│   ├── sample_documents.csv
│   ├── sample_targets.csv
│   └── sample_activities.csv
└── conftest.py              # Pytest конфигурация
```

## Unit тесты

### API клиенты тесты
```python
# tests/unit/test_api_clients.py
import pytest
from unittest.mock import Mock, patch
from library.clients.chembl import ChEMBLClient
from library.clients.crossref import CrossrefClient

class TestChEMBLClient:
    def test_get_document_success(self):
        """Тест успешного получения документа"""
        client = ChEMBLClient(config={'base_url': 'https://test.chembl.com'})
        
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                'document_chembl_id': 'CHEMBL123456',
                'title': 'Test Document'
            }
            mock_get.return_value = mock_response
            
            result = client.get_document('CHEMBL123456')
            
            assert result['document_chembl_id'] == 'CHEMBL123456'
            assert result['title'] == 'Test Document'
    
    def test_get_document_not_found(self):
        """Тест обработки 404 ошибки"""
        client = ChEMBLClient(config={'base_url': 'https://test.chembl.com'})
        
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response
            
            with pytest.raises(Exception):
                client.get_document('CHEMBL999999')
    
    def test_rate_limit_handling(self):
        """Тест обработки rate limit"""
        client = ChEMBLClient(config={'base_url': 'https://test.chembl.com'})
        
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 429
            mock_get.return_value = mock_response
            
            with pytest.raises(Exception):
                client.get_document('CHEMBL123456')
```

### Нормализация тесты
```python
# tests/unit/test_normalize.py
import pytest
import pandas as pd
from library.normalize.document_normalizer import DocumentNormalizer

class TestDocumentNormalizer:
    def test_normalize_doi(self):
        """Тест нормализации DOI"""
        normalizer = DocumentNormalizer()
        
        test_cases = [
            ('https://doi.org/10.1021/acs.jmedchem.0c01234', '10.1021/acs.jmedchem.0c01234'),
            ('DOI:10.1021/acs.jmedchem.0c01234', '10.1021/acs.jmedchem.0c01234'),
            ('10.1021/acs.jmedchem.0c01234', '10.1021/acs.jmedchem.0c01234'),
            ('', ''),
            (None, None)
        ]
        
        for input_doi, expected in test_cases:
            result = normalizer.normalize_doi(input_doi)
            assert result == expected
    
    def test_normalize_chembl_id(self):
        """Тест нормализации ChEMBL ID"""
        normalizer = DocumentNormalizer()
        
        test_cases = [
            ('chembl123', 'CHEMBL123'),
            ('CHEMBL123', 'CHEMBL123'),
            ('123', 'CHEMBL123'),
            ('', ''),
            (None, None)
        ]
        
        for input_id, expected in test_cases:
            result = normalizer.normalize_chembl_id(input_id)
            assert result == expected
    
    def test_normalize_dataframe(self):
        """Тест нормализации DataFrame"""
        normalizer = DocumentNormalizer()
        
        df = pd.DataFrame({
            'document_chembl_id': ['chembl123', 'chembl456'],
            'doi': ['https://doi.org/10.1021/acs.jmedchem.0c01234', ''],
            'title': ['  Test Title  ', 'Another Title']
        })
        
        result = normalizer.normalize(df)
        
        assert result['document_chembl_id'].tolist() == ['CHEMBL123', 'CHEMBL456']
        assert result['doi'].tolist() == ['10.1021/acs.jmedchem.0c01234', '']
        assert result['title'].tolist() == ['Test Title', 'Another Title']
```

### Схемы тесты
```python
# tests/unit/test_schemas.py
import pytest
import pandas as pd
from library.schemas.document_schema import DocumentInputSchema, DocumentOutputSchema

class TestDocumentSchemas:
    def test_input_schema_validation(self):
        """Тест валидации входной схемы"""
        schema = DocumentInputSchema()
        
        # Валидные данные
        valid_df = pd.DataFrame({
            'document_chembl_id': ['CHEMBL123', 'CHEMBL456'],
            'title': ['Test Title 1', 'Test Title 2'],
            'doi': ['10.1021/acs.jmedchem.0c01234', ''],
            'journal': ['J Med Chem', 'Nature'],
            'year': [2020, 2021]
        })
        
        result = schema.validate(valid_df)
        assert len(result) == 2
        
        # Невалидные данные
        invalid_df = pd.DataFrame({
            'document_chembl_id': ['', 'CHEMBL456'],  # Пустой ID
            'title': ['Test Title 1', ''],  # Пустой title
            'doi': ['invalid-doi', '10.1021/acs.jmedchem.0c01234'],
            'journal': ['J Med Chem', 'Nature'],
            'year': [2020, 2021]
        })
        
        with pytest.raises(Exception):
            schema.validate(invalid_df)
    
    def test_output_schema_validation(self):
        """Тест валидации выходной схемы"""
        schema = DocumentOutputSchema()
        
        valid_df = pd.DataFrame({
            'document_chembl_id': ['CHEMBL123', 'CHEMBL456'],
            'title': ['Test Title 1', 'Test Title 2'],
            'doi': ['10.1021/acs.jmedchem.0c01234', ''],
            'journal': ['J Med Chem', 'Nature'],
            'year': [2020, 2021],
            'source_system': ['chembl', 'chembl'],
            'extracted_at': ['2025-10-24T14:30:22Z', '2025-10-24T14:30:22Z'],
            'hash_row': ['abc123', 'def456'],
            'hash_business_key': ['ghi789', 'jkl012']
        })
        
        result = schema.validate(valid_df)
        assert len(result) == 2
```

## Интеграционные тесты

### API интеграция
```python
# tests/integration/test_api_integration.py
import pytest
from library.clients.chembl import ChEMBLClient
from library.clients.crossref import CrossrefClient

class TestAPIIntegration:
    @pytest.mark.integration
    def test_chembl_api_connectivity(self):
        """Тест подключения к ChEMBL API"""
        client = ChEMBLClient(config={'base_url': 'https://www.ebi.ac.uk/chembl/api/data'})
        
        # Тест с реальным API
        result = client.get_document('CHEMBL123456')
        
        assert 'document_chembl_id' in result
        assert result['document_chembl_id'] == 'CHEMBL123456'
    
    @pytest.mark.integration
    def test_crossref_api_connectivity(self):
        """Тест подключения к Crossref API"""
        client = CrossrefClient(config={'base_url': 'https://api.crossref.org'})
        
        # Тест с реальным API
        result = client.search_works('chembl')
        
        assert 'message' in result
        assert 'items' in result['message']
    
    @pytest.mark.integration
    def test_api_error_handling(self):
        """Тест обработки ошибок API"""
        client = ChEMBLClient(config={'base_url': 'https://www.ebi.ac.uk/chembl/api/data'})
        
        # Тест с несуществующим ID
        with pytest.raises(Exception):
            client.get_document('CHEMBL999999999')
```

### Pipeline интеграция
```python
# tests/integration/test_pipeline_integration.py
import pytest
from library.documents.pipeline import DocumentPipeline

class TestPipelineIntegration:
    @pytest.mark.integration
    def test_document_pipeline_integration(self):
        """Тест интеграции пайплайна документов"""
        pipeline = DocumentPipeline(config='tests/fixtures/config_document_test.yaml')
        
        # Запуск пайплайна с тестовыми данными
        result = pipeline.run(input_file='tests/fixtures/sample_documents.csv')
        
        assert result['status'] == 'success'
        assert result['records_processed'] > 0
        assert result['records_accepted'] > 0
    
    @pytest.mark.integration
    def test_pipeline_error_handling(self):
        """Тест обработки ошибок в пайплайне"""
        pipeline = DocumentPipeline(config='tests/fixtures/config_document_test.yaml')
        
        # Тест с невалидными данными
        with pytest.raises(Exception):
            pipeline.run(input_file='tests/fixtures/invalid_documents.csv')
```

## E2E тесты

### Documents Pipeline E2E
```python
# tests/e2e/test_documents_pipeline.py
import pytest
import pandas as pd
from pathlib import Path
from library.documents.pipeline import DocumentPipeline

class TestDocumentsPipelineE2E:
    @pytest.mark.e2e
    def test_full_documents_pipeline(self):
        """Полный E2E тест пайплайна документов"""
        pipeline = DocumentPipeline(config='tests/fixtures/config_document_test.yaml')
        
        # Запуск пайплайна
        result = pipeline.run(input_file='tests/fixtures/sample_documents.csv')
        
        # Проверка результатов
        assert result['status'] == 'success'
        assert Path(result['output_file']).exists()
        assert Path(result['meta_file']).exists()
        assert Path(result['qc_file']).exists()
        
        # Проверка содержимого выходного файла
        output_df = pd.read_csv(result['output_file'])
        assert len(output_df) > 0
        assert 'document_chembl_id' in output_df.columns
        assert 'title' in output_df.columns
        assert 'doi' in output_df.columns
        
        # Проверка метаданных
        import yaml
        with open(result['meta_file'], 'r') as f:
            meta = yaml.safe_load(f)
        
        assert meta['pipeline']['name'] == 'documents'
        assert meta['data']['row_count'] > 0
        assert meta['validation']['schema_passed'] == True
    
    @pytest.mark.e2e
    def test_deterministic_output(self):
        """Тест детерминированности вывода"""
        pipeline = DocumentPipeline(config='tests/fixtures/config_document_test.yaml')
        
        # Первый запуск
        result1 = pipeline.run(input_file='tests/fixtures/sample_documents.csv')
        
        # Второй запуск
        result2 = pipeline.run(input_file='tests/fixtures/sample_documents.csv')
        
        # Проверка идентичности файлов
        with open(result1['output_file'], 'rb') as f1, open(result2['output_file'], 'rb') as f2:
            assert f1.read() == f2.read()
```

### Targets Pipeline E2E
```python
# tests/e2e/test_targets_pipeline.py
import pytest
from library.targets.pipeline import TargetPipeline

class TestTargetsPipelineE2E:
    @pytest.mark.e2e
    def test_full_targets_pipeline(self):
        """Полный E2E тест пайплайна мишеней"""
        pipeline = TargetPipeline(config='tests/fixtures/config_target_test.yaml')
        
        # Запуск пайплайна
        result = pipeline.run(input_file='tests/fixtures/sample_targets.csv')
        
        # Проверка результатов
        assert result['status'] == 'success'
        assert result['records_processed'] > 0
        assert result['records_accepted'] > 0
        
        # Проверка качества данных
        assert result['quality_metrics']['fill_rate'] > 0.8
        assert result['quality_metrics']['duplicate_rate'] < 0.1
```

## Contract тесты

### API Contract тесты
```python
# tests/contract/test_api_contracts.py
import pytest
from library.clients.chembl import ChEMBLClient

class TestAPIContracts:
    def test_chembl_document_contract(self):
        """Тест контракта ChEMBL Document API"""
        client = ChEMBLClient(config={'base_url': 'https://www.ebi.ac.uk/chembl/api/data'})
        
        # Тест с валидным ID
        result = client.get_document('CHEMBL123456')
        
        # Проверка обязательных полей
        assert 'document_chembl_id' in result
        assert 'title' in result
        assert 'document_type' in result
        
        # Проверка типов данных
        assert isinstance(result['document_chembl_id'], str)
        assert isinstance(result['title'], str)
        assert isinstance(result['document_type'], str)
        
        # Проверка формата ID
        assert result['document_chembl_id'].startswith('CHEMBL')
    
    def test_crossref_works_contract(self):
        """Тест контракта Crossref Works API"""
        client = CrossrefClient(config={'base_url': 'https://api.crossref.org'})
        
        # Тест поиска
        result = client.search_works('chembl')
        
        # Проверка структуры ответа
        assert 'message' in result
        assert 'items' in result['message']
        assert isinstance(result['message']['items'], list)
        
        # Проверка полей в элементах
        if result['message']['items']:
            item = result['message']['items'][0]
            assert 'DOI' in item
            assert 'title' in item
```

## Тестовые данные

### Fixtures
```python
# tests/conftest.py
import pytest
import pandas as pd
from pathlib import Path

@pytest.fixture
def sample_documents_df():
    """Тестовые данные документов"""
    return pd.DataFrame({
        'document_chembl_id': ['CHEMBL123456', 'CHEMBL789012'],
        'title': ['Test Document 1', 'Test Document 2'],
        'doi': ['10.1021/acs.jmedchem.0c01234', '10.1038/nature12345'],
        'journal': ['J Med Chem', 'Nature'],
        'year': [2020, 2021]
    })

@pytest.fixture
def sample_targets_df():
    """Тестовые данные мишеней"""
    return pd.DataFrame({
        'target_chembl_id': ['CHEMBL240', 'CHEMBL251'],
        'pref_name': ['Adenosine A1 receptor', 'Adenosine A2A receptor'],
        'target_type': ['SINGLE PROTEIN', 'SINGLE PROTEIN'],
        'organism': ['Homo sapiens', 'Homo sapiens']
    })

@pytest.fixture
def test_config():
    """Тестовая конфигурация"""
    return {
        'sources': {
            'chembl': {
                'enabled': True,
                'base_url': 'https://www.ebi.ac.uk/chembl/api/data',
                'timeout_sec': 30.0
            }
        },
        'runtime': {
            'workers': 1,
            'limit': 10
        },
        'validation': {
            'strict': False
        }
    }
```

## Pytest конфигурация

### pytest.ini
```ini
[tool:pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = 
    -v
    --tb=short
    --strict-markers
    --disable-warnings
    --cov=src/library
    --cov-report=html
    --cov-report=term-missing
    --cov-fail-under=90
markers =
    unit: Unit tests
    integration: Integration tests
    e2e: End-to-end tests
    contract: Contract tests
    slow: Slow tests
    api: API tests
    network: Network tests
```

### Запуск тестов
```bash
# Все тесты
pytest

# Только unit тесты
pytest -m unit

# Только интеграционные тесты
pytest -m integration

# Только E2E тесты
pytest -m e2e

# Тесты с покрытием
pytest --cov=src/library --cov-report=html

# Параллельный запуск
pytest -n auto
```

## Continuous Integration

### GitHub Actions
```yaml
# .github/workflows/test.yml
name: Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.10, 3.11, 3.12]
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e .[dev]
    
    - name: Run unit tests
      run: pytest -m unit --cov=src/library --cov-report=xml
    
    - name: Run integration tests
      run: pytest -m integration
    
    - name: Run E2E tests
      run: pytest -m e2e
    
    - name: Upload coverage to Codecov
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
```

## Лучшие практики

### 1. Структура тестов
- Группируйте тесты по функциональности
- Используйте описательные имена тестов
- Следуйте принципу AAA (Arrange, Act, Assert)

### 2. Тестовые данные
- Используйте фиксированные тестовые данные
- Создайте реалистичные сценарии
- Избегайте зависимостей между тестами

### 3. Моки и стабы
- Мокайте внешние зависимости
- Используйте реальные API для интеграционных тестов
- Тестируйте обработку ошибок

### 4. Покрытие кода
- Стремитесь к покрытию >90%
- Тестируйте граничные случаи
- Включайте негативные тесты

### 5. Производительность
- Используйте параллельный запуск
- Оптимизируйте медленные тесты
- Кэшируйте тестовые данные
