"""
Тесты для проверки синхронизации схем Pandera с column_order в конфигах.

Тест-кейсы:
1. Проверить, что column_order в конфиге совпадает с полями в схеме Pandera
2. Проверить типы всех полей в схеме
3. Проверить, что CSV записывается с правильным порядком колонок
4. Интеграционный тест: запуск пайплайна → проверка структуры итогового CSV
"""

import pytest
import yaml
import pandas as pd
from pathlib import Path
from typing import List

# Добавляем src в путь для импорта
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from library.schemas.document_schema_normalized import DocumentNormalizedSchema
from library.schemas.testitem_schema_normalized import TestitemNormalizedSchema
from library.common.schema_sync_validator import SchemaSyncValidator, validate_schema_sync


class TestSchemaSync:
    """Тесты синхронизации схем Pandera с конфигами."""
    
    @pytest.fixture
    def configs_dir(self):
        """Путь к директории с конфигами."""
        return Path(__file__).parent.parent / "configs"
    
    @pytest.fixture
    def document_config_path(self, configs_dir):
        """Путь к конфигу документов."""
        return configs_dir / "config_document.yaml"
    
    @pytest.fixture
    def testitem_config_path(self, configs_dir):
        """Путь к конфигу теститемов."""
        return configs_dir / "config_testitem.yaml"
    
    def load_config_column_order(self, config_path: Path) -> List[str]:
        """Загружает column_order из конфигурационного файла."""
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config.get('determinism', {}).get('column_order', [])
    
    def load_schema_columns(self, schema_class) -> List[str]:
        """Загружает колонки из схемы Pandera."""
        schema = schema_class.get_schema()
        return list(schema.columns.keys())
    
    def test_document_schema_sync(self, document_config_path):
        """Тест синхронизации схемы документов с конфигом."""
        # Загружаем column_order из конфига
        config_columns = self.load_config_column_order(document_config_path)
        assert len(config_columns) > 0, "column_order в конфиге документов не должен быть пустым"
        
        # Загружаем колонки из схемы
        schema_columns = self.load_schema_columns(DocumentNormalizedSchema)
        assert len(schema_columns) > 0, "Схема документов не должна быть пустой"
        
        # Проверяем, что все колонки из конфига есть в схеме
        config_set = set(config_columns)
        schema_set = set(schema_columns)
        
        missing_in_schema = config_set - schema_set
        assert len(missing_in_schema) == 0, f"Колонки из column_order отсутствуют в схеме документов: {missing_in_schema}"
        
        # Проверяем, что количество колонок совпадает
        assert len(config_columns) == len(schema_columns), \
            f"Количество колонок не совпадает: конфиг={len(config_columns)}, схема={len(schema_columns)}"
        
        # Проверяем порядок колонок
        assert config_columns == schema_columns, "Порядок колонок в конфиге и схеме должен совпадать"
    
    def test_testitem_schema_sync(self, testitem_config_path):
        """Тест синхронизации схемы теститемов с конфигом."""
        # Загружаем column_order из конфига
        config_columns = self.load_config_column_order(testitem_config_path)
        assert len(config_columns) > 0, "column_order в конфиге теститемов не должен быть пустым"
        
        # Загружаем колонки из схемы
        schema_columns = self.load_schema_columns(TestitemNormalizedSchema)
        assert len(schema_columns) > 0, "Схема теститемов не должна быть пустой"
        
        # Проверяем, что все колонки из конфига есть в схеме
        config_set = set(config_columns)
        schema_set = set(schema_columns)
        
        missing_in_schema = config_set - schema_set
        assert len(missing_in_schema) == 0, f"Колонки из column_order отсутствуют в схеме теститемов: {missing_in_schema}"
        
        # Проверяем, что количество колонок совпадает
        assert len(config_columns) == len(schema_columns), \
            f"Количество колонок не совпадает: конфиг={len(config_columns)}, схема={len(schema_columns)}"
        
        # Проверяем порядок колонок
        assert config_columns == schema_columns, "Порядок колонок в конфиге и схеме должен совпадать"
    
    def test_document_schema_types(self):
        """Тест типов полей в схеме документов."""
        schema = DocumentNormalizedSchema.get_schema()
        
        # Проверяем, что схема содержит ожидаемые типы
        expected_types = {
            'document_chembl_id': 'str',
            'document_pubmed_id': 'int64',
            'document_classification': 'str',
            'referenses_on_previous_experiments': 'bool',
            'original_experimental_document': 'bool',
            'retrieved_at': 'datetime64[ns]',
            'publication_date': 'datetime64[ns]',
            'document_sortorder': 'int64',
            'valid_doi': 'bool',
            'invalid_doi': 'bool',
        }
        
        for column_name, expected_type in expected_types.items():
            if column_name in schema.columns:
                column = schema.columns[column_name]
                # Проверяем, что тип колонки соответствует ожидаемому
                assert str(column.dtype) == expected_type, \
                    f"Колонка {column_name} имеет тип {column.dtype}, ожидался {expected_type}"
    
    def test_document_metadata_columns_in_final_output(self, document_config_path):
        """Тест что метаданные index, hash_row, hash_business_key включены в финальный CSV."""
        # Загружаем column_order из конфига
        config_columns = self.load_config_column_order(document_config_path)
        
        # Проверяем наличие метаданных в column_order
        metadata_columns = ["index", "hash_row", "hash_business_key"]
        for col in metadata_columns:
            assert col in config_columns, f"Колонка {col} должна быть в column_order"
        
        # Проверяем позицию метаданных после document_sortorder
        sortorder_index = config_columns.index("document_sortorder")
        index_pos = config_columns.index("index")
        hash_row_pos = config_columns.index("hash_row")
        hash_business_key_pos = config_columns.index("hash_business_key")
        
        assert index_pos > sortorder_index, "index должен быть после document_sortorder"
        assert hash_row_pos > sortorder_index, "hash_row должен быть после document_sortorder"
        assert hash_business_key_pos > sortorder_index, "hash_business_key должен быть после document_sortorder"
        
        # Проверяем что метаданные идут в правильном порядке
        assert index_pos < hash_row_pos < hash_business_key_pos, "Метаданные должны идти в порядке: index, hash_row, hash_business_key"
    
    def test_testitem_schema_types(self):
        """Тест типов полей в схеме теститемов."""
        schema = TestitemNormalizedSchema.get_schema()
        
        # Проверяем, что схема содержит ожидаемые типы
        expected_types = {
            'molecule_chembl_id': 'str',
            'molregno': 'int64',
            'pref_name': 'str',
            'max_phase': 'int64',
            'therapeutic_flag': 'bool',
            'mw_freebase': 'float64',
            'alogp': 'float64',
            'hba': 'int64',
            'hbd': 'int64',
            'psa': 'float64',
            'rtb': 'int64',
            'ro3_pass': 'bool',
            'num_ro5_violations': 'int64',
        }
        
        for column_name, expected_type in expected_types.items():
            if column_name in schema.columns:
                column = schema.columns[column_name]
                # Проверяем, что тип колонки соответствует ожидаемому
                assert str(column.dtype) == expected_type, \
                    f"Колонка {column_name} имеет тип {column.dtype}, ожидался {expected_type}"
    
    def test_schema_sync_validator(self, configs_dir):
        """Тест валидатора синхронизации схем."""
        validator = SchemaSyncValidator(strict_mode=False)
        
        # Валидируем все схемы
        is_valid = validator.validate_all_schemas(configs_dir)
        
        # Проверяем, что валидация прошла успешно
        assert is_valid, "Валидация схем должна пройти успешно"
        
        # Проверяем, что нет ошибок
        assert len(validator.errors) == 0, f"Не должно быть ошибок: {validator.errors}"
        
        # Проверяем отчет
        report = validator.get_validation_report()
        assert not report['has_errors'], "В отчете не должно быть ошибок"
    
    def test_validate_schema_sync_function(self, configs_dir):
        """Тест функции validate_schema_sync."""
        is_valid = validate_schema_sync(configs_dir, strict_mode=False)
        assert is_valid, "Функция validate_schema_sync должна вернуть True"
    
    def test_csv_column_order_consistency(self, configs_dir):
        """Тест консистентности порядка колонок для CSV."""
        # Загружаем column_order из конфигов
        doc_config_path = configs_dir / "config_document.yaml"
        testitem_config_path = configs_dir / "config_testitem.yaml"
        
        doc_columns = self.load_config_column_order(doc_config_path)
        testitem_columns = self.load_config_column_order(testitem_config_path)
        
        # Проверяем, что column_order не пустой
        assert len(doc_columns) > 0, "column_order документов не должен быть пустым"
        assert len(testitem_columns) > 0, "column_order теститемов не должен быть пустым"
        
        # Проверяем, что нет дубликатов в column_order
        assert len(doc_columns) == len(set(doc_columns)), "В column_order документов есть дубликаты"
        assert len(testitem_columns) == len(set(testitem_columns)), "В column_order теститемов есть дубликаты"
        
        # Проверяем, что первая колонка - это primary key
        assert doc_columns[0] == "document_chembl_id", "Первая колонка документов должна быть document_chembl_id"
        assert testitem_columns[0] == "molecule_chembl_id", "Первая колонка теститемов должна быть molecule_chembl_id"
    
    def test_schema_metadata_consistency(self):
        """Тест консистентности метаданных в схемах."""
        doc_schema = DocumentNormalizedSchema.get_schema()
        testitem_schema = TestitemNormalizedSchema.get_schema()
        
        # Проверяем, что все колонки имеют описания
        for column_name, column in doc_schema.columns.items():
            assert column.description is not None, f"Колонка {column_name} в схеме документов не имеет описания"
            assert len(column.description.strip()) > 0, f"Описание колонки {column_name} в схеме документов пустое"
        
        for column_name, column in testitem_schema.columns.items():
            assert column.description is not None, f"Колонка {column_name} в схеме теститемов не имеет описания"
            assert len(column.description.strip()) > 0, f"Описание колонки {column_name} в схеме теститемов пустое"
        
        # Проверяем, что все колонки имеют метаданные нормализации
        for column_name, column in doc_schema.columns.items():
            assert hasattr(column, 'metadata'), f"Колонка {column_name} в схеме документов не имеет метаданных"
            assert column.metadata is not None, f"Метаданные колонки {column_name} в схеме документов пустые"
            assert 'normalization_functions' in column.metadata, \
                f"Колонка {column_name} в схеме документов не имеет normalization_functions"
        
        for column_name, column in testitem_schema.columns.items():
            assert hasattr(column, 'metadata'), f"Колонка {column_name} в схеме теститемов не имеет метаданных"
            assert column.metadata is not None, f"Метаданные колонки {column_name} в схеме теститемов пустые"
            assert 'normalization_functions' in column.metadata, \
                f"Колонка {column_name} в схеме теститемов не имеет normalization_functions"


class TestSchemaSyncIntegration:
    """Интеграционные тесты синхронизации схем."""
    
    def test_schema_validation_with_sample_data(self):
        """Тест валидации схем с примерными данными."""
        # Создаем примерные данные для документов (только основные поля)
        doc_data = {
            'document_chembl_id': ['CHEMBL123', 'CHEMBL456'],
            'document_pubmed_id': [12345, 67890],
            'document_classification': ['Journal Article', 'Review'],
            'referenses_on_previous_experiments': [True, False],
            'original_experimental_document': [True, True],
            'document_citation': ['Test Citation 1', 'Test Citation 2'],
            'pubmed_mesh_descriptors': ['Mesh1', 'Mesh2'],
            'pubmed_mesh_qualifiers': ['Qual1', 'Qual2'],
            'pubmed_chemical_list': ['Chem1', 'Chem2'],
            'crossref_subject': ['Subject1', 'Subject2'],
            'chembl_pmid': ['12345', '67890'],
            'crossref_pmid': ['12345', '67890'],
            'openalex_pmid': ['12345', '67890'],
            'pubmed_pmid': ['12345', '67890'],
            'semantic_scholar_pmid': ['12345', '67890'],
            'chembl_title': ['Title 1', 'Title 2'],
            'crossref_title': ['Title 1', 'Title 2'],
            'openalex_title': ['Title 1', 'Title 2'],
            'pubmed_article_title': ['Title 1', 'Title 2'],
            'semantic_scholar_title': ['Title 1', 'Title 2'],
            'chembl_abstract': ['Abstract 1', 'Abstract 2'],
            'crossref_abstract': ['Abstract 1', 'Abstract 2'],
            'openalex_abstract': ['Abstract 1', 'Abstract 2'],
            'pubmed_abstract': ['Abstract 1', 'Abstract 2'],
            'chembl_authors': ['Author 1', 'Author 2'],
            'crossref_authors': ['Author 1', 'Author 2'],
            'openalex_authors': ['Author 1', 'Author 2'],
            'pubmed_authors': ['Author 1', 'Author 2'],
            'semantic_scholar_authors': ['Author 1', 'Author 2'],
            'chembl_doi': ['10.1000/test1', '10.1000/test2'],
            'crossref_doi': ['10.1000/test1', '10.1000/test2'],
            'openalex_doi': ['10.1000/test1', '10.1000/test2'],
            'pubmed_doi': ['10.1000/test1', '10.1000/test2'],
            'semantic_scholar_doi': ['10.1000/test1', '10.1000/test2'],
            'chembl_doc_type': ['Journal Article', 'Review'],
            'crossref_doc_type': ['Journal Article', 'Review'],
            'openalex_doc_type': ['Journal Article', 'Review'],
            'openalex_crossref_doc_type': ['Journal Article', 'Review'],
            'pubmed_doc_type': ['Journal Article', 'Review'],
            'semantic_scholar_doc_type': ['Journal Article', 'Review'],
            'chembl_issn': ['1234-5678', '8765-4321'],
            'crossref_issn': ['1234-5678', '8765-4321'],
            'openalex_issn': ['1234-5678', '8765-4321'],
            'pubmed_issn': ['1234-5678', '8765-4321'],
            'semantic_scholar_issn': ['1234-5678', '8765-4321'],
            'chembl_journal': ['Journal 1', 'Journal 2'],
            'crossref_journal': ['Journal 1', 'Journal 2'],
            'openalex_journal': ['Journal 1', 'Journal 2'],
            'pubmed_journal': ['Journal 1', 'Journal 2'],
            'semantic_scholar_journal': ['Journal 1', 'Journal 2'],
            'chembl_year': [2024, 2023],
            'crossref_year': [2024, 2023],
            'openalex_year': [2024, 2023],
            'pubmed_year': [2024, 2023],
            'chembl_volume': ['1', '2'],
            'crossref_volume': ['1', '2'],
            'openalex_volume': ['1', '2'],
            'pubmed_volume': ['1', '2'],
            'chembl_issue': ['1', '2'],
            'crossref_issue': ['1', '2'],
            'openalex_issue': ['1', '2'],
            'pubmed_issue': ['1', '2'],
            'crossref_first_page': ['1', '10'],
            'openalex_first_page': ['1', '10'],
            'pubmed_first_page': ['1', '10'],
            'crossref_last_page': ['10', '20'],
            'openalex_last_page': ['10', '20'],
            'pubmed_last_page': ['10', '20'],
            'chembl_error': [None, None],
            'crossref_error': [None, None],
            'openalex_error': [None, None],
            'pubmed_error': [None, None],
            'semantic_scholar_error': [None, None],
            'pubmed_year_completed': [2024, 2023],
            'pubmed_month_completed': [1, 2],
            'pubmed_day_completed': [1, 15],
            'pubmed_year_revised': [2024, 2023],
            'pubmed_month_revised': [1, 2],
            'pubmed_day_revised': [1, 15],
            'publication_date': pd.to_datetime(['2024-01-01', '2024-02-01']),
            'document_sortorder': [1, 2],
            'valid_doi': [True, False],
            'valid_journal': [True, True],
            'valid_year': [True, True],
            'valid_volume': [True, True],
            'valid_issue': [True, True],
            'invalid_doi': [False, True],
            'invalid_journal': [False, False],
            'invalid_year': [False, False],
            'invalid_volume': [False, False],
            'invalid_issue': [False, False],
        }
        
        # Создаем DataFrame
        doc_df = pd.DataFrame(doc_data)
        
        # Валидируем с помощью схемы
        doc_schema = DocumentNormalizedSchema.get_schema()
        
        try:
            validated_df = doc_schema.validate(doc_df)
            assert len(validated_df) == 2, "Валидация должна пройти успешно"
        except Exception as e:
            pytest.fail(f"Валидация схемы документов не прошла: {e}")
        
        # Создаем примерные данные для теститемов (только основные поля)
        testitem_data = {
            'molecule_chembl_id': ['CHEMBL789', 'CHEMBL012'],
            'molregno': [789, 12],
            'pref_name': ['Test Drug 1', 'Test Drug 2'],
            'pref_name_key': ['TEST_DRUG_1', 'TEST_DRUG_2'],
            'parent_chembl_id': ['CHEMBL789', 'CHEMBL012'],
            'parent_molregno': [789, 12],
            'max_phase': [3, 4],
            'therapeutic_flag': [True, True],
            'dosed_ingredient': [True, True],
            'first_approval': [2020, 2021],
            'structure_type': ['MOLECULE', 'MOLECULE'],
            'molecule_type': ['Small molecule', 'Small molecule'],
            'mw_freebase': [300.5, 450.2],
            'alogp': [2.1, 3.5],
            'hba': [5, 7],
            'hbd': [2, 3],
            'psa': [80.5, 120.3],
            'rtb': [4, 6],
            'ro3_pass': [True, False],
            'num_ro5_violations': [0, 1],
            'acd_most_apka': [3.5, 4.2],
            'acd_most_bpka': [8.1, 9.3],
            'acd_logp': [2.1, 3.5],
            'acd_logd': [1.8, 3.2],
            'molecular_species': ['NEUTRAL', 'NEUTRAL'],
            'full_mwt': [300.5, 450.2],
            'aromatic_rings': [2, 3],
            'heavy_atoms': [20, 30],
            'qed_weighted': [0.7, 0.8],
            'mw_monoisotopic': [300.4, 450.1],
            'full_molformula': ['C15H20N2O2', 'C20H25N3O3'],
            'hba_lipinski': [5, 7],
            'hbd_lipinski': [2, 3],
            'num_lipinski_ro5_violations': [0, 1],
            'oral': [True, True],
            'parenteral': [False, True],
            'topical': [False, False],
            'black_box_warning': [False, False],
            'natural_product': [False, False],
            'first_in_class': [True, False],
            'chirality': [True, False],
            'prodrug': [False, False],
            'inorganic_flag': [False, False],
            'polymer_flag': [False, False],
            'usan_year': [2020, 2021],
            'availability_type': ['Prescription', 'Prescription'],
            'usan_stem': ['-mab', '-mab'],
            'usan_substem': ['-tu-', '-tu-'],
            'usan_stem_definition': ['Monoclonal antibody', 'Monoclonal antibody'],
            'indication_class': ['Oncology', 'Oncology'],
            'withdrawn_flag': [False, False],
            'withdrawn_year': [None, None],
            'withdrawn_country': [None, None],
            'withdrawn_reason': [None, None],
            'mechanism_of_action': ['Inhibitor', 'Inhibitor'],
            'direct_interaction': [True, True],
            'molecular_mechanism': ['Enzyme inhibition', 'Enzyme inhibition'],
            'drug_chembl_id': ['CHEMBL789', 'CHEMBL012'],
            'drug_name': ['Test Drug 1', 'Test Drug 2'],
            'drug_type': ['Small molecule', 'Small molecule'],
            'drug_substance_flag': [True, True],
            'drug_indication_flag': [True, True],
            'drug_antibacterial_flag': [False, False],
            'drug_antiviral_flag': [False, False],
            'drug_antifungal_flag': [False, False],
            'drug_antiparasitic_flag': [False, False],
            'drug_antineoplastic_flag': [True, True],
            'drug_immunosuppressant_flag': [False, False],
            'drug_antiinflammatory_flag': [False, False],
            'pubchem_cid': [12345, 67890],
            'pubchem_molecular_formula': ['C15H20N2O2', 'C20H25N3O3'],
            'pubchem_molecular_weight': [300.5, 450.2],
            'pubchem_canonical_smiles': ['CCN(CC)CCCC(C)NC1=C2C=CC(Cl)=CC2=NC=C1', 'CCN(CC)CCCC(C)NC1=C2C=CC(Cl)=CC2=NC=C1'],
            'pubchem_isomeric_smiles': ['CCN(CC)CCCC(C)NC1=C2C=CC(Cl)=CC2=NC=C1', 'CCN(CC)CCCC(C)NC1=C2C=CC(Cl)=CC2=NC=C1'],
            'pubchem_inchi': ['InChI=1S/C15H20N2O2/c1-2-3-4-5-6-7-8-9-10-11-12-13-14-15/h1-15H2', 'InChI=1S/C20H25N3O3/c1-2-3-4-5-6-7-8-9-10-11-12-13-14-15-16-17-18-19-20/h1-20H2'],
            'pubchem_inchi_key': ['ABCDEFGHIJKLMNO-UHFFFAOYSA-N', 'ABCDEFGHIJKLMNO-UHFFFAOYSA-N'],
            'pubchem_registry_id': ['12345', '67890'],
            'pubchem_rn': ['12345', '67890'],
            'standardized_inchi': ['InChI=1S/C15H20N2O2/c1-2-3-4-5-6-7-8-9-10-11-12-13-14-15/h1-15H2', 'InChI=1S/C20H25N3O3/c1-2-3-4-5-6-7-8-9-10-11-12-13-14-15-16-17-18-19-20/h1-20H2'],
            'standardized_inchi_key': ['ABCDEFGHIJKLMNO-UHFFFAOYSA-N', 'ABCDEFGHIJKLMNO-UHFFFAOYSA-N'],
            'standardized_smiles': ['CCN(CC)CCCC(C)NC1=C2C=CC(Cl)=CC2=NC=C1', 'CCN(CC)CCCC(C)NC1=C2C=CC(Cl)=CC2=NC=C1'],
            'index': [1, 2],
            'pipeline_version': ['1.0.0', '1.0.0'],
            'source_system': ['ChEMBL', 'ChEMBL'],
            'chembl_release': ['33', '33'],
            'extracted_at': pd.to_datetime(['2025-01-01', '2025-01-02']),
            'hash_row': ['hash1', 'hash2'],
            'hash_business_key': ['key1', 'key2'],
            'all_names': ['Test Drug 1', 'Test Drug 2'],
            'canonical_smiles': ['CCN(CC)CCCC(C)NC1=C2C=CC(Cl)=CC2=NC=C1', 'CCN(CC)CCCC(C)NC1=C2C=CC(Cl)=CC2=NC=C1'],
            'inchi_key_from_mol': ['ABCDEFGHIJKLMNO-UHFFFAOYSA-N', 'ABCDEFGHIJKLMNO-UHFFFAOYSA-N'],
            'inchi_key_from_smiles': ['ABCDEFGHIJKLMNO-UHFFFAOYSA-N', 'ABCDEFGHIJKLMNO-UHFFFAOYSA-N'],
            'is_radical': [False, False],
            'mw_<100_or_>1000': [False, False],
            'n_stereocenters': [1, 2],
            'nstereo': [2, 4],
            'salt_chembl_id': [None, None],
            'standard_inchi_key': ['ABCDEFGHIJKLMNO-UHFFFAOYSA-N', 'ABCDEFGHIJKLMNO-UHFFFAOYSA-N'],
            'standard_inchi_skeleton': ['InChI=1S/C15H20N2O2', 'InChI=1S/C20H25N3O3'],
            'standard_inchi_stereo': ['InChI=1S/C15H20N2O2/c1-2-3-4-5-6-7-8-9-10-11-12-13-14-15/h1-15H2', 'InChI=1S/C20H25N3O3/c1-2-3-4-5-6-7-8-9-10-11-12-13-14-15-16-17-18-19-20/h1-20H2'],
            'atc_classifications': ['A01AA01', 'A01AA02'],
            'biotherapeutic': ['None', 'None'],
            'chemical_probe': [False, False],
            'cross_references': ['Ref1', 'Ref2'],
            'helm_notation': [None, None],
            'molecule_hierarchy': ['Hierarchy1', 'Hierarchy2'],
            'molecule_properties': ['Properties1', 'Properties2'],
            'molecule_structures': ['Structures1', 'Structures2'],
            'molecule_synonyms': ['Synonyms1', 'Synonyms2'],
            'orphan': [False, False],
            'veterinary': [False, False],
            'standard_inchi': ['InChI=1S/C15H20N2O2/c1-2-3-4-5-6-7-8-9-10-11-12-13-14-15/h1-15H2', 'InChI=1S/C20H25N3O3/c1-2-3-4-5-6-7-8-9-10-11-12-13-14-15-16-17-18-19-20/h1-20H2'],
            'chirality_chembl': ['Chiral', 'Achiral'],
            'molecule_type_chembl': ['Small molecule', 'Small molecule'],
        }
        
        # Создаем DataFrame
        testitem_df = pd.DataFrame(testitem_data)
        
        # Валидируем с помощью схемы
        testitem_schema = TestitemNormalizedSchema.get_schema()
        
        try:
            validated_df = testitem_schema.validate(testitem_df)
            assert len(validated_df) == 2, "Валидация должна пройти успешно"
        except Exception as e:
            pytest.fail(f"Валидация схемы теститемов не прошла: {e}")


if __name__ == "__main__":
    # Запуск тестов
    pytest.main([__file__, "-v"])
