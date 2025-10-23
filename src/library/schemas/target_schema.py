"""
Pandera схемы для валидации данных таргетов.

Предоставляет схемы для входных, сырых и нормализованных данных таргетов.
"""

from typing import Optional
import pandas as pd
    import pandera as pa
from pandera import Column, DataFrameSchema, Check


class TargetInputSchema:
    """Схемы для входных данных таргетов."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для входных данных таргетов."""
        return DataFrameSchema({
            "target_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL target ID format"),
                    Check.not_null()
                ],
                nullable=False,
                description="ChEMBL ID таргета"
            )
        })


class TargetRawSchema:
    """Схемы для сырых данных таргетов из API."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для сырых данных таргетов."""
        return DataFrameSchema({
            # Основные поля ChEMBL
            "target_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL target ID format"),
                    Check.not_null()
                ],
                nullable=False,
                description="ChEMBL ID таргета"
            ),
            "pref_name": Column(pa.String, nullable=True, description="Предпочтительное название"),
            "hgnc_name": Column(pa.String, nullable=True, description="Название по HGNC"),
            "hgnc_id": Column(pa.String, nullable=True, description="HGNC ID"),
            "target_type": Column(pa.String, nullable=True, description="Тип таргета"),
            "tax_id": Column(pa.Int, nullable=True, description="Таксономический ID"),
            "species_group_flag": Column(pa.Bool, nullable=True, description="Флаг группировки по видам"),
            "target_components": Column(pa.String, nullable=True, description="Компоненты таргета"),
            "protein_classifications": Column(pa.String, nullable=True, description="Классификация белка"),
            "cross_references": Column(pa.String, nullable=True, description="Перекрестные ссылки"),
            "reaction_ec_numbers": Column(pa.String, nullable=True, description="EC номера реакций"),
            "retrieved_at": Column(
                pa.DateTime,
                checks=[Check.not_null()],
                nullable=False,
                description="Время получения данных"
            ),
            
            # UniProt поля
            "uniprot_id_primary": Column(pa.String, nullable=True, description="Первичный UniProt ID"),
            "uniProtkbId": Column(pa.String, nullable=True, description="UniProtKB ID"),
            "secondaryAccessions": Column(pa.String, nullable=True, description="Вторичные accession номера"),
            "secondaryAccessionNames": Column(pa.String, nullable=True, description="Названия вторичных accession"),
            "recommendedName": Column(pa.String, nullable=True, description="Рекомендуемое название"),
            "protein_name_alt": Column(pa.String, nullable=True, description="Альтернативное название белка"),
            "geneName": Column(pa.String, nullable=True, description="Название гена"),
            "gene_symbol_list": Column(pa.String, nullable=True, description="Список символов генов"),
            "organism": Column(pa.String, nullable=True, description="Организм"),
            "taxon_id": Column(pa.Int, nullable=True, description="Таксономический ID"),
            "lineage_superkingdom": Column(pa.String, nullable=True, description="Суперцарство в линии"),
            "lineage_phylum": Column(pa.String, nullable=True, description="Тип в линии"),
            "lineage_class": Column(pa.String, nullable=True, description="Класс в линии"),
            "sequence_length": Column(pa.Int, nullable=True, description="Длина последовательности"),
            "molecular_function": Column(pa.String, nullable=True, description="Молекулярная функция"),
            "cellular_component": Column(pa.String, nullable=True, description="Клеточный компонент"),
            "subcellular_location": Column(pa.String, nullable=True, description="Субклеточная локализация"),
            "topology": Column(pa.String, nullable=True, description="Топология"),
            "transmembrane": Column(pa.Bool, nullable=True, description="Трансмембранный"),
            "intramembrane": Column(pa.Bool, nullable=True, description="Внутримембранный"),
            "glycosylation": Column(pa.Bool, nullable=True, description="Гликозилирование"),
            "lipidation": Column(pa.Bool, nullable=True, description="Липидирование"),
            "disulfide_bond": Column(pa.Bool, nullable=True, description="Дисульфидная связь"),
            "modified_residue": Column(pa.Bool, nullable=True, description="Модифицированный остаток"),
            "phosphorylation": Column(pa.Bool, nullable=True, description="Фосфорилирование"),
            "acetylation": Column(pa.Bool, nullable=True, description="Ацетилирование"),
            "ubiquitination": Column(pa.Bool, nullable=True, description="Убиквитинирование"),
            "signal_peptide": Column(pa.Bool, nullable=True, description="Сигнальный пептид"),
            "propeptide": Column(pa.Bool, nullable=True, description="Пропептид"),
            "xref_ensembl": Column(pa.String, nullable=True, description="Ссылки на Ensembl"),
            "xref_pdb": Column(pa.String, nullable=True, description="Ссылки на PDB"),
            "xref_alphafold": Column(pa.String, nullable=True, description="Ссылки на AlphaFold"),
            "family": Column(pa.String, nullable=True, description="Семейство"),
            "SUPFAM": Column(pa.String, nullable=True, description="SUPFAM классификация"),
            "PROSITE": Column(pa.String, nullable=True, description="PROSITE классификация"),
            "InterPro": Column(pa.String, nullable=True, description="InterPro классификация"),
            "Pfam": Column(pa.String, nullable=True, description="Pfam классификация"),
            "PRINTS": Column(pa.String, nullable=True, description="PRINTS классификация"),
            "TCDB": Column(pa.String, nullable=True, description="TCDB классификация"),
            "GuidetoPHARMACOLOGY": Column(pa.String, nullable=True, description="Guide to Pharmacology"),
            "reactions": Column(pa.String, nullable=True, description="Реакции"),
            "isoform_ids": Column(pa.String, nullable=True, description="ID изоформ"),
            "isoform_names": Column(pa.String, nullable=True, description="Названия изоформ"),
            "isoform_synonyms": Column(pa.String, nullable=True, description="Синонимы изоформ"),
            "uniprot_last_update": Column(pa.Date, nullable=True, description="Последнее обновление UniProt"),
            "uniprot_version": Column(pa.Int, nullable=True, description="Версия UniProt"),
            
            # IUPHAR поля
            "iuphar_target_id": Column(pa.Int, nullable=True, description="IUPHAR ID таргета"),
            "iuphar_family_id": Column(pa.Int, nullable=True, description="IUPHAR ID семейства"),
            "iuphar_type": Column(pa.String, nullable=True, description="Тип по IUPHAR"),
            "iuphar_class": Column(pa.String, nullable=True, description="Класс по IUPHAR"),
            "iuphar_subclass": Column(pa.String, nullable=True, description="Подкласс по IUPHAR"),
            "iuphar_chain": Column(pa.String, nullable=True, description="Цепь по IUPHAR"),
            "iuphar_name": Column(pa.String, nullable=True, description="Название по IUPHAR"),
            "iuphar_full_id_path": Column(pa.String, nullable=True, description="Полный путь ID по IUPHAR"),
            "iuphar_full_name_path": Column(pa.String, nullable=True, description="Полный путь названий по IUPHAR"),
            
            # GtoP поля
            "gtop_target_id": Column(pa.Int, nullable=True, description="GtoP ID таргета"),
            "gtop_synonyms": Column(pa.String, nullable=True, description="Синонимы по GtoP"),
            "gtop_natural_ligands_n": Column(pa.Int, nullable=True, description="Количество природных лигандов"),
            "gtop_interactions_n": Column(pa.Int, nullable=True, description="Количество взаимодействий"),
            "gtop_function_text_short": Column(pa.String, nullable=True, description="Краткое описание функции"),
        })


class TargetNormalizedSchema:
    """Схемы для нормализованных данных таргетов."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для нормализованных данных таргетов."""
        return DataFrameSchema({
            # Основные поля
            "target_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL target ID format"),
                    Check.not_null()
                ],
                nullable=False,
                description="ChEMBL ID таргета"
            ),
            "pref_name": Column(pa.String, nullable=True, description="Предпочтительное название"),
            "hgnc_name": Column(pa.String, nullable=True, description="Название по HGNC"),
            "hgnc_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r'^HGNC:\d+$', error="Invalid HGNC ID format")
                ],
                nullable=True,
                description="HGNC ID"
            ),
            "target_type": Column(pa.String, nullable=True, description="Тип таргета"),
            "tax_id": Column(
                pa.Int,
                checks=[
                    Check.greater_than(0, error="Taxonomy ID must be > 0")
                ],
        nullable=True, 
                description="Таксономический ID"
            ),
            "species_group_flag": Column(pa.Bool, nullable=True, description="Флаг группировки по видам"),
            "target_components": Column(pa.String, nullable=True, description="Компоненты таргета"),
            "protein_classifications": Column(pa.String, nullable=True, description="Классификация белка"),
            "cross_references": Column(pa.String, nullable=True, description="Перекрестные ссылки"),
            "reaction_ec_numbers": Column(pa.String, nullable=True, description="EC номера реакций"),
            "retrieved_at": Column(
                pa.DateTime,
                checks=[Check.not_null()],
                nullable=False,
                description="Время получения данных"
            ),
            
            # UniProt поля
            "uniprot_id_primary": Column(
                pa.String,
                checks=[
                    Check.str_matches(
                        r'^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$',
                        error="Invalid UniProt ID format"
                    )
                ],
                nullable=True,
                description="Первичный UniProt ID"
            ),
            "uniProtkbId": Column(pa.String, nullable=True, description="UniProtKB ID"),
            "secondaryAccessions": Column(pa.String, nullable=True, description="Вторичные accession номера"),
            "secondaryAccessionNames": Column(pa.String, nullable=True, description="Названия вторичных accession"),
            "recommendedName": Column(pa.String, nullable=True, description="Рекомендуемое название"),
            "protein_name_alt": Column(pa.String, nullable=True, description="Альтернативное название белка"),
            "geneName": Column(pa.String, nullable=True, description="Название гена"),
            "gene_symbol_list": Column(pa.String, nullable=True, description="Список символов генов"),
            "organism": Column(pa.String, nullable=True, description="Организм"),
            "taxon_id": Column(
                pa.Int,
                checks=[
                    Check.greater_than(0, error="Taxon ID must be > 0")
                ],
                nullable=True,
                description="Таксономический ID"
            ),
            "lineage_superkingdom": Column(pa.String, nullable=True, description="Суперцарство в линии"),
            "lineage_phylum": Column(pa.String, nullable=True, description="Тип в линии"),
            "lineage_class": Column(pa.String, nullable=True, description="Класс в линии"),
            "sequence_length": Column(
                pa.Int,
                checks=[
                    Check.greater_than(0, error="Sequence length must be > 0")
                ],
                nullable=True,
                description="Длина последовательности"
            ),
            "molecular_function": Column(pa.String, nullable=True, description="Молекулярная функция"),
            "cellular_component": Column(pa.String, nullable=True, description="Клеточный компонент"),
            "subcellular_location": Column(pa.String, nullable=True, description="Субклеточная локализация"),
            "topology": Column(pa.String, nullable=True, description="Топология"),
            "transmembrane": Column(pa.Bool, nullable=True, description="Трансмембранный"),
            "intramembrane": Column(pa.Bool, nullable=True, description="Внутримембранный"),
            "glycosylation": Column(pa.Bool, nullable=True, description="Гликозилирование"),
            "lipidation": Column(pa.Bool, nullable=True, description="Липидирование"),
            "disulfide_bond": Column(pa.Bool, nullable=True, description="Дисульфидная связь"),
            "modified_residue": Column(pa.Bool, nullable=True, description="Модифицированный остаток"),
            "phosphorylation": Column(pa.Bool, nullable=True, description="Фосфорилирование"),
            "acetylation": Column(pa.Bool, nullable=True, description="Ацетилирование"),
            "ubiquitination": Column(pa.Bool, nullable=True, description="Убиквитинирование"),
            "signal_peptide": Column(pa.Bool, nullable=True, description="Сигнальный пептид"),
            "propeptide": Column(pa.Bool, nullable=True, description="Пропептид"),
            "xref_ensembl": Column(pa.String, nullable=True, description="Ссылки на Ensembl"),
            "xref_pdb": Column(pa.String, nullable=True, description="Ссылки на PDB"),
            "xref_alphafold": Column(pa.String, nullable=True, description="Ссылки на AlphaFold"),
            "family": Column(pa.String, nullable=True, description="Семейство"),
            "SUPFAM": Column(pa.String, nullable=True, description="SUPFAM классификация"),
            "PROSITE": Column(pa.String, nullable=True, description="PROSITE классификация"),
            "InterPro": Column(pa.String, nullable=True, description="InterPro классификация"),
            "Pfam": Column(pa.String, nullable=True, description="Pfam классификация"),
            "PRINTS": Column(pa.String, nullable=True, description="PRINTS классификация"),
            "TCDB": Column(pa.String, nullable=True, description="TCDB классификация"),
            "GuidetoPHARMACOLOGY": Column(pa.String, nullable=True, description="Guide to Pharmacology"),
            "reactions": Column(pa.String, nullable=True, description="Реакции"),
            "isoform_ids": Column(pa.String, nullable=True, description="ID изоформ"),
            "isoform_names": Column(pa.String, nullable=True, description="Названия изоформ"),
            "isoform_synonyms": Column(pa.String, nullable=True, description="Синонимы изоформ"),
            "uniprot_last_update": Column(pa.Date, nullable=True, description="Последнее обновление UniProt"),
            "uniprot_version": Column(
                pa.Int,
                checks=[
                    Check.greater_than(0, error="UniProt version must be > 0")
                ],
        nullable=True, 
                description="Версия UniProt"
            ),
            
            # IUPHAR поля
            "iuphar_target_id": Column(pa.Int, nullable=True, description="IUPHAR ID таргета"),
            "iuphar_family_id": Column(pa.Int, nullable=True, description="IUPHAR ID семейства"),
            "iuphar_type": Column(pa.String, nullable=True, description="Тип по IUPHAR"),
            "iuphar_class": Column(pa.String, nullable=True, description="Класс по IUPHAR"),
            "iuphar_subclass": Column(pa.String, nullable=True, description="Подкласс по IUPHAR"),
            "iuphar_chain": Column(pa.String, nullable=True, description="Цепь по IUPHAR"),
            "iuphar_name": Column(pa.String, nullable=True, description="Название по IUPHAR"),
            "iuphar_full_id_path": Column(pa.String, nullable=True, description="Полный путь ID по IUPHAR"),
            "iuphar_full_name_path": Column(pa.String, nullable=True, description="Полный путь названий по IUPHAR"),
            
            # GtoP поля
            "gtop_target_id": Column(pa.Int, nullable=True, description="GtoP ID таргета"),
            "gtop_synonyms": Column(pa.String, nullable=True, description="Синонимы по GtoP"),
            "gtop_natural_ligands_n": Column(
                pa.Int,
                checks=[
                    Check.greater_than_or_equal_to(0, error="Natural ligands count must be >= 0")
                ],
                nullable=True,
                description="Количество природных лигандов"
            ),
            "gtop_interactions_n": Column(
                pa.Int,
                checks=[
                    Check.greater_than_or_equal_to(0, error="Interactions count must be >= 0")
                ],
        nullable=True, 
                description="Количество взаимодействий"
            ),
            "gtop_function_text_short": Column(pa.String, nullable=True, description="Краткое описание функции"),
            
            # Системные поля
            "index": Column(
                pa.Int,
                checks=[
                    Check.greater_than_or_equal_to(0, error="Index must be >= 0"),
                    Check.not_null()
                ],
                nullable=False,
                description="Порядковый номер записи"
            ),
            "pipeline_version": Column(
                pa.String,
                checks=[Check.not_null()],
                nullable=False,
                description="Версия пайплайна"
            ),
            "source_system": Column(
                pa.String,
                checks=[Check.not_null()],
                nullable=False,
                description="Система-источник"
            ),
            "chembl_release": Column(pa.String, nullable=True, description="Версия ChEMBL"),
            "extracted_at": Column(
                pa.DateTime,
                checks=[Check.not_null()],
                nullable=False,
                description="Время извлечения данных"
            ),
            "hash_row": Column(
                pa.String,
                checks=[Check.not_null()],
                nullable=False,
                description="Хеш строки SHA256"
            ),
            "hash_business_key": Column(
                pa.String,
                checks=[Check.not_null()],
                nullable=False,
                description="Хеш бизнес-ключа SHA256"
            ),
            
            # Ошибки и статусы
            "extraction_errors": Column(pa.String, nullable=True, description="Ошибки извлечения (JSON)"),
            "validation_errors": Column(pa.String, nullable=True, description="Ошибки валидации (JSON)"),
            "extraction_status": Column(
                pa.String,
                checks=[
                    Check.isin(["success", "partial", "failed"], error="Invalid extraction status")
                ],
                nullable=True,
                description="Статус извлечения"
            ),
        })


class TargetSchemaValidator:
    """Валидатор схем таргетов."""
    
    def __init__(self):
        self.input_schema = TargetInputSchema.get_schema()
        self.raw_schema = TargetRawSchema.get_schema()
        self.normalized_schema = TargetNormalizedSchema.get_schema()
    
    def validate_input(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидировать входные данные таргетов."""
        return self.input_schema.validate(df)
    
    def validate_raw(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидировать сырые данные таргетов."""
        return self.raw_schema.validate(df)
    
    def validate_normalized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидировать нормализованные данные таргетов."""
        return self.normalized_schema.validate(df)
    
    def get_schema_errors(self, df: pd.DataFrame, schema_type: str = "normalized") -> list:
        """Получить ошибки схемы."""
        try:
            if schema_type == "input":
                self.input_schema.validate(df)
            elif schema_type == "raw":
                self.raw_schema.validate(df)
            elif schema_type == "normalized":
                self.normalized_schema.validate(df)
            else:
                raise ValueError(f"Неизвестный тип схемы: {schema_type}")
            return []
        except pa.errors.SchemaError as e:
            return [str(error) for error in e.failure_cases]
    
    def is_valid(self, df: pd.DataFrame, schema_type: str = "normalized") -> bool:
        """Проверить валидность данных."""
        try:
            if schema_type == "input":
                self.input_schema.validate(df)
            elif schema_type == "raw":
                self.raw_schema.validate(df)
            elif schema_type == "normalized":
                self.normalized_schema.validate(df)
            else:
                raise ValueError(f"Неизвестный тип схемы: {schema_type}")
            return True
        except pa.errors.SchemaError:
            return False


# Глобальный экземпляр валидатора
target_schema_validator = TargetSchemaValidator()


def validate_target_input(df: pd.DataFrame) -> pd.DataFrame:
    """Валидировать входные данные таргетов."""
    return target_schema_validator.validate_input(df)


def validate_target_raw(df: pd.DataFrame) -> pd.DataFrame:
    """Валидировать сырые данные таргетов."""
    return target_schema_validator.validate_raw(df)


def validate_target_normalized(df: pd.DataFrame) -> pd.DataFrame:
    """Валидировать нормализованные данные таргетов."""
    return target_schema_validator.validate_normalized(df)


def get_target_schema_errors(df: pd.DataFrame, schema_type: str = "normalized") -> list:
    """Получить ошибки схемы таргетов."""
    return target_schema_validator.get_schema_errors(df, schema_type)


def is_target_valid(df: pd.DataFrame, schema_type: str = "normalized") -> bool:
    """Проверить валидность данных таргетов."""
    return target_schema_validator.is_valid(df, schema_type)