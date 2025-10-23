"""
Pandera схемы для валидации данных targets с нормализацией.

Предоставляет схемы для входных, сырых и нормализованных данных targets
с атрибутами нормализации для каждой колонки.
"""

import pandera as pa
from pandera import Check, Column, DataFrameSchema


def add_normalization_metadata(column: Column, functions: list[str]) -> Column:
    """Добавляет метаданные нормализации к колонке.
    
    Args:
        column: Колонка Pandera
        functions: Список функций нормализации
        
    Returns:
        Колонка с добавленными метаданными
    """
    metadata = column.metadata or {}
    metadata["normalization_functions"] = functions
    return Column(
        column.dtype,
        checks=column.checks,
        nullable=column.nullable,
        description=column.description,
        metadata=metadata
    )


class TargetNormalizedSchema:
    """Схемы для нормализованных данных targets."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для нормализованных данных targets."""
        return DataFrameSchema({
            # Основные поля
            "target_chembl_id": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL target ID format"),
                        Check(lambda x: x.notna())
                    ],
                    nullable=False,
                    description="ChEMBL ID target"
                ),
                ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
            ),
            "pref_name": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Предпочтительное название"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "hgnc_name": add_normalization_metadata(
                Column(pa.String, nullable=True, description="HGNC название"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "hgnc_id": add_normalization_metadata(
                Column(pa.String, nullable=True, description="HGNC ID"),
                ["normalize_string_strip", "normalize_hgnc_id"]
            ),
            "target_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип target"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "tax_id": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Taxonomy ID"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "species_group_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг группы видов"),
                ["normalize_boolean"]
            ),
            "target_components": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Компоненты target"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "protein_classifications": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Классификации белков"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "cross_references": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Перекрестные ссылки"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "reaction_ec_numbers": add_normalization_metadata(
                Column(pa.String, nullable=True, description="EC номера реакций"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            
            # UniProt поля
            "uniprot_id_primary": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Основной UniProt ID"),
                ["normalize_string_strip", "normalize_string_upper", "normalize_uniprot_id"]
            ),
            "uniProtkbId": add_normalization_metadata(
                Column(pa.String, nullable=True, description="UniProtKB ID"),
                ["normalize_string_strip", "normalize_string_upper", "normalize_uniprot_id"]
            ),
            "secondaryAccessions": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Вторичные акцессии"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "secondaryAccessionNames": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Названия вторичных акцессий"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "recommendedName": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Рекомендуемое название"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "protein_name_alt": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Альтернативное название белка"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "geneName": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Название гена"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "gene_symbol_list": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Список символов генов"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "organism": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Организм"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "taxon_id": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Taxon ID"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "lineage_superkingdom": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Суперцарство"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "lineage_phylum": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "lineage_class": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Класс"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "sequence_length": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Длина последовательности"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "molecular_function": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Молекулярная функция"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "cellular_component": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Клеточный компонент"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "subcellular_location": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Субклеточная локализация"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "topology": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Топология"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            
            # PTM флаги
            "transmembrane": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Трансмембранный"),
                ["normalize_boolean"]
            ),
            "intramembrane": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Внутримембранный"),
                ["normalize_boolean"]
            ),
            "glycosylation": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Гликозилирование"),
                ["normalize_boolean"]
            ),
            "lipidation": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Липидирование"),
                ["normalize_boolean"]
            ),
            "disulfide_bond": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Дисульфидная связь"),
                ["normalize_boolean"]
            ),
            "modified_residue": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Модифицированный остаток"),
                ["normalize_boolean"]
            ),
            "phosphorylation": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Фосфорилирование"),
                ["normalize_boolean"]
            ),
            "acetylation": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Ацетилирование"),
                ["normalize_boolean"]
            ),
            "ubiquitination": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Убиквитинирование"),
                ["normalize_boolean"]
            ),
            "signal_peptide": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Сигнальный пептид"),
                ["normalize_boolean"]
            ),
            "propeptide": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Пропептид"),
                ["normalize_boolean"]
            ),
            
            # Cross-references
            "xref_ensembl": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Ensembl ссылка"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "xref_pdb": add_normalization_metadata(
                Column(pa.String, nullable=True, description="PDB ссылка"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "xref_alphafold": add_normalization_metadata(
                Column(pa.String, nullable=True, description="AlphaFold ссылка"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            
            # Семейства и домены
            "family": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Семейство"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "SUPFAM": add_normalization_metadata(
                Column(pa.String, nullable=True, description="SUPFAM"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "PROSITE": add_normalization_metadata(
                Column(pa.String, nullable=True, description="PROSITE"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "InterPro": add_normalization_metadata(
                Column(pa.String, nullable=True, description="InterPro"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "Pfam": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Pfam"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "PRINTS": add_normalization_metadata(
                Column(pa.String, nullable=True, description="PRINTS"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "TCDB": add_normalization_metadata(
                Column(pa.String, nullable=True, description="TCDB"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "GuidetoPHARMACOLOGY": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Guide to Pharmacology"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            
            # Реакции
            "reactions": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Реакции"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            
            # Изоформы
            "isoform_ids": add_normalization_metadata(
                Column(pa.String, nullable=True, description="ID изоформ"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "isoform_names": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Названия изоформ"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "isoform_synonyms": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Синонимы изоформ"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            
            # UniProt метаданные
            "uniprot_last_update": add_normalization_metadata(
                Column(pa.DateTime, nullable=True, description="Последнее обновление UniProt"),
                ["normalize_datetime_iso8601"]
            ),
            "uniprot_version": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Версия UniProt"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            
            # IUPHAR поля
            "iuphar_target_id": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="IUPHAR target ID"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "iuphar_family_id": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="IUPHAR family ID"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "iuphar_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="IUPHAR тип"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "iuphar_class": add_normalization_metadata(
                Column(pa.String, nullable=True, description="IUPHAR класс"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "iuphar_subclass": add_normalization_metadata(
                Column(pa.String, nullable=True, description="IUPHAR подкласс"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "iuphar_chain": add_normalization_metadata(
                Column(pa.String, nullable=True, description="IUPHAR цепь"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "iuphar_name": add_normalization_metadata(
                Column(pa.String, nullable=True, description="IUPHAR название"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "iuphar_full_id_path": add_normalization_metadata(
                Column(pa.String, nullable=True, description="IUPHAR полный путь ID"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "iuphar_full_name_path": add_normalization_metadata(
                Column(pa.String, nullable=True, description="IUPHAR полный путь названия"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            
            # GtoPdb поля
            "gtop_target_id": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="GtoPdb target ID"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "gtop_synonyms": add_normalization_metadata(
                Column(pa.String, nullable=True, description="GtoPdb синонимы"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "gtop_natural_ligands_n": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Количество природных лигандов"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "gtop_interactions_n": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Количество взаимодействий"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "gtop_function_text_short": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Краткое описание функции"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            
            # Системные поля
            "index": add_normalization_metadata(
                Column(pa.Int64, nullable=False, description="Индекс записи"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "pipeline_version": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Версия пайплайна"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "source_system": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Исходная система"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "chembl_release": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Релиз ChEMBL"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "extracted_at": add_normalization_metadata(
                Column(pa.DateTime, nullable=True, description="Время извлечения данных"),
                ["normalize_datetime_iso8601"]
            ),
            "hash_row": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[Check(lambda x: x.notna())],
                    nullable=False,
                    description="SHA256 хеш строки"
                ),
                ["normalize_string_strip", "normalize_string_lower"]
            ),
            "hash_business_key": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[Check(lambda x: x.notna())],
                    nullable=False,
                    description="SHA256 хеш бизнес-ключа"
                ),
                ["normalize_string_strip", "normalize_string_lower"]
            ),
            
            # Дополнительные поля из реального вывода
            "component_description": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Описание компонента"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "component_id": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="ID компонента"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "relationship": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Отношение компонента"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "gene": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Символ гена"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "uniprot_id": add_normalization_metadata(
                Column(pa.String, nullable=True, description="UniProt ID из ChEMBL"),
                ["normalize_string_strip", "normalize_string_upper", "normalize_uniprot_id"]
            ),
            "mapping_uniprot_id": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Маппинг UniProt ID"),
                ["normalize_string_strip", "normalize_string_upper", "normalize_uniprot_id"]
            ),
            "chembl_alternative_name": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Альтернативное название ChEMBL"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "ec_code": add_normalization_metadata(
                Column(pa.String, nullable=True, description="EC код фермента"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "uniprot_ids_all": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Все UniProt ID"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "protein_name_canonical": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Каноническое название белка"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "features_signal_peptide": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Сигнальный пептид (features)"),
                ["normalize_boolean"]
            ),
            "features_transmembrane": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Трансмембранный (features)"),
                ["normalize_boolean"]
            ),
            "features_topology": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Топология (features)"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "ptm_glycosylation": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Гликозилирование (текст)"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "ptm_lipidation": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Липидирование (текст)"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "ptm_disulfide_bond": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Дисульфидная связь (текст)"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "ptm_modified_residue": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Модифицированный остаток (текст)"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "xref_chembl": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Ссылки на ChEMBL"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "xref_uniprot": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Ссылки на UniProt"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "xref_iuphar": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Ссылки на IUPHAR"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "pfam": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Pfam классификация (lowercase)"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "interpro": add_normalization_metadata(
                Column(pa.String, nullable=True, description="InterPro классификация (lowercase)"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "reaction_ec_numbers_uniprot": add_normalization_metadata(
                Column(pa.String, nullable=True, description="EC номера реакций UniProt"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "timestamp_utc": add_normalization_metadata(
                Column(pa.DateTime, nullable=True, description="Временная метка UTC"),
                ["normalize_datetime_iso8601"]
            ),
            "gene_symbol": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Символ гена"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "iuphar_gene_symbol": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Символ гена IUPHAR"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "iuphar_hgnc_id": add_normalization_metadata(
                Column(pa.String, nullable=True, description="HGNC ID IUPHAR"),
                ["normalize_string_strip", "normalize_hgnc_id"]
            ),
            "iuphar_hgnc_name": add_normalization_metadata(
                Column(pa.String, nullable=True, description="HGNC название IUPHAR"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "iuphar_uniprot_id_primary": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Первичный UniProt ID IUPHAR"),
                ["normalize_string_strip", "normalize_string_upper", "normalize_uniprot_id"]
            ),
            "iuphar_uniprot_name": add_normalization_metadata(
                Column(pa.String, nullable=True, description="UniProt название IUPHAR"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "iuphar_organism": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Организм IUPHAR"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "iuphar_taxon_id": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Таксономический ID IUPHAR"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "iuphar_description": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Описание IUPHAR"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "iuphar_abbreviation": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Аббревиатура IUPHAR"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "unified_name": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Унифицированное название"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "unified_organism": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Унифицированный организм"),
                ["normalize_string_strip", "normalize_string_titlecase"]
            ),
            "unified_tax_id": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Унифицированный таксономический ID"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "unified_target_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Унифицированный тип таргета"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "has_chembl_data": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Есть данные ChEMBL"),
                ["normalize_boolean"]
            ),
            "has_uniprot_data": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Есть данные UniProt"),
                ["normalize_boolean"]
            ),
            "has_iuphar_data": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Есть данные IUPHAR"),
                ["normalize_boolean"]
            ),
            "has_gtopdb_data": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Есть данные GtoPdb"),
                ["normalize_boolean"]
            ),
            "has_name": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Есть название"),
                ["normalize_boolean"]
            ),
            "has_organism": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Есть организм"),
                ["normalize_boolean"]
            ),
            "has_tax_id": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Есть таксономический ID"),
                ["normalize_boolean"]
            ),
            "has_target_type": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Есть тип таргета"),
                ["normalize_boolean"]
            ),
            "multi_source_validated": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Валидировано несколькими источниками"),
                ["normalize_boolean"]
            ),
            "protein_class_pred_L1": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Предсказание класса белка L1"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "extraction_status": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Статус извлечения"),
                ["normalize_string_strip", "normalize_string_lower"]
            ),
            "protein_class_pred_rule_id": add_normalization_metadata(
                Column(pa.String, nullable=True, description="ID правила предсказания класса"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "protein_class_pred_L3": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Предсказание класса белка L3"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "validation_errors": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Ошибки валидации (JSON)"),
                ["normalize_string_strip"]
            ),
            "extraction_errors": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Ошибки извлечения (JSON)"),
                ["normalize_string_strip"]
            ),
            "protein_class_pred_L2": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Предсказание класса белка L2"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "protein_synonym_list": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Список синонимов белка"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "protein_class_pred_confidence": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="Уверенность предсказания класса"),
                ["normalize_float", "normalize_float_range"]
            ),
            "protein_class_pred_evidence": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Доказательства предсказания класса"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
        })
