"""Pandera schemas for target data validation."""

from __future__ import annotations

import importlib.util

from pandera.typing import Series
from pandera import DataFrameSchema, Column, Check

_PANDERA_PANDAS_SPEC = importlib.util.find_spec("pandera.pandas")
if _PANDERA_PANDAS_SPEC is not None:  # pragma: no cover - import side effect
    import pandera.pandas as pa  # type: ignore[no-redef]
else:  # pragma: no cover - import side effect
    import pandera as pa


class TargetInputSchema(pa.DataFrameModel):
    """Schema for input target data from CSV files."""

    target_chembl_id: Series[str] = pa.Field(description="ChEMBL target identifier")

    class Config:
        strict = False  # Allow extra columns
        coerce = True


class TargetNormalizedSchema(pa.DataFrameModel):
    """Schema for normalized target data after enrichment."""

    # Business key - only required field
    target_chembl_id: Series[str] = pa.Field(nullable=False)

    class Config:
        strict = False  # allow extra columns from enrichments
        coerce = True


__all__ = ["TargetInputSchema", "TargetNormalizedSchema"]


def validate_target_components_json(value: str | None) -> bool:
    """Validate target_components JSON structure."""
    if not value or value == "":
        return True
    try:
        components = json.loads(value)
        if not isinstance(components, list):
            return False
        for component in components:
            if not isinstance(component, dict):
                return False
            # Check required fields
            if "CHEMBL.TARGET_COMPONENTS.component_id" in component and not isinstance(component["CHEMBL.TARGET_COMPONENTS.component_id"], int):
                return False
            if "accession" in component and not isinstance(component["accession"], str):
                return False
        return True
    except (json.JSONDecodeError, TypeError):
        return False


def validate_target_relations_json(value: str | None) -> bool:
    """Validate target_relations JSON structure."""
    if not value or value == "":
        return True
    try:
        relations = json.loads(value)
        if not isinstance(relations, list):
            return False
        for relation in relations:
            if not isinstance(relation, dict):
                return False
            # Check required fields
            if "target_relation_id" in relation and not isinstance(relation["target_relation_id"], int):
                return False
            if "CHEMBL.TARGETS.target_chembl_id" in relation and not isinstance(relation["CHEMBL.TARGETS.target_chembl_id"], str):
                return False
        return True
    except (json.JSONDecodeError, TypeError):
        return False


def validate_protein_classifications_json(value: str | None) -> bool:
    """Validate protein_classifications JSON structure."""
    if not value or value == "":
        return True
    try:
        classifications = json.loads(value)
        if not isinstance(classifications, list):
            return False
        for classification in classifications:
            if not isinstance(classification, dict):
                return False
            # Check required fields
            if "protein_class_id" in classification and not isinstance(classification["protein_class_id"], int):
                return False
            if "class_level" in classification and not isinstance(classification["class_level"], int):
                return False
        return True
    except (json.JSONDecodeError, TypeError):
        return False


class TargetInputSchema:
    """Схемы для входных данных таргетов."""

    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для входных данных таргетов."""
        return DataFrameSchema(
            {
                "CHEMBL.TARGETS.target_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL target ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID таргета",
                )
            }
        )


class TargetRawSchema:
    """Схемы для сырых данных таргетов из API."""

    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для сырых данных таргетов."""
        return DataFrameSchema(
            {
                # Основные поля ChEMBL
                "CHEMBL.TARGETS.target_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL target ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID таргета",
                ),
                "CHEMBL.TARGETS.pref_name": Column(pa.String, nullable=True, description="Предпочтительное название"),
                "hgnc_name": Column(pa.String, nullable=True, description="Название по HGNC"),
                "hgnc_id": Column(pa.String, nullable=True, description="HGNC ID"),
                "CHEMBL.TARGETS.target_type": Column(pa.String, nullable=True, description="Тип таргета"),
                "CHEMBL.TARGETS.tax_id": Column(pa.Int, nullable=True, description="Таксономический ID"),
                "CHEMBL.TARGETS.species_group_flag": Column(pa.Bool, nullable=True, description="Флаг группировки по видам"),
                "CHEMBL.TARGETS.target_components": Column(pa.String, nullable=True, description="Компоненты таргета"),
                "CHEMBL.PROTEIN_CLASSIFICATION.pref_name": Column(pa.String, nullable=True, description="Классификация белка"),
                "CHEMBL.TARGET_COMPONENTS.xref_id": Column(pa.String, nullable=True, description="Перекрестные ссылки"),
                "reaction_ec_numbers": Column(pa.String, nullable=True, description="EC номера реакций"),
                # UniProt поля
                "uniprot_id_primary": Column(pa.String, nullable=True, description="Первичный UniProt ID"),
                "uniprot_ids_all": Column(pa.String, nullable=True, description="Все UniProt ID"),
                "uniProtkbId": Column(pa.String, nullable=True, description="UniProtKB ID"),
                "secondaryAccessions": Column(pa.String, nullable=True, description="Вторичные accession номера"),
                "secondaryAccessionNames": Column(pa.String, nullable=True, description="Названия вторичных accession"),
                "recommendedName": Column(pa.String, nullable=True, description="Рекомендуемое название"),
                "geneName": Column(pa.String, nullable=True, description="Название гена"),
                "isoform_ids": Column(pa.String, nullable=True, description="ID изоформ"),
                "isoform_names": Column(pa.String, nullable=True, description="Названия изоформ"),
                "isoform_synonyms": Column(pa.String, nullable=True, description="Синонимы изоформ"),
                "protein_name_canonical": Column(pa.String, nullable=True, description="Каноническое название белка"),
                "protein_name_alt": Column(pa.String, nullable=True, description="Альтернативное название белка"),
                "taxon_id": Column(pa.Int, nullable=True, description="Таксономический ID"),
                "lineage_superkingdom": Column(pa.String, nullable=True, description="Суперцарство в линии"),
                "lineage_phylum": Column(pa.String, nullable=True, description="Тип в линии"),
                "lineage_class": Column(pa.String, nullable=True, description="Класс в линии"),
                "sequence_length": Column(pa.Int, nullable=True, description="Длина последовательности"),
                "features_signal_peptide": Column(pa.Bool, nullable=True, description="Сигнальный пептид (features)"),
                "features_transmembrane": Column(pa.Bool, nullable=True, description="Трансмембранный (features)"),
                "features_topology": Column(pa.String, nullable=True, description="Топология (features)"),
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
                "ptm_glycosylation": Column(pa.String, nullable=True, description="Гликозилирование (текст)"),
                "ptm_lipidation": Column(pa.String, nullable=True, description="Липидирование (текст)"),
                "ptm_disulfide_bond": Column(pa.String, nullable=True, description="Дисульфидная связь (текст)"),
                "ptm_modified_residue": Column(pa.String, nullable=True, description="Модифицированный остаток (текст)"),
                "xref_chembl": Column(pa.String, nullable=True, description="Ссылки на ChEMBL"),
                "xref_uniprot": Column(pa.String, nullable=True, description="Ссылки на UniProt"),
                "xref_ensembl": Column(pa.String, nullable=True, description="Ссылки на Ensembl"),
                "xref_iuphar": Column(pa.String, nullable=True, description="Ссылки на IUPHAR"),
                "xref_pdb": Column(pa.String, nullable=True, description="Ссылки на PDB"),
                "xref_alphafold": Column(pa.String, nullable=True, description="Ссылки на AlphaFold"),
                "GuidetoPHARMACOLOGY": Column(pa.String, nullable=True, description="Guide to Pharmacology"),
                "family": Column(pa.String, nullable=True, description="Семейство"),
                "SUPFAM": Column(pa.String, nullable=True, description="SUPFAM классификация"),
                "PROSITE": Column(pa.String, nullable=True, description="PROSITE классификация"),
                "InterPro": Column(pa.String, nullable=True, description="InterPro классификация"),
                "Pfam": Column(pa.String, nullable=True, description="Pfam классификация"),
                "PRINTS": Column(pa.String, nullable=True, description="PRINTS классификация"),
                "TCDB": Column(pa.String, nullable=True, description="TCDB классификация"),
                "pfam": Column(pa.String, nullable=True, description="Pfam классификация (lowercase)"),
                "interpro": Column(pa.String, nullable=True, description="InterPro классификация (lowercase)"),
                "reactions": Column(pa.String, nullable=True, description="Реакции"),
                "reaction_ec_numbers_uniprot": Column(pa.String, nullable=True, description="EC номера реакций UniProt"),
                "uniprot_last_update": Column(pa.String, nullable=True, description="Последнее обновление UniProt"),
                "uniprot_version": Column(pa.Int, nullable=True, description="Версия UniProt"),
                "pipeline_version": Column(pa.String, nullable=False, description="Версия пайплайна"),
                "timestamp_utc": Column(pa.DateTime, nullable=True, description="Временная метка UTC"),
                "gene_symbol": Column(pa.String, nullable=True, description="Символ гена"),
                "gene_symbol_list": Column(pa.String, nullable=True, description="Список символов генов"),
                "CHEMBL.TARGETS.organism": Column(pa.String, nullable=True, description="Организм"),
                # IUPHAR поля
                "iuphar_target_id": Column(pa.Int, nullable=True, description="IUPHAR ID таргета"),
                "iuphar_name": Column(pa.String, nullable=True, description="Название по IUPHAR"),
                "iuphar_family_id": Column(pa.Int, nullable=True, description="IUPHAR ID семейства"),
                "iuphar_full_id_path": Column(pa.String, nullable=True, description="Полный путь ID по IUPHAR"),
                "iuphar_full_name_path": Column(pa.String, nullable=True, description="Полный путь названий по IUPHAR"),
                "iuphar_type": Column(pa.String, nullable=True, description="Тип по IUPHAR"),
                "iuphar_class": Column(pa.String, nullable=True, description="Класс по IUPHAR"),
                "iuphar_subclass": Column(pa.String, nullable=True, description="Подкласс по IUPHAR"),
                "iuphar_chain": Column(pa.String, nullable=True, description="Цепь по IUPHAR"),
                "iuphar_gene_symbol": Column(pa.String, nullable=True, description="Символ гена IUPHAR"),
                "iuphar_hgnc_id": Column(pa.String, nullable=True, description="HGNC ID IUPHAR"),
                "iuphar_hgnc_name": Column(pa.String, nullable=True, description="HGNC название IUPHAR"),
                "iuphar_uniprot_id_primary": Column(pa.String, nullable=True, description="Первичный UniProt ID IUPHAR"),
                "iuphar_uniprot_name": Column(pa.String, nullable=True, description="UniProt название IUPHAR"),
                "iuphar_organism": Column(pa.String, nullable=True, description="Организм IUPHAR"),
                "iuphar_taxon_id": Column(pa.Int, nullable=True, description="Таксономический ID IUPHAR"),
                "iuphar_description": Column(pa.String, nullable=True, description="Описание IUPHAR"),
                "iuphar_abbreviation": Column(pa.String, nullable=True, description="Аббревиатура IUPHAR"),
                # GtoP поля
                "gtop_target_id": Column(pa.Int, nullable=True, description="GtoP ID таргета"),
                "gtop_synonyms": Column(pa.String, nullable=True, description="Синонимы по GtoP"),
                "gtop_natural_ligands_n": Column(pa.Int, nullable=True, description="Количество природных лигандов"),
                "gtop_interactions_n": Column(pa.Int, nullable=True, description="Количество взаимодействий"),
                "gtop_function_text_short": Column(pa.String, nullable=True, description="Краткое описание функции"),
                # Unified fields
                "unified_name": Column(pa.String, nullable=True, description="Унифицированное название"),
                "unified_organism": Column(pa.String, nullable=True, description="Унифицированный организм"),
                "unified_tax_id": Column(pa.Int, nullable=True, description="Унифицированный таксономический ID"),
                "unified_target_type": Column(pa.String, nullable=True, description="Унифицированный тип таргета"),
                # Data quality flags
                "has_chembl_data": Column(pa.Bool, nullable=True, description="Есть данные ChEMBL"),
                "has_uniprot_data": Column(pa.Bool, nullable=True, description="Есть данные UniProt"),
                "has_iuphar_data": Column(pa.Bool, nullable=True, description="Есть данные IUPHAR"),
                "has_gtopdb_data": Column(pa.Bool, nullable=True, description="Есть данные GtoPdb"),
                "has_name": Column(pa.Bool, nullable=True, description="Есть название"),
                "has_organism": Column(pa.Bool, nullable=True, description="Есть организм"),
                "has_tax_id": Column(pa.Bool, nullable=True, description="Есть таксономический ID"),
                "has_target_type": Column(pa.Bool, nullable=True, description="Есть тип таргета"),
                "multi_source_validated": Column(pa.Bool, nullable=True, description="Валидировано несколькими источниками"),
                # Protein class predictions
                "protein_class_pred_L1": Column(pa.String, nullable=True, description="Предсказание класса белка L1"),
                # System metadata
                "index": Column(pa.Int, nullable=False, description="Порядковый номер записи"),
                "extraction_status": Column(pa.String, nullable=False, description="Статус извлечения"),
                "protein_class_pred_rule_id": Column(pa.String, nullable=True, description="ID правила предсказания класса"),
                "source_system": Column(pa.String, nullable=False, description="Система-источник"),
                "protein_class_pred_L3": Column(pa.String, nullable=True, description="Предсказание класса белка L3"),
                "hash_business_key": Column(pa.String, nullable=False, description="Хеш бизнес-ключа SHA256"),
                "validation_errors": Column(pa.String, nullable=True, description="Ошибки валидации (JSON)"),
                "extraction_errors": Column(pa.String, nullable=True, description="Ошибки извлечения (JSON)"),
                "extracted_at": Column(pa.Object, nullable=False, description="Время извлечения данных"),
                "protein_class_pred_L2": Column(pa.String, nullable=True, description="Предсказание класса белка L2"),
                "protein_synonym_list": Column(pa.String, nullable=True, description="Список синонимов белка"),
                "chembl_release": Column(pa.String, nullable=True, description="Версия ChEMBL"),
                "protein_class_pred_confidence": Column(pa.Float, nullable=True, description="Уверенность предсказания класса"),
                "protein_class_pred_evidence": Column(pa.String, nullable=True, description="Доказательства предсказания класса"),
                "hash_row": Column(pa.String, nullable=False, description="Хеш строки SHA256"),
            }
        )


class TargetNormalizedSchema:
    """Схемы для нормализованных данных таргетов."""

    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для нормализованных данных таргетов."""
        return DataFrameSchema(
            {
                # Основные поля
                "CHEMBL.TARGETS.target_chembl_id": Column(
                    pa.String,
                    checks=[Check.str_matches(r"^CHEMBL\d+$", error="Invalid ChEMBL target ID format"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="ChEMBL ID таргета",
                ),
                "CHEMBL.TARGETS.pref_name": Column(
                    pa.String,
                    checks=[Check(lambda x: x.isna() | (x.str.len() <= 255), error="pref_name must be ≤ 255 characters")],
                    nullable=True,
                    description="Предпочтительное название",
                ),
                "hgnc_name": Column(pa.String, nullable=True, description="Название по HGNC"),
                "hgnc_id": Column(
                    pa.String,
                    checks=[Check(lambda x: x.isna() | (x.str.len() == 0) | x.str.match(r"^HGNC:\d+$"), error="Invalid HGNC ID format")],
                    nullable=True,
                    description="HGNC ID",
                ),
                "CHEMBL.TARGETS.target_type": Column(
                    pa.String,
                    checks=[
                        Check(
                            lambda x: x.isna() | (x == "") | x.isin(["SINGLE PROTEIN", "PROTEIN COMPLEX", "PROTEIN FAMILY", "PROTEIN-PROTEIN INTERACTION", "NUCLEIC-ACID", "ORGANISM"]),
                            error="Invalid target_type",
                        )
                    ],
                    nullable=True,
                    description="Тип таргета",
                ),
                "CHEMBL.TARGETS.tax_id": Column(
                    pa.Int, checks=[Check(lambda x: x.isna() | (x > 0), error="Taxonomy ID must be > 0")], nullable=True, description="Таксономический ID"
                ),
                "CHEMBL.TARGETS.species_group_flag": Column(
                    pa.Bool, nullable=True, description="Флаг группировки по видам"
                ),
                "CHEMBL.TARGETS.target_components": Column(
                    pa.String,
                    checks=[Check(lambda x: x.apply(validate_target_components_json), error="Invalid target_components JSON structure")],
                    nullable=True,
                    description="Компоненты таргета",
                ),
                "CHEMBL.PROTEIN_CLASSIFICATION.pref_name": Column(
                    pa.String,
                    checks=[Check(lambda x: x.apply(validate_protein_classifications_json), error="Invalid protein_classifications JSON structure")],
                    nullable=True,
                    description="Классификация белка",
                ),
                "CHEMBL.TARGET_COMPONENTS.xref_id": Column(pa.String, nullable=True, description="Перекрестные ссылки"),
                "reaction_ec_numbers": Column(pa.String, nullable=True, description="EC номера реакций"),
                # UniProt поля
                "uniprot_id_primary": Column(
                    pa.String,
                    checks=[
                        Check(
                            lambda x: x.isna() | (x.str.len() == 0) | x.str.match(r"^[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$"),
                            error="Invalid UniProt ID format",
                        )
                    ],
                    nullable=True,
                    description="Первичный UniProt ID",
                ),
                "uniProtkbId": Column(pa.String, nullable=True, description="UniProtKB ID"),
                "secondaryAccessions": Column(pa.String, nullable=True, description="Вторичные accession номера"),
                "secondaryAccessionNames": Column(pa.String, nullable=True, description="Названия вторичных accession"),
                "recommendedName": Column(pa.String, nullable=True, description="Рекомендуемое название"),
                "protein_name_alt": Column(pa.String, nullable=True, description="Альтернативное название белка"),
                "geneName": Column(pa.String, nullable=True, description="Название гена"),
                "gene_symbol_list": Column(pa.String, nullable=True, description="Список символов генов"),
                "CHEMBL.TARGETS.organism": Column(pa.String, nullable=True, description="Организм"),
                "taxon_id": Column(pa.Int, checks=[Check(lambda x: x.isna() | (x >= 0), error="Taxon ID must be >= 0")], nullable=True, description="Таксономический ID"),
                "lineage_superkingdom": Column(pa.String, nullable=True, description="Суперцарство в линии"),
                "lineage_phylum": Column(pa.String, nullable=True, description="Тип в линии"),
                "lineage_class": Column(pa.String, nullable=True, description="Класс в линии"),
                "sequence_length": Column(
                    pa.Int, checks=[Check(lambda x: x.isna() | (x > 0), error="Sequence length must be > 0")], nullable=True, description="Длина последовательности"
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
                "uniprot_last_update": Column(pa.String, nullable=True, description="Последнее обновление UniProt"),
                "uniprot_version": Column(pa.Int, checks=[Check(lambda x: x.isna() | (x > 0), error="UniProt version must be > 0")], nullable=True, description="Версия UniProt"),
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
                    pa.Int, checks=[Check(lambda x: x.isna() | (x >= 0), error="Natural ligands count must be >= 0")], nullable=True, description="Количество природных лигандов"
                ),
                "gtop_interactions_n": Column(
                    pa.Int, checks=[Check(lambda x: x.isna() | (x >= 0), error="Interactions count must be >= 0")], nullable=True, description="Количество взаимодействий"
                ),
                "gtop_function_text_short": Column(pa.String, nullable=True, description="Краткое описание функции"),
                # Системные поля
                "index": Column(
                    pa.Int,
                    checks=[Check.greater_than_or_equal_to(0, error="Index must be >= 0"), Check(lambda x: x.notna())],
                    nullable=False,
                    description="Порядковый номер записи",
                ),
                "pipeline_version": Column(pa.String, checks=[Check(lambda x: x.notna())], nullable=False, description="Версия пайплайна"),
                "source_system": Column(pa.String, checks=[Check(lambda x: x.notna())], nullable=False, description="Система-источник"),
                "chembl_release": Column(pa.String, nullable=True, description="Версия ChEMBL"),
                "extracted_at": Column(pa.Object, checks=[Check(lambda x: x.notna())], nullable=False, description="Время извлечения данных"),
                "hash_row": Column(pa.String, checks=[Check(lambda x: x.notna())], nullable=False, description="Хеш строки SHA256"),
                "hash_business_key": Column(pa.String, checks=[Check(lambda x: x.notna())], nullable=False, description="Хеш бизнес-ключа SHA256"),
                # Ошибки и статусы
                "extraction_errors": Column(pa.String, nullable=True, description="Ошибки извлечения (JSON)"),
                "validation_errors": Column(pa.String, nullable=True, description="Ошибки валидации (JSON)"),
                "extraction_status": Column(
                    pa.String, checks=[Check.isin(["success", "partial", "failed"], error="Invalid extraction status")], nullable=True, description="Статус извлечения"
                ),
            }
        )


# DEPRECATED: Legacy validation functions removed.
# Use library.common.validation.validate_entity_data() instead.
#
# Example:
# from library.common.validation import validate_entity_data, ValidationStage
# result = validate_entity_data("target", df, ValidationStage.NORMALIZED)
