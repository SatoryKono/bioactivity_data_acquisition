"""
Pandera схемы для валидации данных testitems с нормализацией.

Предоставляет схемы для входных, сырых и нормализованных данных testitems
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


class TestitemNormalizedSchema:
    """Схемы для нормализованных данных testitems."""
    
    @staticmethod
    def get_schema() -> DataFrameSchema:
        """Схема для нормализованных данных testitems."""
        return DataFrameSchema({
            # Основные идентификаторы и метаданные
            "molecule_chembl_id": add_normalization_metadata(
                Column(
                    pa.String,
                    checks=[
                        Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL molecule ID format"),
                        Check(lambda x: x.notna())
                    ],
                    nullable=False,
                    description="ChEMBL ID молекулы"
                ),
                ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
            ),
            "molregno": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Номер регистрации молекулы"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "pref_name": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Предпочтительное название молекулы"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "pref_name_key": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Ключ предпочтительного названия"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "parent_chembl_id": add_normalization_metadata(
                Column(pa.String, nullable=True, description="ID родительской молекулы"),
                ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
            ),
            "parent_molregno": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Номер регистрации родительской молекулы"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "max_phase": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Максимальная фаза разработки"),
                ["normalize_int", "normalize_int_range"]
            ),
            "therapeutic_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг терапевтического применения"),
                ["normalize_boolean"]
            ),
            "dosed_ingredient": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг дозируемого ингредиента"),
                ["normalize_boolean"]
            ),
            "first_approval": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Год первого одобрения"),
                ["normalize_int", "normalize_year"]
            ),
            "structure_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип структуры"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "molecule_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип молекулы"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            
            # Физико-химические свойства ChEMBL
            "mw_freebase": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="Молекулярная масса freebase"),
                ["normalize_float", "normalize_molecular_weight"]
            ),
            "alogp": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="A log P"),
                ["normalize_float", "normalize_float_precision"]
            ),
            "hba": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Акцепторы водородных связей"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "hbd": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Доноры водородных связей"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "psa": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="Полярная поверхность"),
                ["normalize_float", "normalize_float_precision"]
            ),
            "rtb": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Количество ротационных связей"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "ro3_pass": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Правило 3"),
                ["normalize_boolean"]
            ),
            "num_ro5_violations": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Количество нарушений правила 5"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "acd_most_apka": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="ACD наиболее кислый pKa"),
                ["normalize_float", "normalize_float_precision"]
            ),
            "acd_most_bpka": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="ACD наиболее основной pKa"),
                ["normalize_float", "normalize_float_precision"]
            ),
            "acd_logp": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="ACD log P"),
                ["normalize_float", "normalize_float_precision"]
            ),
            "acd_logd": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="ACD log D"),
                ["normalize_float", "normalize_float_precision"]
            ),
            "molecular_species": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Молекулярный вид"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "full_mwt": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="Полная молекулярная масса"),
                ["normalize_float", "normalize_molecular_weight"]
            ),
            "aromatic_rings": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="Количество ароматических колец"),
                ["normalize_float", "normalize_float_precision"]
            ),
            "heavy_atoms": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="Количество тяжелых атомов"),
                ["normalize_float", "normalize_float_precision"]
            ),
            "qed_weighted": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="QED взвешенный"),
                ["normalize_float", "normalize_float_precision"]
            ),
            "mw_monoisotopic": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="Моноизотопная молекулярная масса"),
                ["normalize_float", "normalize_molecular_weight"]
            ),
            "full_molformula": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Полная молекулярная формула"),
                ["normalize_string_strip", "normalize_string_upper", "normalize_molecular_formula"]
            ),
            "hba_lipinski": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="HBA Lipinski"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "hbd_lipinski": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="HBD Lipinski"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "num_lipinski_ro5_violations": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Количество нарушений Lipinski Ro5"),
                ["normalize_int", "normalize_int_positive"]
            ),
            
            # Пути введения и флаги
            "oral": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Оральный путь введения"),
                ["normalize_boolean"]
            ),
            "parenteral": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Парентеральный путь введения"),
                ["normalize_boolean"]
            ),
            "topical": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Топический путь введения"),
                ["normalize_boolean"]
            ),
            "black_box_warning": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Предупреждение черного ящика"),
                ["normalize_boolean"]
            ),
            "natural_product": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Природный продукт"),
                ["normalize_boolean"]
            ),
            "first_in_class": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Первый в классе"),
                ["normalize_boolean"]
            ),
            "chirality": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Хиральность"),
                ["normalize_boolean"]
            ),
            "prodrug": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Пролекарство"),
                ["normalize_boolean"]
            ),
            "inorganic_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Неорганическое соединение"),
                ["normalize_boolean"]
            ),
            "polymer_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Полимер"),
                ["normalize_boolean"]
            ),
            
            # Регистрация и отзыв
            "usan_year": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Год USAN регистрации"),
                ["normalize_int", "normalize_year"]
            ),
            "availability_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип доступности"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "usan_stem": add_normalization_metadata(
                Column(pa.String, nullable=True, description="USAN stem"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "usan_substem": add_normalization_metadata(
                Column(pa.String, nullable=True, description="USAN substem"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "usan_stem_definition": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Определение USAN stem"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "indication_class": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Класс показаний"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "withdrawn_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Отозванное лекарство"),
                ["normalize_boolean"]
            ),
            "withdrawn_year": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Год отзыва"),
                ["normalize_int", "normalize_year"]
            ),
            "withdrawn_country": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Страна отзыва"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "withdrawn_reason": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Причина отзыва"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            
            # Механизм действия
            "mechanism_of_action": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Механизм действия"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "direct_interaction": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Прямое взаимодействие"),
                ["normalize_boolean"]
            ),
            "molecular_mechanism": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Молекулярный механизм"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            
            # Drug данные
            "drug_chembl_id": add_normalization_metadata(
                Column(pa.String, nullable=True, description="ChEMBL ID препарата"),
                ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
            ),
            "drug_name": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Название препарата"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "drug_type": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип препарата"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "drug_substance_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг вещества препарата"),
                ["normalize_boolean"]
            ),
            "drug_indication_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг показаний препарата"),
                ["normalize_boolean"]
            ),
            "drug_antibacterial_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг антибактериального"),
                ["normalize_boolean"]
            ),
            "drug_antiviral_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг противовирусного"),
                ["normalize_boolean"]
            ),
            "drug_antifungal_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг противогрибкового"),
                ["normalize_boolean"]
            ),
            "drug_antiparasitic_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг противопаразитарного"),
                ["normalize_boolean"]
            ),
            "drug_antineoplastic_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг противоопухолевого"),
                ["normalize_boolean"]
            ),
            "drug_immunosuppressant_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг иммуносупрессанта"),
                ["normalize_boolean"]
            ),
            "drug_antiinflammatory_flag": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг противовоспалительного"),
                ["normalize_boolean"]
            ),
            
            # PubChem данные
            "pubchem_cid": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="PubChem CID"),
                ["normalize_int", "normalize_int_positive", "normalize_pubchem_cid"]
            ),
            "pubchem_molecular_formula": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Молекулярная формула PubChem"),
                ["normalize_string_strip", "normalize_string_upper", "normalize_molecular_formula"]
            ),
            "pubchem_molecular_weight": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="Молекулярная масса PubChem"),
                ["normalize_float", "normalize_molecular_weight"]
            ),
            "pubchem_canonical_smiles": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Канонические SMILES PubChem"),
                ["normalize_string_strip", "normalize_smiles", "normalize_smiles_canonical"]
            ),
            "pubchem_isomeric_smiles": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Изомерные SMILES PubChem"),
                ["normalize_string_strip", "normalize_smiles"]
            ),
            "pubchem_inchi": add_normalization_metadata(
                Column(pa.String, nullable=True, description="InChI PubChem"),
                ["normalize_string_strip", "normalize_inchi"]
            ),
            "pubchem_inchi_key": add_normalization_metadata(
                Column(pa.String, nullable=True, description="InChI Key PubChem"),
                ["normalize_string_strip", "normalize_inchi_key"]
            ),
            "pubchem_registry_id": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Registry ID PubChem"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "pubchem_rn": add_normalization_metadata(
                Column(pa.String, nullable=True, description="RN PubChem"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            
            # Стандартизированные структуры
            "standardized_inchi": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Стандартизированный InChI"),
                ["normalize_string_strip", "normalize_inchi"]
            ),
            "standardized_inchi_key": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Стандартизированный InChI Key"),
                ["normalize_string_strip", "normalize_inchi_key"]
            ),
            "standardized_smiles": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Стандартизированные SMILES"),
                ["normalize_string_strip", "normalize_smiles", "normalize_smiles_canonical"]
            ),
            
            # PubChem данные
            "pubchem_cid": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="PubChem CID"),
                ["normalize_int", "normalize_int_positive", "normalize_pubchem_cid"]
            ),
            "pubchem_molecular_formula": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Молекулярная формула PubChem"),
                ["normalize_string_strip", "normalize_string_upper", "normalize_molecular_formula"]
            ),
            "pubchem_molecular_weight": add_normalization_metadata(
                Column(pa.Float64, nullable=True, description="Молекулярная масса PubChem"),
                ["normalize_float", "normalize_molecular_weight"]
            ),
            "pubchem_canonical_smiles": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Канонические SMILES PubChem"),
                ["normalize_string_strip", "normalize_smiles", "normalize_smiles_canonical"]
            ),
            "pubchem_isomeric_smiles": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Изомерные SMILES PubChem"),
                ["normalize_string_strip", "normalize_smiles"]
            ),
            "pubchem_inchi": add_normalization_metadata(
                Column(pa.String, nullable=True, description="InChI PubChem"),
                ["normalize_string_strip", "normalize_inchi"]
            ),
            "pubchem_inchi_key": add_normalization_metadata(
                Column(pa.String, nullable=True, description="InChI Key PubChem"),
                ["normalize_string_strip", "normalize_inchi_key"]
            ),
            "pubchem_registry_id": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Registry ID PubChem"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "pubchem_rn": add_normalization_metadata(
                Column(pa.String, nullable=True, description="RN PubChem"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            
            # Стандартизированные структуры
            "standardized_inchi": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Стандартизированный InChI"),
                ["normalize_string_strip", "normalize_inchi"]
            ),
            "standardized_inchi_key": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Стандартизированный InChI Key"),
                ["normalize_string_strip", "normalize_inchi_key"]
            ),
            "standardized_smiles": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Стандартизированные SMILES"),
                ["normalize_string_strip", "normalize_smiles", "normalize_smiles_canonical"]
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
                Column(
                    pa.DateTime,
                    checks=[Check(lambda x: x.notna())],
                    nullable=False,
                    description="Время извлечения данных"
                ),
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
            
            # Входные данные из input файла
            "all_names": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Все названия молекулы"),
                ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
            ),
            "canonical_smiles": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Канонические SMILES из входных данных"),
                ["normalize_string_strip", "normalize_smiles", "normalize_smiles_canonical"]
            ),
            "inchi_key_from_mol": add_normalization_metadata(
                Column(pa.String, nullable=True, description="InChI ключ из молекулы"),
                ["normalize_string_strip", "normalize_inchi_key"]
            ),
            "inchi_key_from_smiles": add_normalization_metadata(
                Column(pa.String, nullable=True, description="InChI ключ из SMILES"),
                ["normalize_string_strip", "normalize_inchi_key"]
            ),
            "is_radical": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Флаг радикала"),
                ["normalize_boolean"]
            ),
            "mw_<100_or_>1000": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Молекулярная масса <100 или >1000"),
                ["normalize_boolean"]
            ),
            "n_stereocenters": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Количество стереоцентров"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "nstereo": add_normalization_metadata(
                Column(pa.Int64, nullable=True, description="Количество стереоизомеров"),
                ["normalize_int", "normalize_int_positive"]
            ),
            "salt_chembl_id": add_normalization_metadata(
                Column(pa.String, nullable=True, description="ChEMBL ID соли"),
                ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
            ),
            "standard_inchi_key": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Стандартный InChI ключ"),
                ["normalize_string_strip", "normalize_inchi_key"]
            ),
            "standard_inchi_skeleton": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Стандартный InChI скелет"),
                ["normalize_string_strip", "normalize_inchi"]
            ),
            "standard_inchi_stereo": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Стандартный InChI стерео"),
                ["normalize_string_strip", "normalize_inchi"]
            ),
            
            # Вложенные структуры ChEMBL (JSON/распакованные)
            "atc_classifications": add_normalization_metadata(
                Column(pa.String, nullable=True, description="ATC классификации (JSON)"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "biotherapeutic": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Биотерапевтическое соединение (JSON)"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "chemical_probe": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Химический зонд"),
                ["normalize_boolean"]
            ),
            "cross_references": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Перекрестные ссылки (JSON)"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "helm_notation": add_normalization_metadata(
                Column(pa.String, nullable=True, description="HELM нотация"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "molecule_hierarchy": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Иерархия молекулы (JSON)"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "molecule_properties": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Свойства молекулы (JSON)"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "molecule_structures": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Структуры молекулы (JSON)"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "molecule_synonyms": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Синонимы молекулы (JSON)"),
                ["normalize_string_strip", "normalize_string_nfc"]
            ),
            "orphan": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Орфанное лекарство"),
                ["normalize_boolean"]
            ),
            "veterinary": add_normalization_metadata(
                Column(pa.Bool, nullable=True, description="Ветеринарное лекарство"),
                ["normalize_boolean"]
            ),
            "standard_inchi": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Стандартный InChI"),
                ["normalize_string_strip", "normalize_inchi"]
            ),
            "chirality_chembl": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Хиральность из ChEMBL"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
            "molecule_type_chembl": add_normalization_metadata(
                Column(pa.String, nullable=True, description="Тип молекулы из ChEMBL"),
                ["normalize_string_strip", "normalize_string_upper"]
            ),
        })
