from __future__ import annotations

from pipelines.base import FileBasedPipeline, PipelineSpec
from pipelines.schemas import testitems_schema

_TESTITEMS_SPEC = PipelineSpec(
    name="testitems_chembl",
    business_key=("molecule_chembl_id",),
    required_fields=("molecule_chembl_id", "_chembl_db_version", "_api_version", "load_meta_id"),
    optional_fields=(
        "pref_name",
        "molecule_type",
        "max_phase",
        "first_approval",
        "first_in_class",
        "availability_type",
        "black_box_warning",
        "chirality",
        "dosed_ingredient",
        "helm_notation",
        "indication_class",
        "inorganic_flag",
        "natural_product",
        "prodrug",
        "structure_type",
        "therapeutic_flag",
        "molecule_hierarchy__molecule_chembl_id",
        "molecule_hierarchy__parent_chembl_id",
        "molecule_structures__canonical_smiles",
        "molecule_structures__molfile",
        "molecule_structures__standard_inchi",
        "molecule_structures__standard_inchi_key",
        "molecule_properties__alogp",
        "molecule_properties__aromatic_rings",
        "molecule_properties__cx_logd",
        "molecule_properties__cx_logp",
        "molecule_properties__cx_most_apka",
        "molecule_properties__cx_most_bpka",
        "molecule_properties__full_molformula",
        "molecule_properties__full_mwt",
        "molecule_properties__hba",
        "molecule_properties__hba_lipinski",
        "molecule_properties__hbd",
        "molecule_properties__hbd_lipinski",
        "molecule_properties__heavy_atoms",
        "molecule_properties__molecular_species",
        "molecule_properties__mw_freebase",
        "molecule_properties__mw_monoisotopic",
        "molecule_properties__num_lipinski_ro5_violations",
        "molecule_properties__num_ro5_violations",
        "molecule_properties__psa",
        "molecule_properties__qed_weighted",
        "molecule_properties__ro3_pass",
        "molecule_properties__rtb",
        "atc_classifications",
        "cross_references__flat",
        "molecule_synonyms__flat",
    ),
    require_hash_business_key=True,
)


class TestItemsChemblPipeline(FileBasedPipeline):
    """Пайплайн загрузки размерности тестовых элементов из ChEMBL."""

    def __init__(self, run_id: str, config, strict_validation: bool = False) -> None:
        super().__init__(
            run_id,
            config=config,
            spec=_TESTITEMS_SPEC,
            schema=testitems_schema(),
            strict_validation=strict_validation,
        )


__all__ = ["TestItemsChemblPipeline"]
