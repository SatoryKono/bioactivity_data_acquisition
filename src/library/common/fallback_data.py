"""Fallback data for when ChEMBL API is unavailable."""

from typing import Any


def get_fallback_assay_data(assay_chembl_id: str) -> dict[str, Any]:
    """Get fallback assay data when API is unavailable.

    This provides realistic sample data for testing and development.
    """
    # Sample assay data based on real ChEMBL patterns
    fallback_data = {
        "CHEMBL1000139": {
            "assay_chembl_id": "CHEMBL1000139",
            "assay_type": "B",
            "assay_category": "Binding",
            "assay_cell_type": None,
            "assay_classifications": '[{"assay_class_id": 1, "class_type": "BAO", "l1": "assay", "l2": "binding", "l3": "protein"}]',
            "assay_group": None,
            "assay_organism": "Guinea pig",
            "assay_parameters": [{"type": "IC50", "relation": "=", "value": 100.0, "units": "nM"}],
            "assay_parameters_json": '[{"type": "IC50", "relation": "=", "value": 100.0, "units": "nM"}]',
            "assay_strain": None,
            "assay_subcellular_fraction": "Membrane",
            "assay_tax_id": 10141,
            "assay_test_type": "Displacement",
            "assay_tissue": "Liver",
            "assay_type_description": "Binding assay",
            "bao_format": "BAO_0000249",
            "bao_label": "binding assay",
            "bao_endpoint": "BAO_0000048",
            "cell_chembl_id": None,
            "confidence_description": "High confidence",
            "confidence_score": 9,
            "assay_description": "Displacement of [3H](-)-(S)-emopamil from EBP in guinea pig liver membrane",
            "document_chembl_id": "CHEMBL1140235",
            "relationship_description": "Direct protein target",
            "relationship_type": "D",
            "src_assay_id": "1000139",
            "src_id": 1,
            "target_chembl_id": "CHEMBL5525",
            "tissue_chembl_id": None,
            "variant_sequence": None,
            "variant_sequence_json": None,
            "source_system": "ChEMBL_FALLBACK",
            "extracted_at": "2025-01-01T00:00:00Z",
        },
        "CHEMBL1000140": {
            "assay_chembl_id": "CHEMBL1000140",
            "assay_type": "B",
            "assay_category": "Binding",
            "assay_cell_type": None,
            "assay_classifications": '[{"assay_class_id": 1, "class_type": "BAO", "l1": "assay", "l2": "binding", "l3": "protein"}]',
            "assay_group": None,
            "assay_organism": "Guinea pig",
            "assay_parameters": [{"type": "IC50", "relation": "=", "value": 50.0, "units": "nM"}],
            "assay_parameters_json": '[{"type": "IC50", "relation": "=", "value": 50.0, "units": "nM"}]',
            "assay_strain": None,
            "assay_subcellular_fraction": "Membrane",
            "assay_tax_id": 10141,
            "assay_test_type": "Displacement",
            "assay_tissue": "Brain",
            "assay_type_description": "Binding assay",
            "bao_format": "BAO_0000249",
            "bao_label": "binding assay",
            "bao_endpoint": "BAO_0000048",
            "cell_chembl_id": None,
            "confidence_description": "High confidence",
            "confidence_score": 9,
            "assay_description": "Displacement of [3H](+)-pentazocine from sigma 1 receptor in guinea pig brain membrane",
            "document_chembl_id": "CHEMBL1140235",
            "relationship_description": "Direct protein target",
            "relationship_type": "D",
            "src_assay_id": "1000140",
            "src_id": 1,
            "target_chembl_id": "CHEMBL4153",
            "tissue_chembl_id": None,
            "variant_sequence": None,
            "variant_sequence_json": None,
            "source_system": "ChEMBL_FALLBACK",
            "extracted_at": "2025-01-01T00:00:00Z",
        },
    }

    # Return specific assay data or generic fallback
    if assay_chembl_id in fallback_data:
        return fallback_data[assay_chembl_id]

    # Generic fallback for unknown assays
    return {
        "assay_chembl_id": assay_chembl_id,
        "assay_type": "B",
        "assay_category": "Binding",
        "assay_cell_type": None,
        "assay_classifications": None,
        "assay_group": None,
        "assay_organism": "Human",
        "assay_parameters": None,
        "assay_parameters_json": None,
        "assay_strain": None,
        "assay_subcellular_fraction": None,
        "assay_tax_id": 9606,
        "assay_test_type": "Inhibition",
        "assay_tissue": None,
        "assay_type_description": "Generic binding assay",
        "bao_format": "BAO_0000249",
        "bao_label": "binding assay",
        "bao_endpoint": "BAO_0000048",
        "cell_chembl_id": None,
        "confidence_description": "Medium confidence",
        "confidence_score": 5,
        "assay_description": f"Fallback data for assay {assay_chembl_id}",
        "document_chembl_id": None,
        "relationship_description": "Direct protein target",
        "relationship_type": "D",
        "src_assay_id": assay_chembl_id.replace("CHEMBL", ""),
        "src_id": 1,
        "target_chembl_id": "CHEMBL0001",  # Generic target
        "tissue_chembl_id": None,
        "variant_sequence": None,
        "variant_sequence_json": None,
        "source_system": "ChEMBL_FALLBACK",
        "extracted_at": "2025-01-01T00:00:00Z",
    }


def get_fallback_target_data(target_chembl_id: str) -> dict[str, Any]:
    """Get fallback target data when API is unavailable."""
    return {
        "target_chembl_id": target_chembl_id,
        "target_organism": "Human",
        "target_tax_id": 9606,
        "target_uniprot_accession": "P12345",
        "target_isoform": None,
        "source_system": "ChEMBL_FALLBACK",
        "extracted_at": "2025-01-01T00:00:00Z",
    }


def get_fallback_assay_class_data(assay_class_id: int) -> dict[str, Any]:
    """Get fallback assay class data when API is unavailable."""
    return {
        "assay_class_id": assay_class_id,
        "assay_class_bao_id": "BAO_0000249",
        "assay_class_type": "binding",
        "assay_class_l1": "assay",
        "assay_class_l2": "binding",
        "assay_class_l3": "protein",
        "assay_class_description": "Generic binding assay class",
    }
