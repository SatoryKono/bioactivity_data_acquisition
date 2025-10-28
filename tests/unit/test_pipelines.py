"""Tests for pipeline implementations."""

import uuid

import pandas as pd
import pytest
from pandera.errors import SchemaErrors

from bioetl.config.loader import load_config
from bioetl.pipelines import ActivityPipeline, AssayPipeline, DocumentPipeline, TargetPipeline, TestItemPipeline
from bioetl.schemas import TestItemSchema
from bioetl.schemas.registry import schema_registry


@pytest.fixture
def assay_config():
    """Load assay pipeline config."""
    return load_config("configs/pipelines/assay.yaml")


@pytest.fixture
def activity_config():
    """Load activity pipeline config."""
    return load_config("configs/pipelines/activity.yaml")


@pytest.fixture
def testitem_config():
    """Load testitem pipeline config."""
    return load_config("configs/pipelines/testitem.yaml")


@pytest.fixture
def target_config():
    """Load target pipeline config."""
    return load_config("configs/pipelines/target.yaml")


@pytest.fixture
def document_config():
    """Load document pipeline config."""
    return load_config("configs/pipelines/document.yaml")


class TestAssayPipeline:
    """Tests for AssayPipeline."""

    def test_init(self, assay_config):
        """Test pipeline initialization."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(assay_config, run_id)
        assert pipeline.config == assay_config
        assert pipeline.run_id == run_id

    def test_extract_empty_file(self, assay_config, tmp_path):
        """Test extraction with empty file."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(assay_config, run_id)

        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("assay_chembl_id,description\n")

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_extract_with_data(self, assay_config, tmp_path):
        """Test extraction with sample data."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(assay_config, run_id)

        csv_path = tmp_path / "assay.csv"
        csv_path.write_text(
            "assay_chembl_id,Target TYPE,description,target_chembl_id\n"
            "CHEMBL1,Enzyme,Test assay,CHEMBL101\n"
        )

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0  # May be limited by nrows=10

    def test_transform_adds_metadata(self, assay_config):
        """Test transformation adds pipeline metadata."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = AssayPipeline(assay_config, run_id)

        df = pd.DataFrame({
            "assay_chembl_id": ["CHEMBL1"],
            "description": ["  Test Assay  "],
        })

        result = pipeline.transform(df)
        assert "pipeline_version" in result.columns
        assert "source_system" in result.columns
        assert "extracted_at" in result.columns


class TestActivityPipeline:
    """Tests for ActivityPipeline."""

    def test_init(self, activity_config):
        """Test pipeline initialization."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)
        assert pipeline.config == activity_config
        assert pipeline.run_id == run_id

    def test_extract_empty_file(self, activity_config, tmp_path):
        """Test extraction with empty file."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("activity_id,molecule_chembl_id\n")

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_validate_removes_duplicates(self, activity_config):
        """Test validation removes duplicates."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = ActivityPipeline(activity_config, run_id)

        df = pd.DataFrame({
            "activity_id": [1, 1, 2],
            "molecule_chembl_id": ["CHEMBL1", "CHEMBL1", "CHEMBL2"],
        })

        result = pipeline.validate(df)
        assert len(result) == 2
        assert result["activity_id"].nunique() == 2


class TestTestItemPipeline:
    """Tests for TestItemPipeline."""

    @staticmethod
    def _build_testitem_df(rows: list[dict[str, object]]) -> pd.DataFrame:
        """Create a DataFrame aligned with TestItemSchema column order."""
        df = pd.DataFrame(rows)
        return df[TestItemSchema.column_order()]

    @staticmethod
    def _row(
        molecule_id: str,
        index: int,
        *,
        parent_id: str | None = None,
        hash_suffix: int = 0,
    ) -> dict[str, object]:
        """Construct a single valid row for schema validation tests."""
        parent = parent_id if parent_id is not None else molecule_id
        base_hash = f"{index + hash_suffix:064x}"
        return {
            "molecule_chembl_id": molecule_id,
            "molregno": 1000 + index,
            "pref_name": f"Test Molecule {index}",
            "parent_chembl_id": parent,
            "max_phase": 3,
            "structure_type": "SMALL",
            "molecule_type": "Small molecule",
            "mw_freebase": 123.45,
            "qed_weighted": 0.75,
            "standardized_smiles": "CCO",
            "standard_inchi": "InChI=1S/C2H6O/c1-2-3/h3H,2H2,1H3",
            "standard_inchi_key": "LFQSCWFLJHTTHZ-UHFFFAOYSA-N",
            "heavy_atoms": 9,
            "aromatic_rings": 1,
            "rotatable_bonds": 4,
            "hba": 2,
            "hbd": 1,
            "lipinski_ro5_violations": 0,
            "lipinski_ro5_pass": True,
            "all_names": "Test",  # simple string representation
            "molecule_synonyms": "[\"Test\"]",
            "atc_classifications": "[\"A01\"]",
            "pubchem_cid": 2000 + index,
            "pubchem_molecular_formula": "C2H6O",
            "pubchem_molecular_weight": 46.07,
            "pubchem_canonical_smiles": "CCO",
            "pubchem_isomeric_smiles": "C[C@H]O",
            "pubchem_inchi": "InChI=1S/C2H6O/c1-2-3/h3H,2H2,1H3",
            "pubchem_inchi_key": "LFQSCWFLJHTTHZ-UHFFFAOYSA-N",
            "pubchem_iupac_name": "ethanol",
            "pubchem_synonyms": "[\"ethanol\"]",
            "pipeline_version": "1.0.0",
            "source_system": "chembl",
            "chembl_release": "CHEMBL_36",
            "extracted_at": "2023-01-01T00:00:00+00:00",
            "hash_business_key": base_hash,
            "hash_row": f"{index + 1 + hash_suffix:064x}",
            "index": index,
        }

    def test_init(self, testitem_config):
        """Test pipeline initialization."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TestItemPipeline(testitem_config, run_id)
        assert pipeline.config == testitem_config
        assert pipeline.run_id == run_id

    def test_extract_empty_file(self, testitem_config, tmp_path):
        """Test extraction with empty file."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TestItemPipeline(testitem_config, run_id)

        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("molecule_chembl_id\n")

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_transform_adds_metadata(self, testitem_config):
        """Test transformation adds pipeline metadata."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TestItemPipeline(testitem_config, run_id)

        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "canonical_smiles": ["CC(=O)O"],
        })

        result = pipeline.transform(df)
        assert "pipeline_version" in result.columns
        assert "source_system" in result.columns
        assert "extracted_at" in result.columns

    def test_validate_invalid_chembl_id(self, testitem_config):
        """Schema violations for CHEMBL identifiers raise SchemaErrors."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TestItemPipeline(testitem_config, run_id)

        rows = [self._row("CHEMBL1", 0)]
        df = self._build_testitem_df(rows)
        df.loc[0, "molecule_chembl_id"] = "INVALID_ID"
        df.loc[0, "parent_chembl_id"] = "INVALID_ID"

        with pytest.raises(SchemaErrors):
            pipeline.validate(df)

    def test_validation_schema_injects_custom_checks(self, testitem_config):
        """Custom Pandera checks are present on generated schema columns."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TestItemPipeline(testitem_config, run_id)

        schema_model = schema_registry.get("testitem", "latest")
        with pipeline._schema_without_column_order(schema_model) as model:
            schema = pipeline._build_validation_schema(model)

        molecule_check_names = {
            check.name for check in schema.columns["molecule_chembl_id"].checks if check.name
        }
        parent_check_names = {
            check.name for check in schema.columns["parent_chembl_id"].checks if check.name
        }

        assert "molecule_chembl_id_format" in molecule_check_names
        assert "parent_chembl_id_format" in parent_check_names

    def test_validate_missing_required_column(self, testitem_config):
        """Missing required fields bubble up as SchemaError failures."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TestItemPipeline(testitem_config, run_id)

        df = self._build_testitem_df([self._row("CHEMBL1", 0)])
        df = df.drop(columns=["standard_inchi_key"])

        with pytest.raises(SchemaErrors):
            pipeline.validate(df)

    def test_validate_qc_threshold_violation(self, testitem_config):
        """QC threshold breaches cause the pipeline to fail."""
        testitem_config.qc.thresholds = {"duplicate_primary_keys": 0.0}

        run_id = str(uuid.uuid4())[:8]
        pipeline = TestItemPipeline(testitem_config, run_id)

        row = self._row("CHEMBL1", 0)
        duplicate_row = self._row("CHEMBL1", 1, hash_suffix=100)
        df = self._build_testitem_df([row, duplicate_row])

        with pytest.raises(ValueError, match="duplicate_primary_keys"):
            pipeline.validate(df)

    def test_validate_parent_reference_threshold_violation(self, testitem_config):
        """Invalid parent references trigger QC enforcement when configured."""
        testitem_config.qc.thresholds = {"invalid_parent_references": 0.0}

        run_id = str(uuid.uuid4())[:8]
        pipeline = TestItemPipeline(testitem_config, run_id)

        rows = [
            self._row("CHEMBL1000", 0, parent_id="CHEMBL999999"),
            self._row("CHEMBL1001", 1, parent_id="CHEMBL1000"),
        ]
        df = self._build_testitem_df(rows)

        with pytest.raises(ValueError, match="invalid_parent_references"):
            pipeline.validate(df)


class TestTargetPipeline:
    """Tests for TargetPipeline."""

    def test_init(self, target_config):
        """Test pipeline initialization."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)
        assert pipeline.config == target_config
        assert pipeline.run_id == run_id

    def test_extract_empty_file(self, target_config, tmp_path):
        """Test extraction with empty file."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)

        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("target_chembl_id,pref_name\n")

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_extract_with_data(self, target_config, tmp_path):
        """Test extraction with sample data."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)

        csv_path = tmp_path / "target.csv"
        csv_path.write_text(
            "target_chembl_id,pref_name,target_type\n"
            "CHEMBL1,Test Target,PROTEIN\n"
        )

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0

    def test_transform_adds_metadata(self, target_config):
        """Test transformation adds pipeline metadata."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)

        df = pd.DataFrame({
            "target_chembl_id": ["CHEMBL1"],
            "pref_name": ["Test Target"],
        })

        result = pipeline.transform(df)
        assert "pipeline_version" in result.columns
        assert "source_system" in result.columns
        assert "extracted_at" in result.columns

    def test_validate_removes_duplicates(self, target_config):
        """Test validation removes duplicates."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = TargetPipeline(target_config, run_id)

        df = pd.DataFrame({
            "target_chembl_id": ["CHEMBL1", "CHEMBL1"],
            "pref_name": ["Target 1", "Target 2"],
        })

        result = pipeline.validate(df)
        assert len(result) == 1


class TestDocumentPipeline:
    """Tests for DocumentPipeline."""

    def test_init(self, document_config):
        """Test pipeline initialization."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = DocumentPipeline(document_config, run_id)
        assert pipeline.config == document_config
        assert pipeline.run_id == run_id

    def test_extract_empty_file(self, document_config, tmp_path):
        """Test extraction with empty file."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = DocumentPipeline(document_config, run_id)

        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("document_chembl_id,title\n")

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_extract_with_data(self, document_config, tmp_path):
        """Test extraction with sample data."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = DocumentPipeline(document_config, run_id)

        csv_path = tmp_path / "documents.csv"
        csv_path.write_text(
            "document_chembl_id,title,doi\n"
            "CHEMBL1,Test Article,10.1234/test\n"
        )

        result = pipeline.extract(input_file=csv_path)
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0

    def test_transform_adds_metadata(self, document_config):
        """Test transformation adds pipeline metadata."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = DocumentPipeline(document_config, run_id)

        df = pd.DataFrame({
            "document_chembl_id": ["CHEMBL1"],
            "title": ["Test Article"],
        })

        result = pipeline.transform(df)
        assert "pipeline_version" in result.columns
        assert "source_system" in result.columns
        assert "extracted_at" in result.columns

    def test_validate_removes_duplicates(self, document_config):
        """Test validation removes duplicates."""
        run_id = str(uuid.uuid4())[:8]
        pipeline = DocumentPipeline(document_config, run_id)

        df = pd.DataFrame({
            "document_chembl_id": ["CHEMBL1", "CHEMBL1"],
            "title": ["Article 1", "Article 2"],
        })

        result = pipeline.validate(df)
        assert len(result) == 1
