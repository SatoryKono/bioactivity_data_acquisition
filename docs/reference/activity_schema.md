## Activity schemas

### RawActivitySchema

- source: str (required)
- retrieved_at: datetime (required)
- target_pref_name: str
- standard_value: float
- standard_units: str
- canonical_smiles: str
- activity_id: int
- assay_chembl_id: str
- document_chembl_id: str
- standard_type: str
- standard_relation: str
- target_chembl_id: str
- target_organism: str
- target_tax_id: str

Строгий режим (strict=True): лишние колонки запрещены.

### NormalizedActivitySchema

- source: str (required)
- retrieved_at: datetime (required)
- target: str
- activity_value: float (нормализовано в nM)
- activity_unit: str ("nM")
- smiles: str
