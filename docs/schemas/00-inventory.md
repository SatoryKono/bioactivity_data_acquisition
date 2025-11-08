# Инвентарь валидаторов и нормализаторов

| Function | Kind | Modules | Notes |
| --- | --- | --- | --- |
| `bioetl.core.validators._is_json_mapping_internal` | validator | n/a | tags=dict-items |
| `bioetl.core.validators._is_json_value` | validator | n/a | n/a |
| `bioetl.core.validators.assert_iterable` | validator | n/a | tags=iterable-check |
| `bioetl.core.validators.assert_json_mapping` | validator | n/a | n/a |
| `bioetl.core.validators.assert_list_of` | validator | bioetl.etl.vocab_store | n/a |
| `bioetl.core.validators.is_iterable` | validator | bioetl.config.loader | tags=iterable-check |
| `bioetl.core.validators.is_json_mapping` | validator | n/a | n/a |
| `bioetl.core.validators.is_list_of` | validator | n/a | n/a |
| `bioetl.schemas.activity.activity_chembl._is_valid_activity_properties` | validator | n/a | tags=json-validation |
| `bioetl.schemas.activity.activity_chembl._is_valid_activity_property_item` | validator | n/a | tags=dict-keys |
| `bioetl.schemas.base.create_schema` | utility | bioetl.schemas.activity.activity_chembl, bioetl.schemas.assay.assay_chembl, bioetl.schemas.document.document_chembl, bioetl.schemas.load_meta, bioetl.schemas.target.target_chembl, bioetl.schemas.testitem.testitem_chembl | n/a |
| `bioetl.schemas.common._build_string_column` | utility | n/a | n/a |
| `bioetl.schemas.common.bao_id_column` | utility | bioetl.schemas.activity.activity_chembl | n/a |
| `bioetl.schemas.common.boolean_flag_column` | utility | bioetl.schemas.activity.activity_chembl, bioetl.schemas.activity.enrichment | n/a |
| `bioetl.schemas.common.chembl_id_column` | utility | bioetl.schemas.activity.activity_chembl, bioetl.schemas.assay.assay_chembl, bioetl.schemas.document.document_chembl, bioetl.schemas.target.target_chembl, bioetl.schemas.testitem.testitem_chembl | n/a |
| `bioetl.schemas.common.doi_column` | utility | bioetl.schemas.document.document_chembl | n/a |
| `bioetl.schemas.common.non_nullable_int64_column` | utility | bioetl.schemas.activity.activity_chembl | n/a |
| `bioetl.schemas.common.non_nullable_string_column` | utility | bioetl.schemas.testitem.testitem_chembl | n/a |
| `bioetl.schemas.common.nullable_float64_column` | utility | bioetl.schemas.activity.activity_chembl, bioetl.schemas.testitem.testitem_chembl | n/a |
| `bioetl.schemas.common.nullable_int64_column` | utility | bioetl.schemas.activity.activity_chembl, bioetl.schemas.activity.enrichment, bioetl.schemas.assay.assay_chembl, bioetl.schemas.document.document_chembl | n/a |
| `bioetl.schemas.common.nullable_object_column` | utility | bioetl.schemas.activity.activity_chembl | n/a |
| `bioetl.schemas.common.nullable_pd_int64_column` | utility | bioetl.schemas.target.target_chembl, bioetl.schemas.testitem.testitem_chembl | n/a |
| `bioetl.schemas.common.nullable_string_column` | utility | bioetl.schemas.activity.activity_chembl, bioetl.schemas.activity.enrichment, bioetl.schemas.assay.assay_chembl, bioetl.schemas.assay.enrichment, bioetl.schemas.document.document_chembl, bioetl.schemas.document.enrichment, bioetl.schemas.target.target_chembl, bioetl.schemas.testitem.testitem_chembl | n/a |
| `bioetl.schemas.common.object_column` | utility | n/a | n/a |
| `bioetl.schemas.common.row_metadata_columns` | utility | bioetl.schemas.activity.activity_chembl, bioetl.schemas.assay.assay_chembl | n/a |
| `bioetl.schemas.common.string_column` | utility | n/a | n/a |
| `bioetl.schemas.common.string_column_with_check` | utility | bioetl.schemas.activity.activity_chembl, bioetl.schemas.assay.assay_chembl, bioetl.schemas.document.document_chembl, bioetl.schemas.load_meta, bioetl.schemas.target.target_chembl, bioetl.schemas.testitem.testitem_chembl | n/a |
| `bioetl.schemas.common.uuid_column` | utility | bioetl.schemas.activity.activity_chembl, bioetl.schemas.assay.assay_chembl, bioetl.schemas.document.document_chembl, bioetl.schemas.load_meta, bioetl.schemas.target.target_chembl, bioetl.schemas.testitem.testitem_chembl | n/a |
| `bioetl.schemas.load_meta._coerce_timestamp` | normalizer | n/a | n/a |
| `bioetl.schemas.load_meta._is_valid_json_string` | validator | n/a | tags=json-validation,whitespace-normalization |
| `bioetl.schemas.load_meta._sorted_vocab_ids` | utility | n/a | tags=sorting |
| `bioetl.schemas.load_meta._time_window_consistent` | utility | n/a | n/a |
| `bioetl.schemas.load_meta._validate_json_series` | validator | n/a | n/a |
| `bioetl.schemas.load_meta._validate_optional_json_series` | validator | n/a | n/a |
| `bioetl.schemas.vocab._default_vocab_path` | utility | n/a | n/a |
| `bioetl.schemas.vocab._load_store` | utility | n/a | n/a |
| `bioetl.schemas.vocab._resolve_vocab_path` | utility | n/a | n/a |
| `bioetl.schemas.vocab.refresh_vocab_store_cache` | utility | n/a | n/a |
| `bioetl.schemas.vocab.required_vocab_ids` | utility | bioetl.schemas.activity.activity_chembl, bioetl.schemas.assay.assay_chembl, bioetl.schemas.load_meta, bioetl.schemas.target.target_chembl | n/a |
| `bioetl.schemas.vocab.vocab_ids` | utility | n/a | n/a |
| `bioetl.schemas.vocab.vocab_store` | utility | n/a | n/a |
