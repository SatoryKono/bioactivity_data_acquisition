"""Registry of structured logging event identifiers and helper utilities."""

from __future__ import annotations

import re
from enum import Enum, auto
from typing import Any, Final

from structlog.stdlib import BoundLogger

__all__ = ["LogEvents", "stage_event", "client_event", "emit"]

_STAGE_PART_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9_-]+$")


class LogEventString(str):
    """String wrapper providing dotted value with underscore comparison support."""

    __slots__ = ("legacy",)
    legacy: str

    def __new__(cls, dotted: str) -> "LogEventString":
        """Create a string preserving both dotted and underscore legacy forms."""
        obj = super().__new__(cls, dotted)
        obj.legacy = dotted.replace(".", "_")
        return obj

    def __eq__(self, other: object) -> bool:
        """Support equality comparisons against strings and legacy values."""
        if isinstance(other, str):
            return super().__eq__(other) or other == self.legacy
        return super().__eq__(other)

    def __hash__(self) -> int:
        """Use string hash semantics."""
        return super().__hash__()


class LogEvents(str, Enum):
    """Strongly typed registry of UnifiedLogger events."""

    @staticmethod
    def _generate_next_value_(name: str, start: int, count: int, _last_values: list[str]) -> str:
        """Produce a dotted event identifier based on enum member naming."""
        parts = name.lower().split("_")
        namespace = parts[0] if parts else "event"
        suffix = parts[-1] if len(parts) > 1 else "event"
        action_parts = parts[1:-1] if len(parts) > 2 else []
        if not action_parts:
            action_parts = ["event"]
        action = ".".join(action_parts)
        dotted = ".".join((namespace, action, suffix))
        return LogEventString(dotted)

    def __eq__(self, other: object) -> bool:
        """Compare enum values against strings and legacy identifiers."""
        if isinstance(other, str):
            value = self.value
            if isinstance(value, LogEventString):
                return value == other
            return str(value) == other
        return super().__eq__(other)

    def __hash__(self) -> int:
        """Hash the enum value, respecting wrapped ``LogEventString``."""
        return hash(self.value)

    def legacy(self) -> str:
        """Return underscore-delimited identifier for compatibility tests."""

        value = self.value
        if isinstance(value, LogEventString):
            return value.legacy
        return str(value).replace(".", "_")
    CLI_RUN_START = auto()
    CLI_RUN_FINISH = auto()
    CLI_RUN_ERROR = auto()
    CLI_COMMAND_REGISTRATION_FAILED = auto()
    CLI_DOCTEST_REPORT_WRITTEN = auto()
    CLI_EXAMPLE_RESULT = auto()
    CLI_EXAMPLES_EXTRACTED = auto()
    CLI_EXAMPLES_RUNNING = auto()
    CLI_PIPELINE_API_ERROR = auto()
    CLI_PIPELINE_CLASS_INVALID = auto()
    CLI_PIPELINE_CLASS_LOOKUP_FAILED = auto()
    CLI_PIPELINE_COMPLETED = auto()
    CLI_PIPELINE_FAILED = auto()
    CLI_PIPELINE_STARTED = auto()
    CLI_REGISTRY_LOOKUP_FAILED = auto()
    STAGE_RUN_START = auto()
    STAGE_RUN_FINISH = auto()
    STAGE_RUN_ERROR = auto()
    STAGE_EXTRACT_START = auto()
    STAGE_EXTRACT_FINISH = auto()
    STAGE_EXTRACT_ERROR = auto()
    STAGE_TRANSFORM_START = auto()
    STAGE_TRANSFORM_FINISH = auto()
    STAGE_TRANSFORM_ERROR = auto()
    STAGE_VALIDATE_START = auto()
    STAGE_VALIDATE_FINISH = auto()
    STAGE_VALIDATE_ERROR = auto()
    STAGE_WRITE_START = auto()
    STAGE_WRITE_FINISH = auto()
    STAGE_WRITE_ERROR = auto()
    STAGE_CLEANUP_START = auto()
    STAGE_CLEANUP_FINISH = auto()
    STAGE_CLEANUP_ERROR = auto()
    CLIENT_REQUEST = "client.request.sent"
    CLIENT_RETRY = "client.request.retry"
    CLIENT_RATE_LIMIT = "client.rate_limit.hit"
    CLIENT_CIRCUIT_OPEN = "client.circuit.open"
    HTTP_RATE_LIMITER_WAIT = auto()
    HTTP_REQUEST_COMPLETED = auto()
    HTTP_REQUEST_EXCEPTION = auto()
    HTTP_REQUEST_FAILED = auto()
    HTTP_REQUEST_METHOD_OVERRIDE = auto()
    HTTP_REQUEST_RETRY = auto()
    HTTP_RESOLVE_URL = auto()
    ACTIVITY_FETCH_ERROR = auto()
    ACTIVITY_NO_IDS = auto()
    ACTIVITY_NO_RECORDS_FETCHED = auto()
    ACTIVITY_ID_DUPLICATES_FOUND = auto()
    ACTIVITY_ID_UNIQUENESS_CHECK_SKIPPED = auto()
    ACTIVITY_ID_UNIQUENESS_VERIFIED = auto()
    ACTIVITY_PROPERTIES_ITEM_UNHANDLED = auto()
    ACTIVITY_PROPERTIES_MISSING = auto()
    ACTIVITY_PROPERTIES_NORMALIZATION_FAILED = auto()
    ACTIVITY_PROPERTIES_NULL = auto()
    ACTIVITY_PROPERTIES_PARSE_FAILED = auto()
    ACTIVITY_PROPERTIES_PROCESSED = auto()
    ACTIVITY_PROPERTIES_SERIALIZATION_FAILED = auto()
    ACTIVITY_PROPERTIES_UNHANDLED_TYPE = auto()
    ACTIVITY_PROPERTY_DUPLICATE_REMOVED = auto()
    ACTIVITY_PROPERTY_TRUV_VALIDATION_FAILED = auto()
    ARRAY_FIELDS_SERIALIZED = auto()
    ARTIFACTS_WRITTEN = auto()
    ASSAY_CLASS_ID_COLUMN_MISSING_AFTER_ENRICHMENT = auto()
    ASSAY_CLASS_ID_EMPTY_AFTER_ENRICHMENT = auto()
    ASSAY_CLASS_ID_ENRICHMENT_STATS = auto()
    ASSAY_CLASS_ID_EXTRACTED_FROM_CLASSIFICATIONS = auto()
    ASSAY_CLASSIFICATIONS_RESET_NON_STRING = auto()
    ASSAY_MISSING_FIELDS_IN_API_RESPONSE = auto()
    ASSAY_PARAMETERS_RESET_NON_STRING = auto()
    AUDIT_FINISHED = auto()
    AUDIT_STARTED = auto()
    BOOL_CONVERSION_FAILED = auto()
    BUILDING_WRITE_ARTIFACTS = auto()
    CATALOG_EXTRACT_DONE = auto()
    CATALOG_EXTRACT_START = auto()
    CATALOG_WRITTEN = auto()
    CHECKING_OUTPUT_ARTIFACTS = auto()
    CHEMBL_HANDSHAKE = auto()
    CHEMBL_ACTIVITY_BATCH_CIRCUIT_BREAKER = auto()
    CHEMBL_ACTIVITY_BATCH_PROCESSED = auto()
    CHEMBL_ACTIVITY_BATCH_REQUEST_ERROR = auto()
    CHEMBL_ACTIVITY_BATCH_SUMMARY = auto()
    CHEMBL_ACTIVITY_BATCH_UNHANDLED_ERROR = auto()
    CHEMBL_ACTIVITY_CACHE_STORE_FAILED = auto()
    CHEMBL_ACTIVITY_DEPRECATED_KWARGS = auto()
    CHEMBL_ACTIVITY_EXTRACT_BY_IDS_SUMMARY = auto()
    CHEMBL_ACTIVITY_INVALID_ACTIVITY_IDS = auto()
    CHEMBL_ASSAY_EXTRACT_BY_IDS_SUMMARY = auto()
    CHEMBL_ASSAY_EXTRACT_SKIPPED = auto()
    CHEMBL_ASSAY_HANDSHAKE = auto()
    CHEMBL_ASSAY_MISSING_REQUIRED_FIELD = auto()
    CHEMBL_ASSAY_SELECT_FIELDS = auto()
    CHEMBL_CLIENT_OFFLINE_STUB_ACTIVATED = auto()
    CHEMBL_CLIENT_OFFLINE_STUB_FORCED = auto()
    CHEMBL_CLIENT_INITIALIZED = auto()
    CHEMBL_DESCRIPTOR_STATUS = auto()
    CHEMBL_DESCRIPTOR_STATUS_FAILED = auto()
    CHEMBL_DOCUMENT_EXTRACT_BY_IDS_SUMMARY = auto()
    CHEMBL_TARGET_EXTRACT_BY_IDS_SUMMARY = auto()
    CHEMBL_TARGET_EXTRACT_SKIPPED = auto()
    CHEMBL_TESTITEM_EXTRACT_BY_IDS_SUMMARY = auto()
    CHEMBL_TESTITEM_EXTRACT_SKIPPED = auto()
    CHEMBL_TESTITEM_SELECT_FIELDS = auto()
    CHEMBL_TESTITEM_STATUS = auto()
    CHEMBL_TESTITEM_STATUS_FAILED = auto()
    CIRCUIT_BREAKER_BLOCKED = auto()
    CIRCUIT_BREAKER_TRANSITION = auto()
    CLIENT_CLEANUP_FAILED = auto()
    CLIENT_FACTORY_BUILD = auto()
    COMMENT_CHECK_NOT_IMPLEMENTED = auto()
    COMMENT_FIELDS_ENSURED = auto()
    COMPONENT_CLASS_FETCH_ERROR = auto()
    COMPONENT_SEQUENCE_FETCH_ERROR = auto()
    COMPOUND_RECORD_FETCH_COMPLETE = auto()
    COMPOUND_RECORD_FETCH_ERROR = auto()
    COMPOUND_RECORD_INCOMPLETE_PAGINATION = auto()
    COMPOUND_RECORD_NO_IDS_AFTER_CLEANUP = auto()
    COMPOUND_RECORD_NO_PAIRS = auto()
    CONFIG_INVALID = auto()
    CONFIG_MISSING = auto()
    CONFIG_VALID = auto()
    CURATION_LEVEL_MISSING = auto()
    DATA_VALIDITY_COMMENT_FALLBACK_APPLIED = auto()
    DATA_VALIDITY_COMMENT_FALLBACK_NO_ITEMS = auto()
    DATA_VALIDITY_COMMENT_FROM_API = auto()
    DATA_VALIDITY_COMMENT_WHITELIST_UNAVAILABLE = auto()
    DATASET_WRITTEN = auto()
    DEDUPLICATE_MOLECULES_COMPLETED = auto()
    DETERMINISM_CHECK_REPORT_WRITTEN = auto()
    DETERMINISM_CHECK_START = auto()
    DICTIONARY_MISSING = auto()
    DIRECTORY_WARNINGS = auto()
    DOC_CODE_MATRIX_WRITTEN = auto()
    DOCS_DIRECTORY_MISSING = auto()
    DOCUMENT_DEDUPLICATION_APPLIED = auto()
    DOCUMENT_ID_DUPLICATES = auto()
    DROP_EXTRA_COLUMNS_BEFORE_VALIDATION = auto()
    DUP_FINDER_FAILED = auto()
    ENRICH_PROTEIN_CLASSIFICATIONS_COMPLETE = auto()
    ENRICH_PROTEIN_CLASSIFICATIONS_SKIPPED = auto()
    ENRICH_PROTEIN_CLASSIFICATIONS_START = auto()
    ENRICH_TARGET_COMPONENTS_COMPLETE = auto()
    ENRICH_TARGET_COMPONENTS_SKIPPED = auto()
    ENRICH_TARGET_COMPONENTS_START = auto()
    ENRICHMENT_BY_PAIRS_COMPLETE = auto()
    ENRICHMENT_BY_PAIRS_NO_RECORDS_FOUND = auto()
    ENRICHMENT_BY_PAIRS_SKIPPED_NO_VALID_PAIRS = auto()
    ENRICHMENT_BY_PAIRS_SOME_PAIRS_NOT_FOUND = auto()
    ENRICHMENT_BY_RECORD_ID_NO_RECORDS_FOUND = auto()
    ENRICHMENT_BY_RECORD_ID_SKIPPED_NO_VALID_IDS = auto()
    ENRICHMENT_CLASSIFICATIONS_COMPLETE = auto()
    ENRICHMENT_CLASSIFICATIONS_COMPLETED = auto()
    ENRICHMENT_CLASSIFICATIONS_DISABLED = auto()
    ENRICHMENT_CLASSIFICATIONS_STARTED = auto()
    ENRICHMENT_COMPLETED = auto()
    ENRICHMENT_CONFIG_ERROR = auto()
    ENRICHMENT_DATA_VALIDITY_DESCRIPTION_ALREADY_FILLED = auto()
    ENRICHMENT_FETCH_ERROR_BY_PAIRS = auto()
    ENRICHMENT_FETCH_ERROR_BY_RECORD_ID = auto()
    ENRICHMENT_FETCHING_ASSAY_CLASS_MAP = auto()
    ENRICHMENT_FETCHING_ASSAY_CLASSIFICATIONS = auto()
    ENRICHMENT_FETCHING_ASSAY_PARAMETERS = auto()
    ENRICHMENT_FETCHING_ASSAYS = auto()
    ENRICHMENT_FETCHING_COMPOUND_RECORDS_BY_PAIRS = auto()
    ENRICHMENT_FETCHING_COMPOUND_RECORDS_BY_RECORD_ID = auto()
    ENRICHMENT_FETCHING_DATA_VALIDITY = auto()
    ENRICHMENT_FETCHING_TERMS = auto()
    ENRICHMENT_NO_RECORDS_FOUND = auto()
    ENRICHMENT_PARAMETERS_COMPLETE = auto()
    ENRICHMENT_PARAMETERS_COMPLETED = auto()
    ENRICHMENT_PARAMETERS_STARTED = auto()
    ENRICHMENT_SKIPPED_EMPTY_DATAFRAME = auto()
    ENRICHMENT_SKIPPED_MISSING_COLUMNS = auto()
    ENRICHMENT_SKIPPED_MISSING_SOURCE = auto()
    ENRICHMENT_SKIPPED_NO_ASSAY_CONFIG = auto()
    ENRICHMENT_SKIPPED_NO_CHEMBL_CONFIG = auto()
    ENRICHMENT_SKIPPED_NO_ENRICH_CONFIG = auto()
    ENRICHMENT_SKIPPED_NO_VALID_COMMENTS = auto()
    ENRICHMENT_SKIPPED_NO_VALID_IDS = auto()
    ENRICHMENT_STAGES_COMPLETED = auto()
    EXTRACT_ASSAY_FIELDS_COMPLETE = auto()
    EXTRACT_ASSAY_FIELDS_FETCH_ERROR = auto()
    EXTRACT_ASSAY_FIELDS_FETCHING = auto()
    EXTRACT_ASSAY_FIELDS_NO_RECORDS = auto()
    EXTRACT_ASSAY_FIELDS_SKIPPED = auto()
    EXTRACT_COMPLETED = auto()
    EXTRACT_DATA_VALIDITY_DESCRIPTIONS_COMPLETE = auto()
    EXTRACT_DATA_VALIDITY_DESCRIPTIONS_FETCH_ERROR = auto()
    EXTRACT_DATA_VALIDITY_DESCRIPTIONS_FETCHING = auto()
    EXTRACT_DATA_VALIDITY_DESCRIPTIONS_NO_RECORDS = auto()
    EXTRACT_DATA_VALIDITY_DESCRIPTIONS_SKIPPED = auto()
    EXTRACT_IDS_PAGINATED_BATCH_ERROR = auto()
    EXTRACT_IDS_PAGINATED_NO_VALID_IDS = auto()
    EXTRACT_IDS_PAGINATED_SUMMARY = auto()
    EXTRACT_STARTED = auto()
    FILTER_SKIPPED_MISSING_COLUMNS = auto()
    FILTERED_INVALID_ROWS = auto()
    FINALISING_OUTPUT = auto()
    FLATTEN_NESTED_STRUCTURES_COMPLETED = auto()
    FOREIGN_KEY_INTEGRITY_CHECK_FAILED = auto()
    FOREIGN_KEY_INTEGRITY_CHECK_SKIPPED = auto()
    FOREIGN_KEY_INTEGRITY_INVALID = auto()
    FOREIGN_KEY_INTEGRITY_VERIFIED = auto()
    FOREIGN_KEY_VALIDATION = auto()
    HIGHLY_EMPTY_COLUMNS_DETECTED = auto()
    IDENTIFIER_COLUMNS_MISSING = auto()
    IDENTIFIER_HARMONIZATION = auto()
    IDENTIFIER_MISMATCH = auto()
    IDENTIFIERS_NORMALIZED = auto()
    INPUT_FILE_EMPTY = auto()
    INPUT_FILE_EMPTY_IDS = auto()
    INPUT_FILE_MISSING_ID_COLUMN = auto()
    INPUT_FILE_NOT_FOUND = auto()
    INPUT_IDS_READ = auto()
    INPUT_LIMIT_ACTIVE = auto()
    INVALID_ASSAY_TAX_ID_RANGE = auto()
    INVALID_POSITIVE_INTEGER = auto()
    INVALID_RELATION = auto()
    INVALID_STANDARD_FLAG = auto()
    INVALID_STANDARD_RELATION = auto()
    INVALID_STANDARD_TYPE = auto()
    INVALID_TARGET_CHEMBL_ID = auto()
    INVARIANT_DATA_VALIDITY_DESCRIPTION_WITHOUT_COMMENT = auto()
    INVENTORY_COLLECTED = auto()
    INVENTORY_WRITTEN = auto()
    JOIN_COMPLETED = auto()
    JOIN_SKIPPED_EMPTY_DATAFRAME = auto()
    JOIN_SKIPPED_MISSING_COLUMNS = auto()
    JOIN_SKIPPED_NO_ACTIVITY_DATA = auto()
    LIGAND_EFFICIENCY_MISSING_WITH_STANDARD_VALUE = auto()
    LINK_AUDIT_COMPLETED = auto()
    LINK_CHECK_START = auto()
    LOAD_META_BEGIN = auto()
    LOAD_META_FINISH = auto()
    LOAD_META_PAGE = auto()
    LYCHEE_FINISHED = auto()
    LYCHEE_NOT_AVAILABLE = auto()
    LYCHEE_NOT_FOUND = auto()
    LYCHEE_TIMEOUT = auto()
    MARKDOWN_READ_FAILED = auto()
    MEASUREMENTS_NORMALIZED = auto()
    METADATA_WRITTEN = auto()
    MISSING_COLUMN_NOT_REQUESTED = auto()
    MISSING_COLUMNS_HANDLED = auto()
    MISSING_FIELD_IN_RESPONSE = auto()
    MISSING_FIELD_NOT_REQUESTED = auto()
    MISSING_OPTIONAL_COLUMN = auto()
    MOLECULE_PREF_NAME_ENRICHED = auto()
    NEGATIVE_LOWER_VALUE = auto()
    NEGATIVE_STANDARD_UPPER_VALUE = auto()
    NEGATIVE_STANDARD_VALUE = auto()
    NEGATIVE_UPPER_VALUE = auto()
    NESTED_SERIALIZATION_FAILED = auto()
    NO_INPUT_FILE = auto()
    NORMALIZE_IDENTIFIERS_COMPLETED = auto()
    NORMALIZE_NUMERIC_FIELDS_COMPLETED = auto()
    NORMALIZE_STRING_FIELDS_COMPLETED = auto()
    OUTPUT_ARTIFACT_CHECK_COMPLETED = auto()
    OUTPUT_COLUMNS_DROPPED = auto()
    OUTPUT_COLUMNS_MISSING = auto()
    PARSE_ERRORS = auto()
    PIPELINE_COMPLETED = auto()
    PIPELINE_DETERMINISTIC = auto()
    PIPELINE_FAILED = auto()
    PIPELINE_INVENTORY_COMPLETED = auto()
    PIPELINE_NOT_DETERMINISTIC = auto()
    PIPELINE_RUN_FAILED = auto()
    PIPELINE_STARTED = auto()
    PREPARING_DIRECTORIES = auto()
    PROTEIN_CLASSIFICATION_FETCH_ERROR = auto()
    PROTEIN_FAMILY_CLASSIFICATION_FETCH_ERROR = auto()
    PYTEST_FINISHED = auto()
    PYTEST_JSON_MISSING = auto()
    READING_INPUT = auto()
    REMOVE_EXTRA_COLUMNS_COMPLETED = auto()
    ROW_INDEX_ADDED = auto()
    ROW_INDEX_FILLED = auto()
    ROW_SUBTYPE_ADDED = auto()
    ROW_SUBTYPE_FILLED = auto()
    RUNNING_PIPELINE_CHECK = auto()
    RUNNING_PYTEST = auto()
    SAMPLE_APPLIED = auto()
    SAMPLE_SIZE_EXCEEDS_POPULATION = auto()
    SCAN_COMPLETE = auto()
    SCAN_START = auto()
    SCHEMA_COLUMNS_ADDED = auto()
    SCHEMA_EXTRA_COLUMNS_DROPPED = auto()
    SCHEMA_GUARD_REPORT_WRITTEN = auto()
    SCHEMA_REGISTRY_INVALID = auto()
    SCHEMA_REGISTRY_VALID = auto()
    SCHEMA_VERSION_EXPECTED = auto()
    SCHEMA_VERSION_MIGRATION_PLAN = auto()
    SCHEMA_VERSION_MIGRATION_APPLIED = auto()
    SCHEMA_VALIDATION_COMPLETED = auto()
    SCHEMA_VALIDATION_FAILED = auto()
    SEMANTIC_DIFF_EXTRACT_START = auto()
    SEMANTIC_DIFF_WRITTEN = auto()
    SOFT_ENUM_UNKNOWN_DATA_VALIDITY_COMMENT = auto()
    SPECIES_GROUP_FLAG_CONVERSION_FAILED = auto()
    STRING_FIELDS_NORMALIZED = auto()
    TARGET_COMPONENT_FETCH_ERROR = auto()
    TARGET_DIRECTORY_EXISTS = auto()
    TESTS_FAILED = auto()
    TESTS_SUCCEEDED = auto()
    TRANSFORM_COMPLETED = auto()
    TRANSFORM_EMPTY_DATAFRAME = auto()
    TRANSFORM_STARTED = auto()
    TRUV_VALIDATION_ERROR = auto()
    TRUV_VALIDATION_FAILED = auto()
    TRUV_VALIDATION_PASSED = auto()
    TRUV_VALIDATION_SKIPPED_MISSING_COLUMN = auto()
    TRUV_VALIDATION_WARNING = auto()
    TYPE_CONVERSION_FAILED = auto()
    TYPE_IGNORE_REMOVED = auto()
    UNKNOWN_DATA_VALIDITY_COMMENTS_DETECTED = auto()
    VALIDATE_COMPLETED = auto()
    VALIDATE_EMPTY_DATAFRAME = auto()
    VALIDATE_STARTED = auto()
    VALIDATING_ASSAY_PARAMETERS_TRUV = auto()
    VALIDATING_CONFIG = auto()
    VALIDATION_COERCE_ONLY_PASSTHROUGH = auto()
    VALIDATION_COMPLETED = auto()
    VALIDATION_ERROR = auto()
    VALIDATION_ERROR_DETAIL = auto()
    VALIDATION_ERRORS_TRUNCATED = auto()
    VALIDATION_FAILED = auto()
    VALIDATION_FAILURE_CASES = auto()
    VALIDATION_ISSUE_RECORDED = auto()
    VALIDATION_RETRY_WITHOUT_COERCE = auto()
    VALIDATION_SCHEMA_LOADED = auto()
    VALIDATION_SKIPPED = auto()
    VALIDITY_COMMENTS_METRICS = auto()
    VOCAB_AUDIT_COMPLETED = auto()
    VOCAB_STORE_BUILT = auto()
    WRITE_ARTIFACTS_PREPARED = auto()
    WRITE_COMPLETED = auto()
    WRITE_SORT_CONFIG_SET = auto()
    WRITE_STARTED = auto()
    WRITING_DATASET = auto()
    WRITING_META = auto()
    WRITING_METADATA = auto()
    WRITING_QC_ARTIFACT = auto()


_CLIENT_EVENT_MAP: Final[dict[str, LogEvents]] = {
    "request": LogEvents.CLIENT_REQUEST,
    "retry": LogEvents.CLIENT_RETRY,
    "rate_limit": LogEvents.CLIENT_RATE_LIMIT,
    "circuit_open": LogEvents.CLIENT_CIRCUIT_OPEN,
}


def _validate_stage_part(name: str, label: str) -> None:
    """Validate stage identifier fragment against allowed characters."""
    if not name:
        msg = f"{label} must not be empty"
        raise ValueError(msg)
    if _STAGE_PART_PATTERN.fullmatch(name) is None:
        msg = f"{label} must contain only [a-z0-9_-], got {name!r}"
        raise ValueError(msg)


def stage_event(stage: str, suffix: str) -> str:
    """Compose a stage identifier in the form ``stage.<stage>.<suffix>``."""

    _validate_stage_part(stage, "stage")
    _validate_stage_part(suffix, "suffix")
    return f"stage.{stage}.{suffix}"


def client_event(name: str) -> str:
    """Translate a human-readable name into a client event string."""

    try:
        return _CLIENT_EVENT_MAP[name].value
    except KeyError as exc:  # pragma: no cover - defensive guard
        msg = f"Unknown client event: {name!r}"
        raise ValueError(msg) from exc


def emit(logger: BoundLogger, event: str | LogEvents, **fields: Any) -> None:
    """Send an event via ``BoundLogger`` without mutating the provided fields."""

    message = event.value if isinstance(event, LogEvents) else event
    logger.info(message, **fields)

