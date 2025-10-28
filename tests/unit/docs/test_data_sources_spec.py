from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping

import pytest

from bioetl.config.loader import load_config

SPEC_PATH = Path("99-data-sources-and-data-spec.md")

ENTITY_NAME_MAPPING: Mapping[str, str] = {
    "document": "documents",
    "target": "targets",
    "assay": "assays",
    "testitem": "testitems",
    "activity": "activities",
}

SOURCE_LABELS: Mapping[str, tuple[str, ...]] = {
    "chembl": ("ChEMBL",),
    "pubmed": ("PubMed",),
    "crossref": ("Crossref",),
    "openalex": ("OpenAlex",),
    "semantic_scholar": ("Semantic Scholar",),
    "uniprot": ("UniProt",),
    "uniprot_idmapping": ("UniProt",),
    "iuphar": ("IUPHAR",),
    "pubchem": ("PubChem",),
}


def parse_sources_table() -> dict[str, dict[str, str]]:
    """Extract the entity → sources table from the markdown spec."""

    text = SPEC_PATH.read_text(encoding="utf-8")
    lines = text.splitlines()

    table_rows: dict[str, dict[str, str]] = {}
    duplicates: list[str] = []
    in_table = False

    for line in lines:
        if line.startswith("| Сущность"):
            in_table = True
            continue
        if not in_table:
            continue
        if not line.startswith("|"):
            break
        if line.startswith("| ---"):
            continue

        columns = [column.strip() for column in line.strip("|").split("|")]
        if len(columns) < 3:
            continue

        entity_cell = columns[0].strip()
        entity = entity_cell.strip("`").lower()

        if entity in table_rows:
            duplicates.append(entity)

        table_rows[entity] = {
            "primary": columns[1].strip(),
            "enrichment": columns[2].strip(),
        }

    if duplicates:
        pytest.fail(
            "Duplicate entity rows detected in data source specification: "
            + ", ".join(sorted(duplicates))
        )

    return table_rows


def classify_sources(config) -> tuple[set[str], set[str]]:
    """Split configured sources into primary vs enrichment sets."""

    primary_keys: set[str] = set()
    enrichment_keys: set[str] = set()

    for key, source in config.sources.items():
        if not getattr(source, "enabled", True):
            continue

        stage = getattr(source, "stage", None)
        fields_set = getattr(source, "model_fields_set", set())

        if key != "chembl" and "stage" not in fields_set:
            stage = "enrichment"
        elif stage:
            stage = stage.lower()
        else:
            stage = "primary" if key == "chembl" else "enrichment"

        if stage == "primary":
            primary_keys.add(key)
        else:
            enrichment_keys.add(key)

    return primary_keys, enrichment_keys


def labels_for_sources(source_keys: Iterable[str]) -> set[str]:
    labels: set[str] = set()
    for key in source_keys:
        if key not in SOURCE_LABELS:
            pytest.fail(f"Missing human-readable label mapping for source '{key}'")
        labels.update(SOURCE_LABELS[key])
    return labels


def labels_present(text: str, expected: set[str]) -> set[str]:
    text_lower = text.lower()
    return {label for label in expected if label.lower() in text_lower}


def test_pipeline_sources_documented():
    spec_rows = parse_sources_table()

    pipeline_paths = sorted(Path("configs/pipelines").glob("*.yaml"))
    assert pipeline_paths, "No pipeline configurations discovered"

    pipeline_configs = [(path, load_config(path)) for path in pipeline_paths]

    for pipeline_path, config in pipeline_configs:
        pipeline_entity = config.pipeline.entity
        assert (
            pipeline_entity in ENTITY_NAME_MAPPING
        ), f"Add entity mapping for '{pipeline_entity}'"

        spec_entity = ENTITY_NAME_MAPPING[pipeline_entity]
        assert (
            spec_entity in spec_rows
        ), f"Entity '{spec_entity}' from {pipeline_path} missing in spec table"

        row = spec_rows[spec_entity]
        primary_keys, enrichment_keys = classify_sources(config)

        primary_labels = labels_for_sources(primary_keys)
        enrichment_labels = labels_for_sources(enrichment_keys)

        if primary_labels:
            found_primary = labels_present(row["primary"], primary_labels)
            message = "\n".join(
                [
                    "Mismatch between configured primary sources and documented ones",
                    f"Entity: {spec_entity}",
                    f"Pipeline: {pipeline_path}",
                    f"Documented primary column: {row['primary']}",
                    f"Configured primary labels: {sorted(primary_labels)}",
                    f"Found labels: {sorted(found_primary)}",
                ]
            )
            assert found_primary == primary_labels, message

        if enrichment_labels:
            found_enrichment = labels_present(row["enrichment"], enrichment_labels)
            message = "\n".join(
                [
                    "Mismatch between configured enrichment sources and documented ones",
                    f"Entity: {spec_entity}",
                    f"Pipeline: {pipeline_path}",
                    f"Documented enrichment column: {row['enrichment']}",
                    f"Configured enrichment labels: {sorted(enrichment_labels)}",
                    f"Found labels: {sorted(found_enrichment)}",
                ]
            )
            assert found_enrichment == enrichment_labels, message

    documented_entities = set(spec_rows)
    configured_entities = {
        ENTITY_NAME_MAPPING[config.pipeline.entity]
        for _, config in pipeline_configs
    }
    missing_entities = configured_entities - documented_entities
    assert not missing_entities, (
        "The documentation table is missing entities present in pipeline configs: "
        + ", ".join(sorted(missing_entities))
    )
