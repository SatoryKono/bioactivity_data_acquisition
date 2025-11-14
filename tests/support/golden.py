"""Helpers for working with committed golden artifacts."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Mapping

import yaml


def load_yaml_dict(path: Path) -> dict[str, Any]:
    """Return YAML payload as a dict with UTF-8 decoding."""

    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    assert isinstance(payload, dict), f"Expected mapping in {path}"
    return payload


def load_json_dict(path: Path) -> dict[str, Any]:
    """Return JSON payload as a dict with UTF-8 decoding."""

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert isinstance(payload, dict), f"Expected mapping in {path}"
    return payload


def normalize_meta_payload(meta: Mapping[str, Any]) -> dict[str, Any]:
    """Strip volatile ``meta.yaml`` fields for deterministic comparison."""

    normalized = copy.deepcopy(meta)
    dataset_path = normalized.get("dataset_path")
    if dataset_path:
        normalized["dataset_path"] = Path(str(dataset_path)).name
    normalized.pop("generated_at_utc", None)
    normalized.pop("run_id", None)
    normalized["stage_durations_ms"] = {}
    return normalized


def normalize_manifest_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Strip volatile manifest fields and sort artefacts."""

    normalized = copy.deepcopy(payload)
    for key in ("generated_at_utc", "run_directory", "run_id"):
        normalized.pop(key, None)

    artifacts = normalized.get("artifacts")
    if isinstance(artifacts, list):
        normalized["artifacts"] = sorted(
            artifacts,
            key=lambda item: (item.get("name", ""), item.get("path", "")),
        )
    return normalized


def canonical_json(payload: Mapping[str, Any]) -> str:
    """Serialize payload to canonical JSON string."""

    return json.dumps(payload, sort_keys=True, separators=(",", ":"))

