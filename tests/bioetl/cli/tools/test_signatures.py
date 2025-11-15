"""Tests for signature extraction helpers used by CLI tooling."""

from __future__ import annotations

import pytest

from bioetl.cli.tool_specs import TOOL_COMMAND_SPECS
from bioetl.cli.tools._logic.cli_catalog_code_symbols import (
    extract_pipeline_base_signatures,
)
from bioetl.cli.tools._logic.cli_semantic_diff import (
    extract_pipeline_base_from_docs,
    extract_pipeline_base_methods,
)
from bioetl.cli.tools._logic.signatures import (
    signature_from_callable,
    signature_from_docs,
)


def _parameter_fingerprint(signature_payload: dict[str, object]) -> list[tuple[str, str, str | None]]:
    parameters = signature_payload.get("parameters", [])
    assert isinstance(parameters, list)
    fingerprint: list[tuple[str, str, str | None]] = []
    for param in parameters:
        assert isinstance(param, dict)
        fingerprint.append(
            (
                str(param.get("name")),
                str(param.get("kind")),
                param.get("annotation") if isinstance(param.get("annotation"), str) else None,
            )
        )
    return fingerprint


def test_signature_from_callable_roundtrip() -> None:
    """A callable signature should capture defaults, annotations and varargs."""

    def sample(
        a: int,
        b: str = "x",
        *values: float,
        flag: bool = False,
        **options: object,
    ) -> list[int]:
        return [a]

    signature = signature_from_callable(sample, empty_annotation="Any")
    assert signature["name"] == "sample"
    assert signature["return_annotation"] == "list[int]"
    params = signature["parameters"]
    assert params[0]["annotation"] == "int"
    assert params[1]["default"] == "x"
    assert params[2]["kind"] == "VAR_POSITIONAL"
    assert params[3]["kind"] == "KEYWORD_ONLY"
    assert params[4]["kind"] == "VAR_KEYWORD"


@pytest.mark.parametrize(
    "definition",
    [
        "def sample(a: int, b: str = 'x', *values: float, flag: bool = False, **options: object) -> list[int]:",
        """
def sample(
    a: int,
    b: str = 'x',
    *values: float,
    flag: bool = False,
    **options: object,
) -> list[int]:
""",
    ],
)
def test_signature_from_docs_matches_callable(definition: str) -> None:
    """Parsing textual documentation should align with runtime signatures."""

    def sample(
        a: int,
        b: str = "x",
        *values: float,
        flag: bool = False,
        **options: object,
    ) -> list[int]:
        return [a]

    doc_signature = signature_from_docs(definition, empty_annotation="Any")
    code_signature = signature_from_callable(sample, empty_annotation="Any")

    assert doc_signature["name"] == code_signature["name"]
    assert doc_signature["return_annotation"] == code_signature["return_annotation"]
    assert _parameter_fingerprint(doc_signature) == _parameter_fingerprint(code_signature)


def test_pipeline_base_signatures_consistent_with_docs() -> None:
    """Ensure PipelineBase documentation remains aligned with code signatures."""

    code_signatures = extract_pipeline_base_signatures()
    code_subset = extract_pipeline_base_methods()
    doc_signatures = extract_pipeline_base_from_docs()

    assert isinstance(doc_signatures, dict)
    for method_name, doc_signature in doc_signatures.items():
        if not isinstance(doc_signature, dict):
            pytest.skip(f"Documentation extraction failed for {method_name}: {doc_signature}")
        if method_name not in code_subset:
            continue
        runtime_signature = code_signatures.get(method_name)
        assert runtime_signature is not None, f"Missing runtime signature for {method_name}"

        assert doc_signature["return_annotation"] == runtime_signature["return_annotation"]

        doc_params = _parameter_fingerprint(doc_signature)
        runtime_params = _parameter_fingerprint(runtime_signature)
        assert len(doc_params) <= len(runtime_params)
        runtime_lookup = {param[0]: param for param in runtime_params}
        for doc_param in doc_params:
            name = doc_param[0]
            runtime_param = runtime_lookup.get(name)
            assert runtime_param is not None, f"Missing runtime parameter: {name}"
            doc_annotation = doc_param[2]
            runtime_annotation = runtime_param[2]
            if doc_annotation is None or doc_annotation == "Any":
                continue
            assert doc_annotation == runtime_annotation


def test_semantic_diff_tool_is_declared() -> None:
    assert any(spec.code == "semantic_diff" for spec in TOOL_COMMAND_SPECS)
