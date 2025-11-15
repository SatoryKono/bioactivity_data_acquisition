from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from bioetl.tools import generate_pipeline_diff_report as gpdr


def _build_module_content(prefix: str, operator: str) -> str:
    header = """
    \"\"\"Synthetic pipeline module used for diff testing.\"\"\"
    import logging
    from pathlib import Path as _Path


    LOGGER = logging.getLogger(__name__)
    CONFIG = {"flag": True}


    def helper(value: int) -> int:
        LOGGER.info("helper", value=value)
        if value < 0:
            raise ValueError("negative")
        dummy_path = _Path("dummy.txt")
        _ = dummy_path.read_text() if dummy_path.exists() else ""
        return value


    class Worker:
        def execute(self, payload: dict[str, int]) -> int:
            LOGGER.debug("execute", payload=payload)
            return helper(payload["value"])


    RESULT = helper(1)
    """

    functions: list[str] = []
    for index in range(25):
        functions.append(
            textwrap.dedent(
                f"""
                def {prefix}_feature_{index}(value: int) -> int:
                    LOGGER.warning("feature", index={index})
                    return value {operator} {index}
                """
            ).strip()
        )
    trailer = """
    if __name__ == "__main__":
        raise SystemExit(0)
    """
    blocks = [textwrap.dedent(header).strip(), *functions, textwrap.dedent(trailer).strip()]
    return "\n\n".join(blocks) + "\n"


def _prepare_pipeline_layout(root: Path) -> Path:
    chembl_root = root / "src" / "bioetl" / "pipelines" / "chembl"
    (chembl_root / "alpha").mkdir(parents=True, exist_ok=True)
    (chembl_root / "beta").mkdir(parents=True, exist_ok=True)
    for entity in ("alpha", "beta"):
        (chembl_root / entity / "__init__.py").write_text("", encoding="utf-8")
    (chembl_root / "alpha" / "run.py").write_text(
        _build_module_content("alpha", "+"), encoding="utf-8"
    )
    (chembl_root / "beta" / "run.py").write_text(
        _build_module_content("beta", "-"), encoding="utf-8"
    )
    return chembl_root


def test_generate_report_and_diff_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pipeline_root = _prepare_pipeline_layout(tmp_path)

    monkeypatch.setattr(gpdr, "PIPELINE_ENTITIES", ("alpha", "beta"))
    monkeypatch.setattr(gpdr, "MODULE_PRIORITY", ("run.py",))

    alpha_analysis = gpdr.analyze_pipeline(pipeline_root, "alpha")
    beta_analysis = gpdr.analyze_pipeline(pipeline_root, "beta")

    # ensure module normalization works and retains trailing newline
    assert alpha_analysis.modules["run.py"].normalized_source.endswith("\n")

    diff_entries = gpdr.compare_pipelines(alpha_analysis, beta_analysis)
    assert "run.py" in diff_entries
    assert len(diff_entries["run.py"]) >= 20

    rows = gpdr.build_ast_table(alpha_analysis, beta_analysis, "run.py")
    table_text = gpdr._format_table(rows)
    assert "Definition" in table_text

    clusters = gpdr._cluster_definitions(alpha_analysis, beta_analysis, "run.py")
    assert any(item.endswith("exact AST match") for item in clusters)

    side_effect_repr = gpdr._format_side_effects_block("alpha", {"io": ["open"], "logging": []})
    assert side_effect_repr.startswith("alpha:")

    exception_repr = gpdr._format_exceptions_block("alpha", ["ValueError"])
    assert exception_repr.endswith("ValueError")

    report_path = tmp_path / "REPORT" / "LINES_DIFF.md"
    gpdr.generate_report(pipeline_root, report_path)
    content = report_path.read_text(encoding="utf-8")
    assert "Pair: alpha ↔ beta" in content
    assert "Hotspot" in content


def test_diff_blocks_create_multiple_entries(tmp_path: Path) -> None:
    module_path = tmp_path / "demo.py"
    module_path.write_text(
        textwrap.dedent(
            """
            def spam(value: int) -> int:
                return value + 1


            def eggs(value: int) -> int:
                return value - 1
            """
        ),
        encoding="utf-8",
    )

    analysis = gpdr.analyze_module("alpha", module_path)
    spam_info = analysis.definitions["spam"]
    eggs_info = analysis.definitions["eggs"]
    entries = gpdr._diff_blocks(
        "alpha",
        "beta",
        "run.py",
        spam_info,
        eggs_info,
        module_path,
        module_path,
    )
    assert entries
    assert entries[0].diff_text.startswith("--- alpha:run.py")


def test_format_blocks_handle_empty_inputs() -> None:
    assert gpdr._format_table([]) == ""
    assert gpdr._format_side_effects_block("beta", {}) == "beta: ∅"
    assert gpdr._format_exceptions_block("beta", []) == "beta: ∅"

