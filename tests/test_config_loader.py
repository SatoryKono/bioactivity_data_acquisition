from __future__ import annotations

from pathlib import Path

from library.config_loader import AppConfig, RuntimeOverrides, build_runtime


def test_build_runtime_uses_config_defaults(tmp_path: Path) -> None:
    config = AppConfig.from_path(Path("configs/pipelines.toml"))
    overrides = RuntimeOverrides(output_dir=tmp_path, dry_run=False)

    runtime = build_runtime(config, "activity", overrides)

    assert runtime.name == "activity"
    assert runtime.limit == 1000  # default from config
    assert runtime.output_dir == tmp_path
    assert runtime.postprocess is True
    assert runtime.parameters["quality_threshold"] == 0.9


def test_override_limit_and_postprocess(tmp_path: Path) -> None:
    config = AppConfig.from_path(Path("configs/pipelines.toml"))
    overrides = RuntimeOverrides(limit=25, postprocess=False, output_dir=tmp_path)

    runtime = build_runtime(config, "document", overrides)

    assert runtime.limit == 25
    assert runtime.postprocess is False
    assert runtime.parameters["mode"] == "all"
