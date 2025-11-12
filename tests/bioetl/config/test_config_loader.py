# pyright: reportPrivateUsage=false

"""Unit tests for configuration loader."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from bioetl.config import loader as config_loader

_deep_merge = config_loader._deep_merge  # pyright: ignore[reportPrivateUsage]
_assign_nested = config_loader._assign_nested  # pyright: ignore[reportPrivateUsage]
_coerce_value = config_loader._coerce_value  # pyright: ignore[reportPrivateUsage]
_collect_env_overrides = config_loader._collect_env_overrides  # pyright: ignore[reportPrivateUsage]
_ensure_mapping = config_loader._ensure_mapping  # pyright: ignore[reportPrivateUsage]
_load_yaml = config_loader._load_yaml  # pyright: ignore[reportPrivateUsage]
_load_with_extends = config_loader._load_with_extends  # pyright: ignore[reportPrivateUsage]
_migrate_legacy_sections = config_loader._migrate_legacy_sections  # pyright: ignore[reportPrivateUsage]
_resolve_reference = config_loader._resolve_reference  # pyright: ignore[reportPrivateUsage]
_stringify_profile = config_loader._stringify_profile  # pyright: ignore[reportPrivateUsage]
load_config = config_loader.load_config


@pytest.mark.unit
class TestConfigLoader:
    """Test suite for configuration loader."""

    def test_deep_merge_simple(self) -> None:
        """Test deep merge with simple dictionaries."""
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        result = _deep_merge(base, override)

        assert result == {"a": 1, "b": 3, "c": 4}
        assert base == {"a": 1, "b": 2}  # Base should not be modified

    def test_deep_merge_nested(self) -> None:
        """Test deep merge with nested dictionaries."""
        base = {"a": {"x": 1, "y": 2}, "b": 3}
        override = {"a": {"y": 20, "z": 30}, "b": 4}
        result = _deep_merge(base, override)

        assert result == {"a": {"x": 1, "y": 20, "z": 30}, "b": 4}

    def test_deep_merge_override_with_non_dict(self) -> None:
        """Test deep merge when override replaces dict with non-dict."""
        base = {"a": {"x": 1}, "b": 2}
        override = {"a": "string", "b": 3}
        result = _deep_merge(base, override)

        assert result == {"a": "string", "b": 3}

    def test_assign_nested_simple(self) -> None:
        """Test assigning nested values."""
        target: dict[str, Any] = {}
        _assign_nested(target, ["a", "b", "c"], "value")

        assert target == {"a": {"b": {"c": "value"}}}

    def test_assign_nested_existing(self) -> None:
        """Test assigning nested values to existing structure."""
        target = {"a": {"b": {"x": 1}}}
        _assign_nested(target, ["a", "b", "c"], "value")

        assert target == {"a": {"b": {"x": 1, "c": "value"}}}

    def test_assign_nested_overwrites_dict(self) -> None:
        """Test assigning nested values overwrites existing dict."""
        target = {"a": {"b": {"x": 1}}}
        _assign_nested(target, ["a", "b"], "value")

        assert target == {"a": {"b": "value"}}

    def test_coerce_value_string_yaml(self) -> None:
        """Test coercing YAML string values."""
        assert _coerce_value("123") == 123
        assert _coerce_value("true") is True
        assert _coerce_value("false") is False
        assert _coerce_value("null") is None
        assert _coerce_value("[1, 2, 3]") == [1, 2, 3]
        assert _coerce_value('{"key": "value"}') == {"key": "value"}

    def test_coerce_value_string_plain(self) -> None:
        """Test coercing plain string values."""
        assert _coerce_value("plain text") == "plain text"
        assert _coerce_value("not yaml: {") == "not yaml: {"

    def test_coerce_value_non_string(self) -> None:
        """Test coercing non-string values."""
        assert _coerce_value(123) == 123
        assert _coerce_value([1, 2, 3]) == [1, 2, 3]
        assert _coerce_value({"key": "value"}) == {"key": "value"}

    def test_collect_env_overrides_basic(self) -> None:
        """Test collecting environment overrides."""
        env: dict[str, str] = {
            "BIOETL__HTTP__DEFAULT__TIMEOUT_SEC": "30.0",
            "BIOETL__PIPELINE__NAME": "test",
            "OTHER_VAR": "ignored",
        }
        result = _collect_env_overrides(env, prefixes=("BIOETL__",))

        assert result == {
            "http": {"default": {"timeout_sec": 30.0}},
            "pipeline": {"name": "test"},
        }

    def test_collect_env_overrides_multiple_prefixes(self) -> None:
        """Test collecting environment overrides with multiple prefixes."""
        env: dict[str, str] = {
            "BIOETL__KEY1": "value1",
            "BIOACTIVITY__KEY2": "value2",
        }
        result = _collect_env_overrides(env, prefixes=("BIOETL__", "BIOACTIVITY__"))

        assert "key1" in result
        assert "key2" in result

    def test_collect_env_overrides_empty_prefix(self) -> None:
        """Test collecting environment overrides with empty prefix."""
        env: dict[str, str] = {"BIOETL__": "value"}
        result = _collect_env_overrides(env, prefixes=("BIOETL__",))

        assert result == {}

    def test_collect_env_overrides_nested(self) -> None:
        """Test collecting nested environment overrides."""
        env: dict[str, str] = {
            "BIOETL__A__B__C": "value",
        }
        result = _collect_env_overrides(env, prefixes=("BIOETL__",))

        assert result == {"a": {"b": {"c": "value"}}}

    def test_ensure_mapping_valid(self) -> None:
        """Test ensuring value is a mapping."""
        value = {"key": "value"}
        result = _ensure_mapping(value, Path("test.yaml"))

        assert result == {"key": "value"}
        assert isinstance(result, dict)

    def test_ensure_mapping_invalid(self) -> None:
        """Test ensuring non-mapping value raises error."""
        with pytest.raises(TypeError, match="must produce a mapping"):
            _ensure_mapping("not a dict", Path("test.yaml"))

    def test_stringify_profile_relative(self, tmp_path: Path) -> None:
        """Test stringifying profile path relative to base."""
        base = tmp_path / "configs"
        base.mkdir()
        profile = base / "profiles" / "base.yaml"
        profile.parent.mkdir()
        profile.touch()

        result = _stringify_profile(profile, base=base)

        # Handle Windows path separators
        assert result.replace("\\", "/") == "profiles/base.yaml"

    def test_stringify_profile_absolute(self, tmp_path: Path) -> None:
        """Test stringifying absolute profile path."""
        base = tmp_path / "configs"
        base.mkdir()
        profile = Path("/absolute/path/to/profile.yaml")

        result = _stringify_profile(profile, base=base)

        assert result == str(profile)

    def test_load_yaml_basic(self, tmp_path: Path) -> None:
        """Test loading basic YAML file."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("key: value\nnumber: 123\n")

        result = _load_yaml(yaml_file)

        assert result == {"key": "value", "number": 123}

    def test_load_yaml_empty(self, tmp_path: Path) -> None:
        """Test loading empty YAML file."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("")

        result = _load_yaml(yaml_file)

        assert result == {}

    def test_load_yaml_with_include(self, tmp_path: Path) -> None:
        """Test loading YAML file with !include directive."""
        included_file = tmp_path / "included.yaml"
        included_file.write_text("included_key: included_value\n")

        main_file = tmp_path / "main.yaml"
        main_file.write_text(f"key: !include {included_file.name}\n")

        result = _load_yaml(main_file)

        assert "key" in result
        assert result["key"] == {"included_key": "included_value"}

    def test_resolve_reference_absolute(self, tmp_path: Path) -> None:
        """Test resolving absolute path reference."""
        absolute_path = tmp_path / "config.yaml"
        absolute_path.touch()

        result = _resolve_reference(absolute_path, base=tmp_path)

        assert result == absolute_path.resolve()

    def test_resolve_reference_relative(self, tmp_path: Path) -> None:
        """Test resolving relative path reference."""
        base = tmp_path / "configs"
        base.mkdir()
        config_file = base / "config.yaml"
        config_file.touch()

        result = _resolve_reference("config.yaml", base=base)

        assert result == config_file.resolve()

    def test_resolve_reference_not_found(self, tmp_path: Path) -> None:
        """Test resolving non-existent reference raises error."""
        base = tmp_path / "configs"
        base.mkdir()

        with pytest.raises(FileNotFoundError, match="not found"):
            _resolve_reference("nonexistent.yaml", base=base)

    def test_load_with_extends_simple(self, tmp_path: Path) -> None:
        """Test loading YAML with extends."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("key: value\nextends: []\n")

        result = _load_with_extends(config_file, stack=())

        assert result == {"key": "value"}

    def test_load_with_extends_circular(self, tmp_path: Path) -> None:
        """Test loading YAML with circular extends raises error."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(f"extends: {config_file.name}\n")

        with pytest.raises(ValueError, match="Circular extends"):
            _load_with_extends(config_file, stack=())

    def test_load_config_basic(self, tmp_path: Path) -> None:
        """Test loading basic configuration."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            """
version: 1
pipeline:
  name: test_pipeline
  version: "1.0.0"
http:
  default:
    timeout_sec: 30.0
    connect_timeout_sec: 10.0
    read_timeout_sec: 30.0
"""
        )

        config = load_config(config_file, include_default_profiles=False)

        assert config.pipeline.name == "test_pipeline"
        assert config.pipeline.version == "1.0.0"

    def test_load_config_with_cli_overrides(self, tmp_path: Path) -> None:
        """Test loading configuration with CLI overrides."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            """
version: 1
pipeline:
  name: test_pipeline
  version: "1.0.0"
http:
  default:
    timeout_sec: 30.0
"""
        )

        config = load_config(
            config_file,
            cli_overrides={"pipeline.name": "overridden"},
            include_default_profiles=False,
        )

        assert config.pipeline.name == "overridden"

    def test_load_config_not_found(self) -> None:
        """Test loading non-existent configuration raises error."""
        with pytest.raises(FileNotFoundError, match="not found"):
            load_config(Path("nonexistent.yaml"), include_default_profiles=False)

    def test_load_config_with_merge_key(self, tmp_path: Path) -> None:
        """Ensure YAML merge key ``<<`` is honoured during validation."""

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            """
<<: &base
  version: 1
  pipeline:
    name: base_pipeline
    version: "1.0.0"
  http:
    default:
      timeout_sec: 10.0
pipeline:
  name: merged_pipeline
http:
  default:
    timeout_sec: 25.0
"""
        )

        config = load_config(config_file, include_default_profiles=False)

        assert config.pipeline.name == "merged_pipeline"
        assert config.http.default.timeout_sec == 25.0

    def test_load_config_with_include_and_merge(self, tmp_path: Path) -> None:
        """YAML ``!include`` combined with merge key should produce full mapping."""

        base_profile = tmp_path / "base.yaml"
        base_profile.write_text(
            """
version: 1
pipeline:
  name: base_pipeline
  version: "1.0.0"
http:
  default:
    timeout_sec: 30.0
"""
        )

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            f"""
<<: !include {base_profile.name}
pipeline:
  name: override_pipeline
"""
        )

        config = load_config(config_file, include_default_profiles=False)

        assert config.pipeline.name == "override_pipeline"
        assert config.http.default.timeout_sec == 30.0

    def test_migrate_legacy_clients_section(self) -> None:
        """Ensure legacy `clients.chembl.status_endpoint` is migrated into `chembl`."""

        payload: dict[str, Any] = {
            "version": 1,
            "pipeline": {
                "name": "test",
                "version": "1.0.0",
            },
            "http": {
                "default": {
                    "timeout_sec": 30.0,
                    "connect_timeout_sec": 10.0,
                    "read_timeout_sec": 30.0,
                },
            },
            "clients": {
                "chembl": {
                    "status_endpoint": "/legacy-status.json",
                },
            },
        }

        migrated = _migrate_legacy_sections(payload)

        assert "clients" not in migrated
        chembl_section = migrated.get("chembl")
        assert isinstance(chembl_section, dict)
        assert chembl_section["status_endpoint"] == "/legacy-status.json"

    def test_migrate_legacy_clients_section_preserves_existing(self) -> None:
        """Existing `chembl.status_endpoint` must not be overwritten by legacy data."""

        payload: dict[str, Any] = {
            "chembl": {
                "status_endpoint": "/modern.json",
            },
            "clients": {
                "chembl": {
                    "status_endpoint": "/legacy.json",
                },
            },
        }

        migrated = _migrate_legacy_sections(payload)

        assert "clients" not in migrated
        chembl_section = migrated.get("chembl")
        assert isinstance(chembl_section, dict)
        assert chembl_section["status_endpoint"] == "/modern.json"
