#!/usr/bin/env python3
"""Schema-guard: сверка полей конфигов из доков с реальной поддержкой в коде."""

import json
import yaml
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).parent.parent
AUDIT_RESULTS = ROOT / "audit_results"
AUDIT_RESULTS.mkdir(exist_ok=True)
DOCS = ROOT / "docs"


def extract_config_fields_from_code() -> Dict[str, Any]:
    """Извлекает поля конфигов из Pydantic моделей."""
    try:
        from bioetl.config.models import PipelineConfig
        
        fields = {}
        
        if hasattr(PipelineConfig, "model_fields"):
            for field_name, field_info in PipelineConfig.model_fields.items():
                fields[field_name] = {
                    "type": str(field_info.annotation) if hasattr(field_info, "annotation") else None,
                    "required": field_info.is_required() if hasattr(field_info, "is_required") else None,
                }
        
        return fields
    except Exception as e:
        return {"error": str(e)}


def extract_config_fields_from_yaml(config_path: Path) -> Dict[str, Any]:
    """Извлекает поля из YAML конфига."""
    with config_path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config


def main():
    """Основная функция schema-guard."""
    print("Extracting config fields from code...")
    code_fields = extract_config_fields_from_code()
    
    print("Checking config files...")
    config_files = [
        ROOT / "configs" / "pipelines" / "chembl" / "activity_chembl.yaml",
        ROOT / "configs" / "pipelines" / "chembl" / "assay_chembl.yaml",
        ROOT / "configs" / "pipelines" / "chembl" / "testitem_chembl.yaml",
    ]
    
    report = {
        "code_fields": code_fields,
        "config_files": {},
    }
    
    for config_file in config_files:
        if config_file.exists():
            print(f"  Checking {config_file.relative_to(ROOT)}...")
            yaml_fields = extract_config_fields_from_yaml(config_file)
            report["config_files"][str(config_file.relative_to(ROOT))] = {
                "keys": list(yaml_fields.keys()) if isinstance(yaml_fields, dict) else [],
            }
    
    output_file = AUDIT_RESULTS / "schema-guard-report.json"
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"Schema-guard report saved to {output_file}")


if __name__ == "__main__":
    main()

