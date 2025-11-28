
import sys
import traceback
from bioetl.cli.cli_registry import _register_default_pipelines, PIPELINE_REGISTRY

print("Starting registry check...")
# Force re-run of registration if it already ran on import
_register_default_pipelines()

print(f"Registry size: {len(PIPELINE_REGISTRY)}")
print(f"Keys: {list(PIPELINE_REGISTRY.keys())}")
