#!/usr/bin/env python3
"""Generate module inventory and import graph for clone detection."""

from __future__ import annotations

import ast
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).parent.parent.parent
SRC_DIR = PROJECT_ROOT / "src"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
REPORTS_DIR = PROJECT_ROOT / "reports"


def analyze_module(module_path: Path) -> dict[str, Any]:
    """Analyze a single Python module."""
    try:
        content = module_path.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(module_path))
        
        imports: list[str] = []
        functions: list[str] = []
        classes: list[str] = []
        
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.append(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    for alias in node.names:
                        imports.append(f"{module}.{alias.name}" if module else alias.name)
            elif isinstance(node, ast.FunctionDef):
                functions.append(node.name)
            elif isinstance(node, ast.ClassDef):
                classes.append(node.name)
        
        return {
            "path": str(module_path.relative_to(PROJECT_ROOT)),
            "size_bytes": len(content),
            "size_lines": len(content.splitlines()),
            "imports": sorted(set(imports)),
            "functions": functions,
            "classes": classes,
            "import_count": len(set(imports)),
            "function_count": len(functions),
            "class_count": len(classes),
        }
    except Exception as e:
        return {
            "path": str(module_path.relative_to(PROJECT_ROOT)),
            "error": str(e),
        }


def collect_modules(directory: Path, prefix: str = "") -> dict[str, dict[str, Any]]:
    """Collect all Python modules in a directory."""
    modules: dict[str, dict[str, Any]] = {}
    
    for py_file in sorted(directory.rglob("*.py")):
        if "__pycache__" in py_file.parts:
            continue
        
        rel_path = py_file.relative_to(directory)
        module_key = f"{prefix}{rel_path.as_posix().replace('/', '.')[:-3]}"
        
        modules[module_key] = analyze_module(py_file)
    
    return modules


def build_import_graph(modules: dict[str, dict[str, Any]]) -> dict[str, list[str]]:
    """Build import graph from module imports."""
    graph: dict[str, list[str]] = defaultdict(list)
    
    for module_key, module_data in modules.items():
        if "error" in module_data:
            continue
        
        imports = module_data.get("imports", [])
        for imp in imports:
            # Normalize import to module path
            imp_module = imp.split(".")[0]
            if imp_module.startswith("bioetl"):
                graph[module_key].append(imp)
    
    return dict(graph)


def generate_mermaid_graph(modules: dict[str, dict[str, Any]], graph: dict[str, list[str]]) -> str:
    """Generate Mermaid diagram from import graph."""
    lines = ["graph TD"]
    
    # Limit graph size to avoid explosion
    sorted_modules = sorted(modules.keys())[:100]  # Limit to first 100 for readability
    
    for module_key in sorted_modules:
        module_data = modules.get(module_key, {})
        if "error" in module_data:
            continue
        
        # Get short name for display
        display_name = module_key.split(".")[-1] if "." in module_key else module_key
        node_id = module_key.replace(".", "_").replace("-", "_")
        
        # Add node with size info
        size_lines = module_data.get("size_lines", 0)
        lines.append(f'    {node_id}["{display_name}<br/>({size_lines} lines)"]')
        
        # Add edges for imports within bioetl
        imports = module_data.get("imports", [])
        for imp in imports[:10]:  # Limit imports per module
            if imp.startswith("bioetl."):
                target_module = imp.split(".")[0] + "." + ".".join(imp.split(".")[1:])
                target_id = target_module.replace(".", "_").replace("-", "_")
                if target_id != node_id:
                    lines.append(f"    {node_id} --> {target_id}")
    
    return "\n".join(lines)


def main() -> None:
    """Generate module inventory and import graph."""
    print("Analyzing modules...")
    
    # Analyze key directories
    pipelines_modules = collect_modules(SRC_DIR / "bioetl" / "pipelines", "bioetl.pipelines.")
    cli_modules = collect_modules(SRC_DIR / "bioetl" / "cli" / "commands", "bioetl.cli.commands.")
    scripts_modules = collect_modules(SRC_DIR / "scripts", "scripts.")
    
    all_modules = {**pipelines_modules, **cli_modules, **scripts_modules}
    
    print(f"Found {len(all_modules)} modules")
    
    # Build import graph
    import_graph = build_import_graph(all_modules)
    
    # Generate module map
    module_map = {
        "metadata": {
            "total_modules": len(all_modules),
            "pipelines_count": len(pipelines_modules),
            "cli_commands_count": len(cli_modules),
            "scripts_count": len(scripts_modules),
        },
        "modules": all_modules,
    }
    
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    module_map_path = ARTIFACTS_DIR / "module_map.json"
    module_map_path.write_text(json.dumps(module_map, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Generated {module_map_path}")
    
    # Generate Mermaid graph
    mermaid_graph = generate_mermaid_graph(all_modules, import_graph)
    graph_path = ARTIFACTS_DIR / "import_graph.mmd"
    graph_path.write_text(mermaid_graph, encoding="utf-8")
    print(f"Generated {graph_path}")


if __name__ == "__main__":
    main()

