#!/usr/bin/env python3
"""Detect Type I/II text clones in codebase."""

from __future__ import annotations

import csv
import difflib
from collections import defaultdict
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).parent.parent.parent
SRC_DIR = PROJECT_ROOT / "src"
REPORTS_DIR = PROJECT_ROOT / "reports"


def normalize_code(text: str) -> str:
    """Normalize code for comparison (remove comments, normalize whitespace)."""
    lines = []
    for line in text.splitlines():
        # Remove comments
        if "#" in line:
            line = line.split("#")[0]
        # Normalize whitespace
        line = " ".join(line.split())
        if line:
            lines.append(line)
    return "\n".join(lines)


def extract_functions(file_path: Path) -> list[dict[str, Any]]:
    """Extract function definitions from a Python file."""
    try:
        content = file_path.read_text(encoding="utf-8")
        lines = content.splitlines()
        
        functions = []
        in_function = False
        function_start = 0
        function_lines = []
        function_name = ""
        indent_level = 0
        
        for i, line in enumerate(lines, 1):
            stripped = line.lstrip()
            if not stripped or stripped.startswith("#"):
                if in_function:
                    function_lines.append(line)
                continue
            
            current_indent = len(line) - len(stripped)
            
            if stripped.startswith("def ") and not in_function:
                # Start of new function
                function_name = stripped.split("(")[0].replace("def ", "").strip()
                function_start = i
                function_lines = [line]
                in_function = True
                indent_level = current_indent
            elif in_function:
                if current_indent <= indent_level and stripped and not stripped.startswith("@") and not stripped.startswith("def "):
                    # End of function (reached same or lower indent)
                    function_text = "\n".join(function_lines)
                    functions.append({
                        "name": function_name,
                        "start_line": function_start,
                        "end_line": i - 1,
                        "text": function_text,
                        "normalized": normalize_code(function_text),
                    })
                    in_function = False
                    function_lines = []
                else:
                    function_lines.append(line)
        
        # Handle function at end of file
        if in_function:
            function_text = "\n".join(function_lines)
            functions.append({
                "name": function_name,
                "start_line": function_start,
                "end_line": len(lines),
                "text": function_text,
                "normalized": normalize_code(function_text),
            })
        
        return functions
    except Exception as e:
        return []


def compare_texts(text1: str, text2: str) -> float:
    """Calculate similarity ratio between two texts."""
    return difflib.SequenceMatcher(None, text1, text2).ratio()


def detect_clones() -> list[dict[str, Any]]:
    """Detect text clones across the codebase."""
    clone_groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    all_fragments: list[dict[str, Any]] = []
    fragment_id = 1
    
    # Analyze CLI commands
    cli_commands_dir = SRC_DIR / "bioetl" / "cli" / "commands"
    for cmd_file in sorted(cli_commands_dir.glob("chembl_*.py")):
        functions = extract_functions(cmd_file)
        for func in functions:
            if func["name"] == "build_command_config":
                fragment = {
                    "fragment_id": fragment_id,
                    "path": str(cmd_file.relative_to(PROJECT_ROOT)),
                    "start_line": func["start_line"],
                    "end_line": func["end_line"],
                    "normalized": func["normalized"],
                    "type": "CLI_command",
                }
                all_fragments.append(fragment)
                fragment_id += 1
    
    # Analyze pipeline methods _get_chembl_release
    pipelines_dir = SRC_DIR / "bioetl" / "pipelines"
    for pipeline_file in sorted(pipelines_dir.glob("chembl_*.py")):
        functions = extract_functions(pipeline_file)
        for func in functions:
            if func["name"] == "_get_chembl_release":
                fragment = {
                    "fragment_id": fragment_id,
                    "path": str(pipeline_file.relative_to(PROJECT_ROOT)),
                    "start_line": func["start_line"],
                    "end_line": func["end_line"],
                    "normalized": func["normalized"],
                    "type": "pipeline_method",
                }
                all_fragments.append(fragment)
                fragment_id += 1
    
    # Analyze launch scripts
    scripts_dir = SRC_DIR / "scripts"
    for script_file in sorted(scripts_dir.glob("run_chembl_*.py")):
        content = script_file.read_text(encoding="utf-8")
        lines = content.splitlines()
        fragment = {
            "fragment_id": fragment_id,
            "path": str(script_file.relative_to(PROJECT_ROOT)),
            "start_line": 1,
            "end_line": len(lines),
            "normalized": normalize_code(content),
            "type": "launch_script",
        }
        all_fragments.append(fragment)
        fragment_id += 1
    
    # Group similar fragments
    clone_group_id = 1
    processed = set()
    
    for i, frag1 in enumerate(all_fragments):
        if i in processed:
            continue
        
        group = [frag1]
        processed.add(i)
        
        for j, frag2 in enumerate(all_fragments[i+1:], start=i+1):
            if j in processed:
                continue
            
            similarity = compare_texts(frag1["normalized"], frag2["normalized"])
            if similarity >= 0.85:  # Type I/II threshold
                group.append(frag2)
                processed.add(j)
        
        if len(group) > 1:
            for frag in group:
                frag["clone_group"] = clone_group_id
            clone_group_id += 1
    
    return all_fragments


def generate_report(clones: list[dict[str, Any]]) -> None:
    """Generate CSV report of detected clones."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Filter only fragments that are part of clone groups
    cloned_fragments = [f for f in clones if "clone_group" in f]
    
    # Calculate similarities within groups
    clone_groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for frag in cloned_fragments:
        clone_groups[frag["clone_group"]].append(frag)
    
    report_rows = []
    for frag in cloned_fragments:
        group = clone_groups[frag["clone_group"]]
        if len(group) < 2:
            continue
        
        # Calculate average similarity with other group members
        similarities = []
        for other in group:
            if other["fragment_id"] != frag["fragment_id"]:
                sim = compare_texts(frag["normalized"], other["normalized"])
                similarities.append(sim)
        
        avg_similarity = sum(similarities) / len(similarities) if similarities else 1.0
        type_hint = "I" if avg_similarity >= 0.95 else "II"
        
        report_rows.append({
            "fragment_id": frag["fragment_id"],
            "path": frag["path"],
            "start_line": frag["start_line"],
            "end_line": frag["end_line"],
            "clone_group": frag["clone_group"],
            "similarity": f"{avg_similarity:.2f}",
            "type_hint": type_hint,
        })
    
    # Sort by clone group, then by path
    report_rows.sort(key=lambda x: (x["clone_group"], x["path"]))
    
    # Write CSV
    csv_path = REPORTS_DIR / "clone_text.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["fragment_id", "path", "start_line", "end_line", "clone_group", "similarity", "type_hint"], delimiter=";")
        writer.writeheader()
        writer.writerows(report_rows)
    
    print(f"Generated {csv_path} with {len(report_rows)} clone fragments in {len(clone_groups)} groups")


def main() -> None:
    """Main entry point."""
    print("Detecting text clones (Type I/II)...")
    clones = detect_clones()
    print(f"Analyzed {len(clones)} fragments")
    generate_report(clones)


if __name__ == "__main__":
    main()

