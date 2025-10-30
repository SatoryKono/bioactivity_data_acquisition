from __future__ import annotations

import os
import re
from pathlib import Path


def fix_markdown_file(path: Path) -> bool:
    """Apply safe Markdown MD* autofixes to a single file.

    Returns True if file was modified.
    """
    try:
        original = path.read_text(encoding="utf-8")
    except Exception:
        # Fallback for legacy encodings on Windows
        original = path.read_text(encoding="cp1251")

    lines = original.splitlines()

    code_fence_re = re.compile(r"^\s*```")
    heading_no_space_re = re.compile(r"^(#{1,6})([^#\s].*)$")
    heading_indented_re = re.compile(r"^\s+(#{1,6}\s.*)$")
    list_marker_star_re = re.compile(r"^\s*\*\s+")
    list_marker_plus_re = re.compile(r"^\s*\+\s+")

    out_lines: list[str] = []
    in_code = False

    # Pass 1: per-line cleanup
    for line in lines:
        current = line
        if code_fence_re.match(current):
            in_code = not in_code
            out_lines.append(current.rstrip())
            continue
        if not in_code:
            # Remove trailing spaces/tabs
            current = current.rstrip(" \t")
            # Headings: remove indentation and add single space after '#'
            current = heading_indented_re.sub(r"\\1", current)
            current = heading_no_space_re.sub(r"\\1 \\2", current)
            # Normalize unordered list markers to '- '
            if list_marker_star_re.match(current) or list_marker_plus_re.match(current):
                current = re.sub(r"^(\s*)[+*]\s+", r"\\1- ", current)
            # Ensure space after dash for list items like '-item'
            current = re.sub(r"^(\s*)-([^\s-])", r"\\1- \\2", current)
        else:
            # In code blocks only trim trailing whitespace
            current = current.rstrip(" \t")
        out_lines.append(current)

    # Pass 2: structural blank-line normalization
    normalized: list[str] = []
    in_code = False
    prev_blank = True  # treat BOF as blank

    def is_heading(s: str) -> bool:
        return bool(re.match(r"^#{1,6}\s", s))

    i = 0
    n = len(out_lines)
    while i < n:
        line = out_lines[i]
        is_blank = line.strip() == ""

        if code_fence_re.match(line):
            # Ensure blank line before code fence when previous is non-blank and not a heading
            if not prev_blank and normalized and not is_heading(normalized[-1]):
                normalized.append("")
            normalized.append(line)
            i += 1
            # Copy lines until closing fence
            while i < n:
                l2 = out_lines[i]
                normalized.append(l2)
                if code_fence_re.match(l2):
                    break
                i += 1
            # Ensure blank line after code fence when next is non-blank
            if i + 1 < n and out_lines[i + 1].strip() != "":
                normalized.append("")
            prev_blank = True
            i += 1
            continue

        if is_heading(line):
            if not prev_blank:
                normalized.append("")
            normalized.append(line)
            if i + 1 < n and out_lines[i + 1].strip() != "":
                normalized.append("")
            prev_blank = True
            i += 1
            continue

        if is_blank:
            if not prev_blank:
                normalized.append("")
                prev_blank = True
        else:
            normalized.append(line)
            prev_blank = False
        i += 1

    result = "\n".join(normalized).rstrip("\n") + "\n"

    if result != original:
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(result, encoding="utf-8", newline="\n")
        os.replace(tmp_path, path)
        return True
    return False


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    exclude = {"node_modules", "build", ".git", ".venv", "dist", "htmlcov", "__pycache__"}
    fixed_count = 0
    files: list[Path] = []
    for p in root.rglob("*.md"):
        if any(part in exclude for part in p.parts):
            continue
        files.append(p)

    for md in files:
        try:
            if fix_markdown_file(md):
                fixed_count += 1
        except Exception as exc:
            # Best-effort: skip problematic files
            print(f"WARN: failed to fix {md}: {exc}")

    print(f"Fixed {fixed_count} Markdown file(s) out of {len(files)}")


if __name__ == "__main__":
    main()


