from __future__ import annotations

import sys
from pathlib import Path


def is_atx_heading(line: str) -> bool:
    stripped = line.lstrip()
    if not stripped.startswith("#"):
        return False
    # At least one space after hashes and non-empty text
    hashes, _, rest = stripped.partition(" ")
    return 1 <= len(hashes) <= 6 and rest.strip() != ""


def is_list_item(line: str) -> bool:
    stripped = line.lstrip()
    return (
        stripped.startswith("- ")
        or stripped.startswith("* ")
        or stripped[:2].isdigit() and stripped.strip().startswith(".")
    )


def fix_file(path: Path) -> bool:
    original = path.read_text(encoding="utf-8").splitlines()
    changed = False

    # Pass 1: add language to bare fenced code blocks
    out: list[str] = []
    for line in original:
        if line.strip() == "```":
            out.append("```text")
            changed = True
        else:
            out.append(line)

    # Pass 2: ensure blank lines around headings and lists
    result: list[str] = []
    n = len(out)
    i = 0
    seen_headings: set[str] = set()
    while i < n:
        line = out[i]
        prev = result[-1] if result else ""
        next_line = out[i + 1] if i + 1 < n else ""

        # Surround ATX headings with blank lines
        if is_atx_heading(line):
            # normalize trailing punctuation in headings
            stripped = line.rstrip()
            while stripped.endswith((":", ";", ".", "!", "?", "…", "：", "；", "。")):
                stripped = stripped[:-1].rstrip()
            if stripped != line:
                line = stripped
                changed = True

            # deduplicate identical headings by appending marker
            parts = line.lstrip().split(" ", 1)
            heading_text = parts[1] if len(parts) > 1 else ""
            if heading_text in seen_headings:
                line = f"{line} (continued)"
                changed = True
            else:
                seen_headings.add(heading_text)

            if prev.strip() != "":
                result.append("")
                changed = True
            result.append(line)
            if next_line.strip() != "":
                result.append("")
                changed = True
            i += 1
            continue

        # Ensure lists are surrounded by blank lines
        if is_list_item(line):
            if prev.strip() != "" and not is_list_item(prev):
                result.append("")
                changed = True
            result.append(line)
            # Peek ahead to see if list ends next
            j = i + 1
            # write following list lines as-is in subsequent iterations
            if next_line and next_line.strip() == "" and j + 1 < n and not is_list_item(out[j + 1]):
                pass
            i += 1
            continue

        # Ensure blank lines around fenced code blocks
        if line.strip().startswith("```"):
            if prev.strip() != "":
                result.append("")
                changed = True
            result.append(line)
            # add blank line after fence start or end if needed will be handled when next_line consumed
            if next_line.strip() != "":
                # only add blank after closing fence; we don't know here, but safe to add and collapse later
                result.append("")
                changed = True
            i += 1
            continue

        result.append(line)
        i += 1

    # Collapse multiple blank lines to a single blank
    collapsed: list[str] = []
    blank_run = 0
    for line in result:
        if line.strip() == "":
            blank_run += 1
            if blank_run == 1:
                collapsed.append("")
                continue
            changed = True
            continue
        blank_run = 0
        collapsed.append(line)

    if collapsed and collapsed[-1] != "":
        collapsed.append("")
        changed = True

    if changed:
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text("\n".join(collapsed) + "\n", encoding="utf-8")
        tmp_path.replace(path)
    return changed


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    md_files = list(root.glob("*.md")) + list((root / "docs").rglob("*.md"))
    any_changed = False
    for md in md_files:
        any_changed = fix_file(md) or any_changed
    print(f"Fixed: {any_changed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())


