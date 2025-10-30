from __future__ import annotations

import re
from pathlib import Path


HEADING_RE = re.compile(r"^(#{1,6})\s+\S")
LIST_ITEM_RE = re.compile(r"^\s*(?:[-*]|\d+\.)\s+\S")
FENCE_RE = re.compile(r"^\s*```")


def ensure_blank_lines_around_blocks(lines: list[str]) -> list[str]:
    result: list[str] = []
    in_code_fence = False
    i = 0
    n = len(lines)

    def is_blank(idx: int) -> bool:
        if idx < 0 or idx >= n:
            return True
        return lines[idx].strip() == ""

    while i < n:
        line = lines[i]
        if FENCE_RE.match(line):
            in_code_fence = not in_code_fence
            result.append(line)
            i += 1
            continue

        if in_code_fence:
            result.append(line)
            i += 1
            continue

        # Headings: ensure blank line before and after
        if HEADING_RE.match(line):
            if not is_blank(i - 1) and result and result[-1].strip() != "":
                result.append("\n")
            result.append(line)
            # ensure a blank after heading if next is not blank and not end
            if not is_blank(i + 1):
                result.append("\n")
            i += 1
            continue

        # Lists: ensure blank line before and after list blocks
        if LIST_ITEM_RE.match(line):
            # ensure blank before start of list block
            if not is_blank(i - 1) and result and result[-1].strip() != "":
                result.append("\n")
            # consume contiguous list block
            start = i
            while i < n and (LIST_ITEM_RE.match(lines[i]) or lines[i].strip() == ""):
                result.append(lines[i])
                i += 1
            # ensure blank after list block if next non-blank is not end
            if i < n and lines[i].strip() != "":
                result.append("\n")
            continue

        result.append(line)
        i += 1

    return result


def process_file(path: Path) -> None:
    original = path.read_text(encoding="utf-8").splitlines(keepends=True)
    fixed = ensure_blank_lines_around_blocks(original)
    if fixed != original:
        path.write_text("".join(fixed), encoding="utf-8")


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    md_paths = list(root.glob("*.md")) + list((root / "docs").rglob("*.md"))
    for p in md_paths:
        process_file(p)


if __name__ == "__main__":
    main()


