from __future__ import annotations

import io
import os
import re
import sys
from pathlib import Path


def _is_atx_heading(line: str) -> bool:
    stripped = line.lstrip()
    if not stripped.startswith("#"):
        return False
    hashes, _, rest = stripped.partition(" ")
    return 1 <= len(hashes) <= 6 and rest.strip() != ""


def _is_list_item(line: str) -> bool:
    stripped = line.lstrip()
    return (
        stripped.startswith("- ")
        or stripped.startswith("* ")
        or stripped.startswith("+ ")
        or (stripped[:2].isdigit() and stripped.strip().startswith("."))
    )


def _atomic_write_text(path: Path, content: str) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)
        f.flush()
        try:
            os.fsync(f.fileno())
        except (AttributeError, io.UnsupportedOperation, OSError):
            # Best-effort on platforms/filesystems without fsync
            pass
    os.replace(tmp_path, path)


def fix_markdown_file(path: Path) -> bool:
    """Применяет безопасные авто-фиксы Markdown к одному файлу.

    Возвращает True, если файл был изменён.
    Включает: нормализацию fenced-блоков, заголовков, списков, пустых строк,
    дедуп заголовков и атомарную запись с LF.
    """
    try:
        original = path.read_text(encoding="utf-8")
    except Exception:
        # Fallback для легаси-кодировок на Windows
        original = path.read_text(encoding="cp1251")

    lines = original.splitlines()

    heading_no_space_re = re.compile(r"^(#{1,6})([^#\s].*)$")
    heading_indented_re = re.compile(r"^\s+(#{1,6}\s.*)$")
    list_marker_star_re = re.compile(r"^\s*\*\s+")
    list_marker_plus_re = re.compile(r"^\s*\+\s+")

    changed = False

    # Фаза 1: нормализация fenced-блоков и пер-линейные правки вне кода
    out: list[str] = []
    in_fence = False
    for raw in lines:
        line = raw
        stripped = line.strip()
        if stripped.startswith("```"):
            # Fence линия
            if not in_fence:
                in_fence = True
                # Открывающая без языка → ```text
                if stripped == "```":
                    line = "```text"
                    if line != raw:
                        changed = True
                else:
                    # Нормализуем пробелы
                    if line != stripped:
                        line = stripped
                        changed = True
            else:
                # Закрывающая всегда как ```
                in_fence = False
                if stripped != "```":
                    line = "```"
                    changed = True
            out.append(line.rstrip())
            continue

        if in_fence:
            # Внутри кода только trim хвостовых пробелов
            new_line = line.rstrip(" \t")
            if new_line != line:
                changed = True
            out.append(new_line)
            continue

        # Вне кода: трим хвостовые пробелы
        cur = line.rstrip(" \t")
        if cur != line:
            changed = True

        # Заголовки: убрать индентацию и добавить пробел после '#'
        new_cur = heading_indented_re.sub(r"\\1", cur)
        if new_cur != cur:
            changed = True
        cur = new_cur
        new_cur = heading_no_space_re.sub(r"\\1 \\2", cur)
        if new_cur != cur:
            changed = True
        cur = new_cur

        # Маркеры списков: унифицировать к '- '
        if list_marker_star_re.match(cur) or list_marker_plus_re.match(cur):
            new_cur = re.sub(r"^(\s*)[+*]\s+", r"\\1- ", cur)
            if new_cur != cur:
                changed = True
            cur = new_cur
        # Обеспечить пробел после '-' если его нет
        new_cur = re.sub(r"^(\s*)-([^\s-])", r"\\1- \\2", cur)
        if new_cur != cur:
            changed = True
        cur = new_cur

        out.append(cur)

    # Фаза 2: структурные пустые строки и дедуп заголовков
    result: list[str] = []
    n = len(out)
    i = 0
    seen_headings: dict[str, int] = {}
    while i < n:
        line = out[i]
        prev = result[-1] if result else ""
        next_line = out[i + 1] if i + 1 < n else ""

        # Заголовки: обрезать завершающую пунктуацию, дедуп, пустые строки вокруг
        if _is_atx_heading(line):
            stripped = line.rstrip()
            while stripped.endswith((":", ";", ".", "!", "?", "…", "：", "；", "。")):
                stripped = stripped[:-1].rstrip()
            if stripped != line:
                line = stripped
                changed = True

            parts = line.lstrip().split(" ", 1)
            heading_text = parts[1] if len(parts) > 1 else ""
            base_text = heading_text.rsplit(" (continued", 1)[0]
            count = seen_headings.get(base_text, 0)
            if count > 0:
                line = f"{parts[0]} {base_text} (continued {count})"
                changed = True
            seen_headings[base_text] = count + 1

            if prev.strip() != "":
                result.append("")
                changed = True
            result.append(line)
            if next_line.strip() != "":
                result.append("")
                changed = True
            i += 1
            continue

        # Списки: пустая строка перед началом и после блока
        if _is_list_item(line):
            if prev.strip() != "" and not _is_list_item(prev):
                result.append("")
                changed = True
            # Скопировать непрерывный блок списка как есть
            while i < n and (_is_list_item(out[i]) or out[i].strip() == ""):
                result.append(out[i])
                i += 1
            if i < n and out[i].strip() != "":
                result.append("")
                changed = True
            continue

        # Пустая строка перед открывающим fence, если требуется
        if line.strip().startswith("```") and prev.strip() != "" and not prev.strip().startswith("```"):
            result.append("")
            changed = True

        # Пустая строка после закрывающего fence
        if prev.strip() == "```" and line.strip() != "" and not line.strip().startswith("```"):
            result.append("")
            changed = True

        result.append(line)
        i += 1

    # Схлопнуть множественные пустые строки до одной и гарантировать финальную пустую строку
    collapsed: list[str] = []
    blank_run = 0
    for line in result:
        if line.strip() == "":
            blank_run += 1
            if blank_run == 1:
                collapsed.append("")
            else:
                changed = True
            continue
        blank_run = 0
        collapsed.append(line)

    if collapsed and collapsed[-1] != "":
        collapsed.append("")
        changed = True

    final_text = "\n".join(collapsed)

    if final_text + "\n" != original if original.endswith("\n") else final_text != original:
        _atomic_write_text(path, final_text + "\n")
        return True
    return changed


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    exclude = {"node_modules", "build", ".git", ".venv", "dist", "htmlcov", "__pycache__"}

    files: list[Path] = []
    for p in root.rglob("*.md"):
        if any(part in exclude for part in p.parts):
            continue
        files.append(p)
    files.sort(key=lambda p: (tuple(p.parts), str(p).lower()))

    fixed = 0
    for md in files:
        try:
            if fix_markdown_file(md):
                fixed += 1
        except Exception as exc:
            print(f"WARN: failed to fix {md}: {exc}")

    print(f"Fixed {fixed} Markdown file(s) out of {len(files)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
