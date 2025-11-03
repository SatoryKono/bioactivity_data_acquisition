#!/usr/bin/env python3
"""
Скрипт для автоматического исправления ошибок markdownlint (MD*).
Исправляет:
- MD012: Multiple consecutive blank lines → оставляет только одну пустую строку
- MD022: Headings should be surrounded by blank lines → добавляет пустые строки
- MD032: Lists should be surrounded by blank lines → добавляет пустые строки
- MD034: Bare URL used → оборачивает URL в ссылки (опционально)
"""
import re
import sys
from pathlib import Path
from typing import List, Tuple


def fix_multiple_blanks(content: str) -> str:
    """MD012: Удаляет множественные пустые строки."""
    return re.sub(r'\n{3,}', '\n\n', content)


def fix_heading_blanks(content: str) -> str:
    """MD022: Добавляет пустые строки вокруг заголовков."""
    lines = content.split('\n')
    result: List[str] = []

    for i, line in enumerate(lines):
        # Проверяем, является ли строка заголовком
        is_heading = bool(re.match(r'^#{1,6}\s+.+', line))

        if is_heading:
            # Добавляем пустую строку перед заголовком, если предыдущая строка не пустая
            if result and result[-1].strip():
                result.append('')
            result.append(line)
            # Добавляем пустую строку после заголовка, если следующая строка не пустая и не пустая
            if i < len(lines) - 1:
                next_line = lines[i + 1]
                if next_line.strip() and not next_line.strip().startswith('#'):
                    result.append('')
        else:
            result.append(line)

    return '\n'.join(result)


def fix_list_blanks(content: str) -> str:
    """MD032: Добавляет пустые строки вокруг списков."""
    lines = content.split('\n')
    result: List[str] = []
    in_list = False

    for i, line in enumerate(lines):
        is_list_item = bool(re.match(r'^[\s]*[-*+]\s+', line) or re.match(r'^[\s]*\d+[\.\)]\s+', line))

        if is_list_item:
            if not in_list and result and result[-1].strip():
                result.append('')
            in_list = True
            result.append(line)
        else:
            if in_list and line.strip():
                result.append('')
            in_list = False
            result.append(line)

    return '\n'.join(result)


def fix_bare_urls(content: str) -> str:
    """MD034: Обёртывает голые URL в ссылки."""
    lines = content.split('\n')
    result: List[str] = []

    for line in lines:
        # Пропускаем строки в блоках кода (```)
        if line.strip().startswith('```'):
            result.append(line)
            continue

        # Пропускаем строки, которые уже содержат ссылки markdown
        if '](' in line or '<http' in line:
            result.append(line)
            continue

        # Ищем голые URL и оборачиваем их в < >
        url_pattern = r'(?<!<)(?<!\]\()(https?://[^\s<>"{}|\\^`\[\]]+)(?!>)'

        def replace_url(match: re.Match[str]) -> str:
            url = match.group(0)
            # Проверяем, не является ли это частью ссылки markdown
            if '](' in line[:match.start()] or '](' in line[match.end():]:
                return url
            return f'<{url}>'

        line = re.sub(url_pattern, replace_url, line)
        result.append(line)

    return '\n'.join(result)


def fix_markdown_file(file_path: Path) -> Tuple[bool, int]:
    """
    Исправляет ошибки markdownlint в файле.
    Возвращает (были ли изменения, количество исправлений).
    """
    try:
        original_content = file_path.read_text(encoding='utf-8')
        content = original_content

        # Применяем исправления в правильном порядке
        content = fix_multiple_blanks(content)
        content = fix_list_blanks(content)
        content = fix_heading_blanks(content)
        # Исправляем голые URL только в обычном тексте (не в ссылках и коде)
        # Это более консервативный подход
        content = fix_bare_urls(content)

        # Убираем множественные пустые строки в конце файла
        content = content.rstrip() + '\n'

        if content != original_content:
            file_path.write_text(content, encoding='utf-8')
            changes = len([1 for a, b in zip(content.split('\n'), original_content.split('\n')) if a != b])
            return True, changes
        return False, 0
    except Exception as e:
        print(f"Ошибка при обработке {file_path}: {e}", file=sys.stderr)
        return False, 0


def main():
    """Основная функция."""
    if len(sys.argv) > 1:
        md_files = [Path(p) for p in sys.argv[1:]]
    else:
        # Ищем все .md файлы в проекте
        md_files = list(Path('.').rglob('*.md'))
        # Исключаем node_modules и другие служебные директории
        md_files = [f for f in md_files if 'node_modules' not in str(f) and '.git' not in str(f)]

    total_changed = 0
    total_files = 0

    for md_file in sorted(md_files):
        if md_file.exists():
            changed, count = fix_markdown_file(md_file)
            if changed:
                total_changed += 1
                total_files += count
                print(f"Исправлено: {md_file} ({count} изменений)")

    print(f"\nВсего исправлено файлов: {total_changed}")
    print(f"Всего внесено изменений: {total_files}")


if __name__ == '__main__':
    main()
