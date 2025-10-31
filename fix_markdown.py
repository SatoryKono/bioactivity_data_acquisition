#!/usr/bin/env python3
"""Fix common markdown issues in project files."""
import glob
import re
from pathlib import Path


def fix_markdown_references(text: str) -> str:
    """Convert custom code references to standard format."""
    # Pattern: 【F:path†Lstart-end】
    # Replace with: (см. `path` строки start-end).
    # Dagger character is U+2020
    pattern = r'【F:([^】]+?)†L(\d+)-(\d+)】'
    
    def replace_func(match):
        path = match.group(1)
        start = match.group(2)
        end = match.group(3)
        return f'(см. `{path}` строки {start}-{end}).'
    
    text = re.sub(pattern, replace_func, text)
    return text


def fix_numbered_lists(text: str) -> str:
    """Fix escaped numbered list markers."""
    # Pattern: \1- becomes 1.
    # Need to handle multi-digit: \12- becomes 12.
    pattern = r'\\\d+-'
    
    def replace_func(match):
        num = match.group(0).replace('\\', '').replace('-', '')
        return f'{num}.'
    
    text = re.sub(pattern, replace_func, text)
    return text


def process_file(filepath: Path) -> bool:
    """Process a single markdown file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        original = f.read()
    
    modified = original
    modified = fix_markdown_references(modified)
    modified = fix_numbered_lists(modified)
    
    if modified != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(modified)
        return True
    return False


def main():
    """Process all markdown files."""
    md_files = glob.glob('**/*.md', recursive=True)
    
    fixed_count = 0
    for filepath in sorted(md_files):
        path = Path(filepath)
        # Skip files in test data and generated directories
        if any(skip in path.parts for skip in ['htmlcov', '__pycache__', '.git']):
            continue
        
        if process_file(path):
            print(f'Fixed: {filepath}')
            fixed_count += 1
    
    print(f'\nFixed {fixed_count} files')


if __name__ == '__main__':
    main()

