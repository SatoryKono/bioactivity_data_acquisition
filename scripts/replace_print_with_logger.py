#!/usr/bin/env python3
"""Script to replace print() statements with logger calls in tools/ directory."""

import os
import re
import sys
from pathlib import Path

def add_logger_import(file_path: Path) -> str:
    """Add logger import to the beginning of the file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if logger import already exists
    if 'from library.logging_setup import get_logger' in content:
        return content
    
    # Find the last import statement
    lines = content.split('\n')
    import_end = 0
    
    for i, line in enumerate(lines):
        if line.strip().startswith(('import ', 'from ')) and not line.strip().startswith('#'):
            import_end = i + 1
    
    # Add logger import after the last import
    if import_end > 0:
        lines.insert(import_end, '')
        lines.insert(import_end + 1, 'from library.logging_setup import get_logger')
        lines.insert(import_end + 2, '')
        lines.insert(import_end + 3, 'logger = get_logger(__name__)')
    else:
        # If no imports found, add at the beginning
        lines.insert(0, 'from library.logging_setup import get_logger')
        lines.insert(1, '')
        lines.insert(2, 'logger = get_logger(__name__)')
        lines.insert(3, '')
    
    return '\n'.join(lines)

def replace_print_statements(content: str) -> str:
    """Replace print() statements with appropriate logger calls."""
    
    # Pattern to match print statements
    print_pattern = r'print\s*\(\s*([^)]+)\s*\)'
    
    def replace_print(match):
        print_content = match.group(1)
        
        # Determine log level based on content
        if any(keyword in print_content.lower() for keyword in ['error', 'ошибка', 'failed', 'exception']):
            return f'logger.error({print_content})'
        elif any(keyword in print_content.lower() for keyword in ['warning', 'предупреждение', 'warn']):
            return f'logger.warning({print_content})'
        elif any(keyword in print_content.lower() for keyword in ['debug', 'отладка']):
            return f'logger.debug({print_content})'
        else:
            return f'logger.info({print_content})'
    
    return re.sub(print_pattern, replace_print, content)

def process_file(file_path: Path) -> bool:
    """Process a single Python file to replace print() with logger calls."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
        
        # Skip if no print statements
        if 'print(' not in original_content:
            return False
        
        # Add logger import
        content_with_logger = add_logger_import(file_path)
        
        # Replace print statements
        new_content = replace_print_statements(content_with_logger)
        
        # Write back if changed
        if new_content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            return True
        
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Main function to process all Python files in tools/ directory."""
    tools_dir = Path("src/library/tools")
    
    if not tools_dir.exists():
        print(f"Tools directory not found: {tools_dir}")
        print("Note: CLI scripts have been moved to scripts/ directory")
        return
    
    processed_files = 0
    modified_files = 0
    
    for py_file in tools_dir.glob("*.py"):
        if py_file.name == "__init__.py":
            continue
            
        processed_files += 1
        print(f"Processing {py_file}...")
        
        if process_file(py_file):
            modified_files += 1
            print(f"  [MODIFIED] {py_file}")
        else:
            print(f"  [NO CHANGES] {py_file}")
    
    print(f"\nSummary:")
    print(f"  Processed files: {processed_files}")
    print(f"  Modified files: {modified_files}")

if __name__ == "__main__":
    main()
