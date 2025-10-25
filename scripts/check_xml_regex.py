#!/usr/bin/env python3
"""Проверяет отсутствие regex в XML-парсинге."""
import re
import sys


FORBIDDEN_PATTERN = re.compile(r're\.(search|findall|sub|match)\([^)]*<[^>]+>')

def check_file(filepath):
    """Проверяет файл на наличие regex для XML парсинга."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            matches = FORBIDDEN_PATTERN.findall(content)
            if matches:
                print(f"[ERROR] {filepath}: Found regex for XML/HTML parsing")
                for match in matches:
                    print(f"   - Found: re.{match}")
                return False
    except Exception as e:
        print(f"[WARNING] {filepath}: Error reading file: {e}")
        return False
    return True

def main():
    """Основная функция."""
    if len(sys.argv) < 2:
        print("Usage: python check_xml_regex.py <file1> [file2] ...")
        sys.exit(1)
    
    all_ok = True
    for filepath in sys.argv[1:]:
        if not check_file(filepath):
            all_ok = False
    
    if not all_ok:
        print("\n[ERROR] Found regex usage in XML parsing files!")
        print("Use lxml.etree with XPath instead of regex for structured data.")
        sys.exit(1)
    else:
        print("[OK] No regex found in XML parsing files")

if __name__ == "__main__":
    main()
