#!/usr/bin/env python3
"""Check for unwanted files in root directory."""

import sys
from pathlib import Path

ALLOWED_ROOT_FILES = {
    'README.md', 'Makefile', 'pyproject.toml', 'mkdocs.yml',
    'Dockerfile', 'docker-compose.yml', 'pyrightconfig.json',
    '.pre-commit-config.yaml', '.gitignore', '.markdownlint.json',
    '.python-version', 'LICENSE', 'CHANGELOG.md', '.bandit',
    '.banditignore', '.coverage', '.dockerignore', '.env.example',
    '.gitattributes', '.markdown-link-check.json', '.pre-commit-hooks.yaml',
    '.pymarkdown.json', '.safety_policy.yaml', 'package.json', '.jscpd.json'
}

ALLOWED_ROOT_DIRS = {
    'src', 'tests', 'configs', 'scripts', 'docs', 'data', 
    'metadata', 'logs', '.git', '.github', '.vscode', '.idea',
    'venv', '.venv', 'env', 'node_modules', '__pycache__',
    'build', 'dist', 'site', '.pytest_cache', '.mypy_cache',
    '.ruff_cache', '.cursor'
}

def main():
    root = Path('.')
    violations = []
    
    for item in root.iterdir():
        if item.name.startswith('.'):
            # Skip dotfiles not explicitly allowed
            if item.is_file() and item.name not in ALLOWED_ROOT_FILES:
                continue
        
        if item.is_file() and item.name not in ALLOWED_ROOT_FILES:
            violations.append(f'Unwanted file: {item.name}')
        elif item.is_dir() and item.name not in ALLOWED_ROOT_DIRS:
            violations.append(f'Unwanted directory: {item.name}')
    
    if violations:
        print('❌ ROOT CLEANLINESS VIOLATIONS:')
        for v in violations:
            print(f'   - {v}')
        print()
        print('💡 Keep root clean! Move files to appropriate directories.')
        sys.exit(1)
    
    print('✅ Root directory is clean')

if __name__ == '__main__':
    main()
