#!/usr/bin/env python3
"""Script to check version consistency across the project."""

import re
import sys
from pathlib import Path


def get_version_from_pyproject() -> str:
    """Extract version from configs/pyproject.toml."""
    pyproject_path = Path("configs/pyproject.toml")
    if not pyproject_path.exists():
        raise FileNotFoundError("configs/pyproject.toml not found")
    
    content = pyproject_path.read_text()
    match = re.search(r'version\s*=\s*["\']([^"\']+)["\']', content)
    if not match:
        raise ValueError("Version not found in configs/pyproject.toml")
    
    return match.group(1)


def get_version_from_cli() -> str:
    """Extract version from CLI version command."""
    try:
        from importlib.metadata import version
        return version("bioactivity-data-acquisition")
    except Exception:
        # Fallback to hardcoded version
        cli_path = Path("src/library/cli/__init__.py")
        content = cli_path.read_text()
        match = re.search(r'"bioactivity-data-acquisition\s+([^"]+)"', content)
        if match:
            return match.group(1)
        raise ValueError("Version not found in CLI")


def get_version_from_readme() -> str:
    """Extract version from README.md."""
    readme_path = Path("README.md")
    if not readme_path.exists():
        raise FileNotFoundError("README.md not found")
    
    content = readme_path.read_text()
    match = re.search(r'bioactivity-data-acquisition\s+([0-9]+\.[0-9]+\.[0-9]+)', content)
    if not match:
        raise ValueError("Version not found in README.md")
    
    return match.group(1)


def main():
    """Check version consistency across the project."""
    try:
        pyproject_version = get_version_from_pyproject()
        cli_version = get_version_from_cli()
        readme_version = get_version_from_readme()
        
        versions = {
            "configs/pyproject.toml": pyproject_version,
            "CLI": cli_version,
            "README.md": readme_version,
        }
        
        # Check if all versions are the same
        unique_versions = set(versions.values())
        if len(unique_versions) == 1:
            print(f"✅ Version consistency check passed: {pyproject_version}")
            return 0
        else:
            print("❌ Version consistency check failed:")
            for location, version in versions.items():
                print(f"  {location}: {version}")
            return 1
            
    except Exception as e:
        print(f"❌ Version consistency check failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
