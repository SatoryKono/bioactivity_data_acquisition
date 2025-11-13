import tomllib
from pathlib import Path
path = Path('pyproject.toml')
try:
    tomllib.loads(path.read_text(encoding='utf-8'))
    print('ok')
except Exception as exc:
    print('error', exc)
