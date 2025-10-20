#!/usr/bin/env bash
set -euo pipefail

DRY=1
VERBOSE=0

if [[ "${1:-}" == "--apply" ]]; then
  DRY=0
elif [[ "${1:-}" == "--verbose" ]]; then
  VERBOSE=1
elif [[ "${1:-}" == "--help" ]]; then
  echo "Usage: $0 [--apply] [--verbose]" >&2
  exit 0
fi

shift || true
for arg in "$@"; do
  if [[ "$arg" == "--apply" ]]; then DRY=0; fi
  if [[ "$arg" == "--verbose" ]]; then VERBOSE=1; fi
done

[[ $VERBOSE -eq 1 ]] && echo "[clean] DRY=$DRY VERBOSE=$VERBOSE"

protect_core() {
  local p="$1"
  # Защита: не трогаем tests/golden/** и корневые манифесты
  if [[ "$p" =~ ^\./tests/golden/ ]]; then return 1; fi
  # Не трогаем ключевые каталоги проекта и .git
  if [[ "$p" =~ ^\./\.git/ ]]; then return 1; fi
  if [[ "$p" =~ ^\./src/ ]]; then return 1; fi
  if [[ "$p" =~ ^\./tests/ ]]; then return 1; fi
  if [[ "$p" =~ ^\./docs/ ]]; then return 1; fi
  if [[ "$p" =~ ^\./configs/ ]]; then return 1; fi
  if [[ "$p" =~ ^\./\.github/ ]]; then return 1; fi
  case "$p" in
    ./*.md|./*.yml|./*.yaml|./*.toml|./*.ini) return 1;;
  esac
  # Явно сохраняем пример окружения
  if [[ "$p" == "./.env.example" ]]; then return 1; fi
  return 0
}

CANDIDATES=(
  "__pycache__" ".pytest_cache" ".mypy_cache" ".ruff_cache" ".ipynb_checkpoints" ".cache"
  "build" "dist" "wheels" ".eggs" "*.egg-info" "pip-wheel-metadata"
  ".tox" ".nox" ".hypothesis" ".coverage*" "coverage.xml" "htmlcov"
  ".pyre" ".pytype"
  ".venv" "venv" "env" ".env" ".env.*"
  ".idea" ".vscode" ".DS_Store" "Thumbs.db" "*.swp" "*.swo" "*~" "*.bak" "*.tmp" "*.orig"
  "logs" "*.log"
  "data/output"
)

exit_code=0

for pattern in "${CANDIDATES[@]}"; do
  # ограничим глубину до 2 для корневых мусорных паттернов; для выводов/логов допускается глубже
  if [[ "$pattern" == "data/output" ]]; then
    mapfile -t paths < <(find . -path "./.git" -prune -o -path "./data/output/*" -print 2>/dev/null || true)
  else
    mapfile -t paths < <(find . -path "./.git" -prune -o -maxdepth 2 -name "$pattern" -print 2>/dev/null || true)
  fi

  for p in "${paths[@]:-}"; do
    [[ -z "$p" ]] && continue
    if ! protect_core "$p"; then
      [[ $VERBOSE -eq 1 ]] && echo "[clean] skip protected $p"
      continue
    fi
    echo "[clean] remove $p"
    if [[ $DRY -eq 0 ]]; then
      rm -rf -- "$p" || { echo "[clean] failed to remove $p" >&2; exit_code=2; }
    fi
  done
done

exit $exit_code


