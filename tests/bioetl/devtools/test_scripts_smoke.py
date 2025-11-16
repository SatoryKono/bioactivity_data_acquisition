from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _filter_debugger_warnings(text: str) -> str:
    """Удаляет предупреждения debugger'а и pandas из вывода subprocess."""
    lines = text.splitlines()
    filtered_lines = []
    
    for line in lines:
        # Пропускаем строки с предупреждениями debugger'а
        # Предупреждения могут начинаться с временных меток (например, "0.02s - Debugger warning")
        line_lower = line.lower()
        line_stripped = line.strip()
        
        # Пропускаем строки с временными метками и предупреждениями debugger'а
        if any(
            keyword in line_lower
            for keyword in [
                "debugger warning",
                "frozen modules",
                "pydevd_disable_file_validation",
                "make the debugger miss breakpoints",
                "note: debugging will proceed",
                "to python to disable frozen modules",
                "userwarning: the numpy module was reloaded",
                "pandas/__init__.py",
            ]
        ):
            continue
        
        # Пропускаем строки, которые начинаются с временных меток (например, "0.02s -")
        if line_stripped and line_stripped[0].isdigit() and "s -" in line_stripped:
            continue
        
        # Пропускаем строки, которые являются частью предупреждений pandas
        if line_stripped.startswith("__import__") or line_stripped.startswith("_dependency"):
            continue
        
        filtered_lines.append(line)
    
    return "\n".join(filtered_lines)


def _run_script(script: str, *extra: str) -> subprocess.CompletedProcess[str]:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / f"{script}.py"
    env = os.environ.copy()
    env["DISABLE_PANDERA_IMPORT_WARNING"] = "True"
    env["PYTHONWARNINGS"] = "ignore"
    env["PYTHONUNBUFFERED"] = "1"
    env["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"
    env["NO_COLOR"] = "1"
    env["FORCE_COLOR"] = "0"
    env["TERM"] = "dumb"
    return subprocess.run(
        [sys.executable, str(script_path), *extra],
        cwd=repo_root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        env=env,
    )


def test_determinism_check_help() -> None:
    """Проверяет, что скрипт determinism_check возвращает корректный help."""
    result = _run_script("determinism_check", "--help")
    assert result.returncode == 0, f"Script failed with stderr: {result.stderr}"
    
    # Typer выводит help в stderr, но в stdout могут быть логи
    # Проверяем оба потока
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    combined_output = stdout + stderr
    
    assert combined_output, (
        f"Both stdout and stderr are empty before filtering. "
        f"returncode={result.returncode}, "
        f"stdout length={len(stdout)}, "
        f"stderr length={len(stderr)}. "
        f"This may indicate a subprocess capture issue."
    )
    
    # Фильтруем предупреждения отладчика и pandas
    filtered_output = _filter_debugger_warnings(combined_output)
    
    assert filtered_output, (
        f"Output is empty after filtering debugger warnings. "
        f"returncode={result.returncode}, "
        f"stdout length={len(stdout)}, "
        f"stderr length={len(stderr)}, "
        f"filtered length={len(filtered_output)}, "
        f"original output preview (first 500 chars): {combined_output[:500]}"
    )
    
    # Проверяем наличие ключевых слов в отфильтрованном выводе
    preview_length = min(1000, len(filtered_output))
    assert "determinism" in filtered_output.lower() or "--pipeline" in filtered_output, (
        f"Expected 'determinism' or '--pipeline' in filtered output. "
        f"Output preview ({preview_length} chars): {filtered_output[:preview_length]}"
    )
    assert "Usage:" in filtered_output or "--pipeline" in filtered_output, (
        f"Expected 'Usage:' or '--pipeline' in filtered output. "
        f"Output preview ({preview_length} chars): {filtered_output[:preview_length]}"
    )


def test_schema_guard_help() -> None:
    """Проверяет, что скрипт schema_guard возвращает корректный help."""
    result = _run_script("schema_guard", "--help")
    assert result.returncode == 0, f"Script failed with stderr: {result.stderr}"
    
    # Typer выводит help в stderr, но в stdout могут быть логи
    # Проверяем оба потока
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    combined_output = stdout + stderr
    
    assert combined_output, (
        f"Both stdout and stderr are empty before filtering. "
        f"returncode={result.returncode}, "
        f"stdout length={len(stdout)}, "
        f"stderr length={len(stderr)}. "
        f"This may indicate a subprocess capture issue."
    )
    
    # Фильтруем предупреждения отладчика и pandas
    filtered_output = _filter_debugger_warnings(combined_output)
    
    assert filtered_output, (
        f"Output is empty after filtering debugger warnings. "
        f"returncode={result.returncode}, "
        f"stdout length={len(stdout)}, "
        f"stderr length={len(stderr)}, "
        f"filtered length={len(filtered_output)}, "
        f"original output preview (first 500 chars): {combined_output[:500]}"
    )
    
    # Проверяем наличие ключевых слов в отфильтрованном выводе
    output_lower = filtered_output.lower()
    has_schema_keyword = "schema" in output_lower or "pandera" in output_lower
    has_usage = "Usage:" in filtered_output
    
    preview_length = min(1000, len(filtered_output))
    
    assert has_schema_keyword, (
        f"Expected 'schema' or 'pandera' in filtered output. "
        f"Output preview ({preview_length} chars): {filtered_output[:preview_length]}"
    )
    assert has_usage, (
        f"Expected 'Usage:' in filtered output. "
        f"Output preview ({preview_length} chars): {filtered_output[:preview_length]}"
    )


def test_vocab_audit_help() -> None:
    """Проверяет, что скрипт vocab_audit возвращает корректный help."""
    result = _run_script("vocab_audit", "--help")
    assert result.returncode == 0, f"Script failed with stderr: {result.stderr}"
    
    # Typer выводит help в stderr, но в stdout могут быть логи
    # Проверяем оба потока
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    combined_output = stdout + stderr
    
    assert combined_output, (
        f"Both stdout and stderr are empty before filtering. "
        f"returncode={result.returncode}, "
        f"stdout length={len(stdout)}, "
        f"stderr length={len(stderr)}. "
        f"This may indicate a subprocess capture issue."
    )
    
    # Фильтруем предупреждения отладчика и pandas
    filtered_output = _filter_debugger_warnings(combined_output)
    
    assert filtered_output, (
        f"Output is empty after filtering debugger warnings. "
        f"returncode={result.returncode}, "
        f"stdout length={len(stdout)}, "
        f"stderr length={len(stderr)}, "
        f"filtered length={len(filtered_output)}, "
        f"original output preview (first 500 chars): {combined_output[:500]}"
    )
    
    preview_length = min(1000, len(filtered_output))
    assert "vocab" in filtered_output.lower() or "audit" in filtered_output.lower(), (
        f"Expected 'vocab' or 'audit' in filtered output. "
        f"Output preview ({preview_length} chars): {filtered_output[:preview_length]}"
    )
    assert "Usage:" in filtered_output or "--store" in filtered_output, (
        f"Expected 'Usage:' or '--store' in filtered output. "
        f"Output preview ({preview_length} chars): {filtered_output[:preview_length]}"
    )

