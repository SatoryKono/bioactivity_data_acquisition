from __future__ import annotations

from pathlib import Path

import pytest

from tests.support.cli_runner import run_cli_command

ROOT = Path(__file__).resolve().parents[3]


def _filter_debugger_warnings(text: str) -> str:
    """Удаляет предупреждения debugger'а из вывода subprocess."""
    lines = text.splitlines()
    filtered_lines = []
    skip_next = False
    
    for line in lines:
        # Пропускаем строки с предупреждениями debugger'а
        # Предупреждения могут начинаться с временных меток (например, "0.02s - Debugger warning")
        line_lower = line.lower()
        if any(
            keyword in line_lower
            for keyword in [
                "debugger warning",
                "frozen modules",
                "pydevd_disable_file_validation",
                "make the debugger miss breakpoints",
                "note: debugging will proceed",
                "to python to disable frozen modules",
            ]
        ):
            continue
        
        # Пропускаем пустые строки, которые идут сразу после предупреждений
        if not line.strip() and skip_next:
            skip_next = False
            continue
        
        filtered_lines.append(line)
        skip_next = False
    
    return "\n".join(filtered_lines)


@pytest.mark.integration
def test_cli_help_subprocess() -> None:
    """Проверяет, что CLI возвращает корректный help с командами."""
    # Подавляем предупреждения pandera и включаем unbuffered режим для корректного захвата вывода
    # Отключаем цветной вывод для совместимости с subprocess
    result = run_cli_command(
        ["--help"],
        cwd=ROOT,
        timeout=30.0,
        extra_env={
            "PYTHONWARNINGS": "ignore",
            "DISABLE_PANDERA_IMPORT_WARNING": "True",
            "PYTHONUNBUFFERED": "1",
            "PYDEVD_DISABLE_FILE_VALIDATION": "1",
            "NO_COLOR": "1",
            "FORCE_COLOR": "0",
            "TERM": "dumb",
        },
    )
    assert result.returncode == 0, f"CLI failed with stderr: {result.stderr}"
    
    # Безопасная обработка stdout (может быть None или пустым в некоторых окружениях)
    # В некоторых случаях вывод может быть в stderr, поэтому проверяем оба потока
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    
    # Объединяем stdout и stderr для проверки (как в других тестах проекта)
    output = stdout + stderr
    
    # Если вывод пустой до фильтрации, это проблема с захватом subprocess
    assert output, (
        f"Both stdout and stderr are empty before filtering. "
        f"returncode={result.returncode}, "
        f"stdout length={len(stdout)}, "
        f"stderr length={len(stderr)}. "
        f"This may indicate a subprocess capture issue."
    )
    
    # Фильтруем предупреждения отладчика
    filtered_output = _filter_debugger_warnings(output)
    
    assert filtered_output, (
        f"Output is empty after filtering debugger warnings. "
        f"returncode={result.returncode}, "
        f"stdout length={len(stdout)}, "
        f"stderr length={len(stderr)}, "
        f"filtered length={len(filtered_output)}, "
        f"original output preview (first 500 chars): {output[:500]}"
    )
    # Ищем ключевые слова в отфильтрованном выводе
    preview_length = min(1000, len(filtered_output))
    assert "activity_chembl" in filtered_output, (
        f"Expected 'activity_chembl' in filtered output. "
        f"Output preview ({preview_length} chars): {filtered_output[:preview_length]}"
    )



@pytest.mark.integration
def test_cli_list_subprocess() -> None:
    """Проверяет, что CLI команда list возвращает список доступных пайплайнов."""
    # Подавляем предупреждения pandera и включаем unbuffered режим для корректного захвата вывода
    # Отключаем цветной вывод для совместимости с subprocess
    result = run_cli_command(
        ["list"],
        cwd=ROOT,
        timeout=30.0,
        extra_env={
            "PYTHONWARNINGS": "ignore",
            "DISABLE_PANDERA_IMPORT_WARNING": "True",
            "PYTHONUNBUFFERED": "1",
            "PYDEVD_DISABLE_FILE_VALIDATION": "1",
            "NO_COLOR": "1",
            "FORCE_COLOR": "0",
            "TERM": "dumb",
        },
    )
    assert result.returncode == 0, f"CLI failed with stderr: {result.stderr}"
    
    # Безопасная обработка stdout (может быть None или пустым в некоторых окружениях)
    # В некоторых случаях вывод может быть в stderr, поэтому проверяем оба потока
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    
    # Объединяем stdout и stderr для проверки (как в других тестах проекта)
    output = stdout + stderr
    
    # Если вывод пустой до фильтрации, это проблема с захватом subprocess
    assert output, (
        f"Both stdout and stderr are empty before filtering. "
        f"returncode={result.returncode}, "
        f"stdout length={len(stdout)}, "
        f"stderr length={len(stderr)}. "
        f"This may indicate a subprocess capture issue."
    )
    
    # Фильтруем предупреждения отладчика
    filtered_output = _filter_debugger_warnings(output)
    
    assert filtered_output, (
        f"Output is empty after filtering debugger warnings. "
        f"returncode={result.returncode}, "
        f"stdout length={len(stdout)}, "
        f"stderr length={len(stderr)}, "
        f"filtered length={len(filtered_output)}, "
        f"original output preview (first 500 chars): {output[:500]}"
    )
    # Ищем ключевые слова в отфильтрованном выводе
    preview_length = min(1000, len(filtered_output))
    assert "activity_chembl" in filtered_output, (
        f"Expected 'activity_chembl' in filtered output. "
        f"Output preview ({preview_length} chars): {filtered_output[:preview_length]}"
    )

