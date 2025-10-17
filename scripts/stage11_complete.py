#!/usr/bin/env python3
"""
Главный скрипт для завершения Stage 11 - финальная валидация и создание PR.

Объединяет все этапы финальной валидации:
1. Выполнение стандартных проверок (тесты, линтинг, pre-commit, docs)
2. Генерация отчёта валидации
3. Проверка чистоты Git статуса
4. Создание Pull Request

Использование:
    python scripts/stage11_complete.py [--skip-pr] [--verbose] [--dry-run]
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path


class Stage11Completer:
    """Главный класс для завершения Stage 11."""
    
    def __init__(self, verbose: bool = False, dry_run: bool = False):
        self.verbose = verbose
        self.dry_run = dry_run
        self.project_root = Path(__file__).parent.parent
    
    def log(self, message: str, level: str = "INFO") -> None:
        """Логирование с временной меткой."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        prefix = f"[{timestamp}] {level}:"
        print(f"{prefix} {message}")
    
    def run_script(self, script_path: Path, args: list | None = None) -> bool:
        """Запустить Python скрипт."""
        if args is None:
            args = []
        
        cmd = [sys.executable, str(script_path)] + args
        if self.verbose:
            cmd.append("--verbose")
        if self.dry_run:
            cmd.append("--dry-run")
        
        self.log(f"Запуск скрипта: {script_path.name}")
        
        if self.dry_run:
            self.log(f"[DRY RUN] Команда: {' '.join(cmd)}")
            return True
        
        try:
            # Безопасный запуск скриптов - все пути предопределены
            result = subprocess.run(cmd, cwd=self.project_root)
            return result.returncode == 0
        except (subprocess.SubprocessError, OSError) as e:
            self.log(f"❌ Ошибка запуска скрипта {script_path.name}: {e}", "ERROR")
            return False
    
    def run_stage11_workflow(self, skip_pr: bool = False) -> bool:
        """Запустить полный workflow Stage 11."""
        self.log("Начало завершения Stage 11 - финальная валидация")
        self.log("="*60)
        
        # Этап 1: Финальная валидация
        self.log("Этап 1: Выполнение финальной валидации...")
        validation_script = self.project_root / "scripts" / "final_validation.py"
        
        if not validation_script.exists():
            self.log(f"ОШИБКА: Скрипт валидации не найден: {validation_script}", "ERROR")
            return False
        
        validation_success = self.run_script(validation_script)
        
        if not validation_success:
            self.log("ОШИБКА: Валидация завершилась с ошибками", "ERROR")
            self.log("ВНИМАНИЕ: Исправьте проблемы перед продолжением", "WARNING")
            return False
        
        self.log("УСПЕХ: Валидация завершена успешно!")
        
        # Этап 2: Создание PR (если не пропущено)
        if not skip_pr:
            self.log("\nЭтап 2: Создание Pull Request...")
            pr_script = self.project_root / "scripts" / "create_cleanup_pr.py"
            
            if not pr_script.exists():
                self.log(f"ОШИБКА: Скрипт создания PR не найден: {pr_script}", "ERROR")
                return False
            
            pr_success = self.run_script(pr_script)
            
            if not pr_success:
                self.log("ОШИБКА: Создание PR завершилось с ошибками", "ERROR")
                return False
            
            self.log("УСПЕХ: Pull Request создан успешно!")
        else:
            self.log("\nЭтап 2: Создание PR пропущено (--skip-pr)")
        
        # Итоговая сводка
        self.log("\n" + "="*60)
        self.log("STAGE 11 ЗАВЕРШЁН УСПЕШНО!", "SUCCESS")
        self.log("="*60)
        
        if not skip_pr:
            self.log("УСПЕХ: Все проверки пройдены")
            self.log("УСПЕХ: Pull Request создан")
            self.log("УСПЕХ: Репозиторий готов к ревью и слиянию")
        else:
            self.log("УСПЕХ: Все проверки пройдены")
            self.log("УСПЕХ: Репозиторий готов к созданию PR вручную")
        
        self.log("\nСледующие шаги:")
        if not skip_pr:
            self.log("1. Проверьте созданный PR в веб-интерфейсе GitHub")
            self.log("2. Дождитесь ревью от команды")
            self.log("3. После одобрения слейте PR в основную ветку")
        else:
            self.log("1. Запустите: python scripts/create_cleanup_pr.py")
            self.log("2. Или создайте PR вручную через веб-интерфейс")
        
        self.log("\nДокументация:")
        self.log("- CLEANUP_REPORT.md - полный отчёт об очистке")
        self.log("- docs/ - обновлённая документация")
        
        return True


def main():
    """Главная функция."""
    parser = argparse.ArgumentParser(
        description="Завершение Stage 11 - финальная валидация и создание PR",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python scripts/stage11_complete.py                    # Полный workflow
  python scripts/stage11_complete.py --skip-pr          # Только валидация
  python scripts/stage11_complete.py --verbose          # Подробный вывод
  python scripts/stage11_complete.py --dry-run          # Показать что будет сделано

Этапы выполнения:
  1. Финальная валидация (тесты, линтинг, pre-commit, docs)
  2. Генерация отчёта валидации
  3. Создание Pull Request (если не пропущено)
        """
    )
    
    parser.add_argument(
        "--skip-pr",
        action="store_true",
        help="Пропустить создание PR (только валидация)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Подробный вывод команд"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Показать что будет сделано без выполнения команд"
    )
    
    args = parser.parse_args()
    
    # Создание completer и запуск workflow
    completer = Stage11Completer(verbose=args.verbose, dry_run=args.dry_run)
    
    try:
        success = completer.run_stage11_workflow(skip_pr=args.skip_pr)
        
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nПроцесс прерван пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
