#!/usr/bin/env python3
"""
Скрипт финальной валидации репозитория после очистки.

Выполняет стандартные проверки качества кода, тесты, линтинг и сборку документации
для подтверждения здоровья репозитория после cleanup операций.

Использование:
    python scripts/final_validation.py [--output-file REPORT.md] [--verbose]
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


class ValidationResult:
    """Результат выполнения команды валидации."""
    
    def __init__(self, command: str, return_code: int, stdout: str, stderr: str, duration: float):
        self.command = command
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr
        self.duration = duration
        self.success = return_code == 0
    
    def __str__(self) -> str:
        status = "✅ УСПЕХ" if self.success else "❌ ОШИБКА"
        return f"{status} | {self.command} | {self.duration:.2f}с"


class FinalValidator:
    """Основной класс для выполнения финальной валидации."""
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.project_root = Path(__file__).parent.parent
        self.results: list[ValidationResult] = []
        self.start_time = datetime.now()
    
    def log(self, message: str, level: str = "INFO") -> None:
        """Логирование с временной меткой."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        prefix = f"[{timestamp}] {level}:"
        print(f"{prefix} {message}")
    
    def run_command(self, command: list[str], cwd: Path | None = None) -> ValidationResult:
        """Выполнить команду и вернуть результат."""
        if cwd is None:
            cwd = self.project_root
        
        cmd_str = " ".join(command)
        self.log(f"Выполнение: {cmd_str}")
        
        start_time = time.time()
        
        try:
            # Безопасный запуск команд - все команды предопределены
            result = subprocess.run(
                command,
                cwd=cwd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace'
            )
            duration = time.time() - start_time
            
            validation_result = ValidationResult(
                command=cmd_str,
                return_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
                duration=duration
            )
            
            self.results.append(validation_result)
            
            if self.verbose:
                if result.stdout:
                    print("STDOUT:")
                    print(result.stdout)
                if result.stderr:
                    print("STDERR:")
                    print(result.stderr)
            
            return validation_result
            
        except (subprocess.SubprocessError, OSError) as e:
            duration = time.time() - start_time
            error_result = ValidationResult(
                command=cmd_str,
                return_code=1,
                stdout="",
                stderr=str(e),
                duration=duration
            )
            self.results.append(error_result)
            return error_result
    
    def check_git_status(self) -> ValidationResult:
        """Проверить статус Git репозитория."""
        self.log("Проверка статуса Git...")
        return self.run_command(["git", "status", "--porcelain"])
    
    def run_tests(self) -> ValidationResult:
        """Запустить тесты через Makefile."""
        self.log("Запуск тестов...")
        return self.run_command(["make", "test"])
    
    def run_lint(self) -> ValidationResult:
        """Запустить линтинг через Makefile."""
        self.log("Запуск линтинга...")
        return self.run_command(["make", "lint"])
    
    def run_type_check(self) -> ValidationResult:
        """Запустить проверку типов через Makefile."""
        self.log("Запуск проверки типов...")
        return self.run_command(["make", "type-check"])
    
    def run_pre_commit(self) -> ValidationResult:
        """Запустить pre-commit хуки."""
        self.log("Запуск pre-commit хуков...")
        return self.run_command(["pre-commit", "run", "--all-files"])
    
    def build_docs(self) -> ValidationResult:
        """Собрать документацию через mkdocs."""
        self.log("Сборка документации...")
        mkdocs_config = self.project_root / "configs" / "mkdocs.yml"
        if mkdocs_config.exists():
            return self.run_command([
                "mkdocs", "build", 
                "--config-file", str(mkdocs_config),
                "--strict"
            ])
        else:
            # Fallback на стандартную конфигурацию
            return self.run_command(["mkdocs", "build", "--strict"])
    
    def run_quality_checks(self) -> ValidationResult:
        """Запустить полные проверки качества через Makefile."""
        self.log("Запуск полных проверок качества...")
        return self.run_command(["make", "quality"])
    
    def check_repository_health(self) -> dict[str, bool]:
        """Проверить общее здоровье репозитория."""
        health_checks = {}
        
        # Проверка наличия основных файлов
        required_files = [
            "pyproject.toml",
            "Makefile", 
            ".pre-commit-config.yaml",
            "configs/mkdocs.yml",
            "src/library/__init__.py"
        ]
        
        for file_path in required_files:
            full_path = self.project_root / file_path
            health_checks[f"file_exists_{file_path}"] = full_path.exists()
        
        # Проверка размера репозитория (не должен быть слишком большим)
        try:
            result = self.run_command(["git", "count-objects", "-vH"])
            health_checks["repo_size_reasonable"] = result.success and "MB" in result.stdout
        except Exception:
            health_checks["repo_size_reasonable"] = False
        
        return health_checks
    
    def generate_report(self, output_file: Path | None = None) -> str:
        """Сгенерировать отчёт о валидации."""
        total_duration = (datetime.now() - self.start_time).total_seconds()
        
        # Подсчёт статистики
        total_checks = len(self.results)
        successful_checks = sum(1 for r in self.results if r.success)
        failed_checks = total_checks - successful_checks
        
        # Генерация отчёта
        report_lines = [
            "# Отчёт финальной валидации репозитория",
            "",
            f"**Дата**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**Статус**: {'✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ' if failed_checks == 0 else '❌ ОБНАРУЖЕНЫ ПРОБЛЕМЫ'}",
            f"**Длительность**: {total_duration:.2f} секунд",
            "",
            "## Сводка результатов",
            "",
            f"- **Всего проверок**: {total_checks}",
            f"- **Успешных**: {successful_checks} ✅",
            f"- **Неудачных**: {failed_checks} ❌",
            f"- **Процент успеха**: {(successful_checks/total_checks*100):.1f}%",
            "",
            "## Детальные результаты",
            ""
        ]
        
        # Детальные результаты
        for i, result in enumerate(self.results, 1):
            status_icon = "✅" if result.success else "❌"
            report_lines.extend([
                f"### {i}. {result.command}",
                "",
                f"- **Статус**: {status_icon} {'Успех' if result.success else 'Ошибка'}",
                f"- **Код возврата**: {result.return_code}",
                f"- **Длительность**: {result.duration:.2f}с",
                ""
            ])
            
            if result.stdout:
                report_lines.extend([
                    "**Вывод (stdout):**",
                    "```",
                    result.stdout[:1000] + ("..." if len(result.stdout) > 1000 else ""),
                    "```",
                    ""
                ])
            
            if result.stderr:
                report_lines.extend([
                    "**Ошибки (stderr):**",
                    "```",
                    result.stderr[:1000] + ("..." if len(result.stderr) > 1000 else ""),
                    "```",
                    ""
                ])
        
        # Проверки здоровья репозитория
        health_checks = self.check_repository_health()
        report_lines.extend([
            "## Проверки здоровья репозитория",
            ""
        ])
        
        for check_name, passed in health_checks.items():
            status_icon = "✅" if passed else "❌"
            check_display = check_name.replace("_", " ").title()
            report_lines.append(f"- {status_icon} {check_display}")
        
        # Рекомендации
        report_lines.extend([
            "",
            "## Рекомендации",
            ""
        ])
        
        if failed_checks == 0:
            report_lines.extend([
                "🎉 **Все проверки пройдены успешно!**",
                "",
                "Репозиторий готов к:",
                "- Созданию Pull Request",
                "- Слиянию в основную ветку", 
                "- Развёртыванию в продакшн",
                ""
            ])
        else:
            report_lines.extend([
                "⚠️ **Обнаружены проблемы, требующие исправления:**",
                ""
            ])
            
            for result in self.results:
                if not result.success:
                    report_lines.append(f"- Исправить ошибки в: `{result.command}`")
            
            report_lines.extend([
                "",
                "После исправления проблем запустите валидацию повторно:",
                "```bash",
                "python scripts/final_validation.py",
                "```"
            ])
        
        # Git статус
        git_status_result = self.check_git_status()
        if git_status_result.success and git_status_result.stdout.strip():
            report_lines.extend([
                "",
                "## Git статус",
                "",
                "⚠️ **Обнаружены незакоммиченные изменения:**",
                "```",
                git_status_result.stdout,
                "```",
                "",
                "Рекомендуется закоммитить все изменения перед созданием PR."
            ])
        else:
            report_lines.extend([
                "",
                "## Git статус",
                "",
                "✅ Рабочая директория чистая, готово к коммиту."
            ])
        
        report_content = "\n".join(report_lines)
        
        # Сохранение в файл
        if output_file:
            output_file.write_text(report_content, encoding='utf-8')
            self.log(f"Отчёт сохранён в: {output_file}")
        
        return report_content
    
    def run_all_validations(self) -> bool:
        """Запустить все проверки валидации."""
        self.log("Начало финальной валидации репозитория...")
        
        # Основные проверки
        validations = [
            ("Тесты", self.run_tests),
            ("Линтинг", self.run_lint), 
            ("Проверка типов", self.run_type_check),
            ("Pre-commit хуки", self.run_pre_commit),
            ("Сборка документации", self.build_docs),
        ]
        
        for name, validation_func in validations:
            self.log(f"Выполнение: {name}")
            result = validation_func()
            
            if not result.success:
                self.log(f"ОШИБКА: {name} завершился с ошибкой", "ERROR")
                if not self.verbose:
                    print(f"STDERR: {result.stderr}")
            else:
                self.log(f"УСПЕХ: {name} выполнен успешно")
        
        # Итоговая сводка
        total_checks = len(self.results)
        successful_checks = sum(1 for r in self.results if r.success)
        failed_checks = total_checks - successful_checks
        
        self.log(f"Итого: {successful_checks}/{total_checks} проверок пройдено")
        
        if failed_checks == 0:
            self.log("УСПЕХ: Все проверки пройдены! Репозиторий готов к PR.", "SUCCESS")
            return True
        else:
            self.log(f"ВНИМАНИЕ: Обнаружено {failed_checks} проблем. Требуется исправление.", "WARNING")
            return False


def main():
    """Главная функция."""
    parser = argparse.ArgumentParser(
        description="Финальная валидация репозитория после очистки",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python scripts/final_validation.py
  python scripts/final_validation.py --verbose
  python scripts/final_validation.py --output-file validation_report.md
        """
    )
    
    parser.add_argument(
        "--output-file", "-o",
        type=Path,
        help="Файл для сохранения отчёта (по умолчанию: CLEANUP_REPORT.md)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Подробный вывод команд"
    )
    
    args = parser.parse_args()
    
    # Определение файла отчёта
    if args.output_file:
        output_file = args.output_file
    else:
        project_root = Path(__file__).parent.parent
        output_file = project_root / "CLEANUP_REPORT.md"
    
    # Создание валидатора и запуск проверок
    validator = FinalValidator(verbose=args.verbose)
    
    try:
        success = validator.run_all_validations()
        
        # Генерация отчёта
        validator.generate_report(output_file)
        
        # Вывод краткой сводки
        print("\n" + "="*60)
        print("КРАТКАЯ СВОДКА ВАЛИДАЦИИ")
        print("="*60)
        
        for result in validator.results:
            print(result)
        
        print("="*60)
        
        if success:
            print("ВАЛИДАЦИЯ ЗАВЕРШЕНА УСПЕШНО!")
            print("УСПЕХ: Репозиторий готов к созданию Pull Request")
            sys.exit(0)
        else:
            print("ВАЛИДАЦИЯ ЗАВЕРШЕНА С ОШИБКАМИ!")
            print("ВНИМАНИЕ: Требуется исправление проблем перед созданием PR")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nВалидация прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
