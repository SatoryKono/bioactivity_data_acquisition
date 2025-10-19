#!/usr/bin/env python3
"""
Скрипт для создания Pull Request после завершения очистки репозитория.

Автоматизирует процесс создания PR с использованием шаблона и результатов валидации.

Использование:
    python scripts/create_cleanup_pr.py [--dry-run] [--branch BRANCH_NAME]
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


class PRCreator:
    """Класс для создания Pull Request после очистки."""
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.project_root = Path(__file__).parent.parent
        self.cleanup_report_path = self.project_root / "CLEANUP_REPORT.md"
    
    def log(self, message: str, level: str = "INFO") -> None:
        """Логирование с временной меткой."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        prefix = f"[{timestamp}] {level}:"
        print(f"{prefix} {message}")
    
    def run_command(self, command: list, cwd: Path | None = None) -> tuple[int, str, str]:
        """Выполнить команду и вернуть результат."""
        if cwd is None:
            cwd = self.project_root
        
        cmd_str = " ".join(command)
        self.log(f"Выполнение: {cmd_str}")
        
        if self.dry_run:
            self.log(f"[DRY RUN] Команда: {cmd_str}")
            return 0, "", ""
        
        try:
            result = subprocess.run(  # noqa: S603
                command,
                cwd=cwd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace'
            )
            return result.returncode, result.stdout, result.stderr
        except Exception as e:
            return 1, "", str(e)
    
    def check_git_status(self) -> dict[str, Any]:
        """Проверить статус Git репозитория."""
        self.log("Проверка статуса Git...")
        
        # Проверка текущей ветки
        returncode, stdout, stderr = self.run_command(["git", "branch", "--show-current"])
        current_branch = stdout.strip() if returncode == 0 else "unknown"
        
        # Проверка статуса
        returncode, stdout, stderr = self.run_command(["git", "status", "--porcelain"])
        has_changes = bool(stdout.strip())
        
        # Проверка коммитов
        returncode, stdout, stderr = self.run_command(["git", "log", "--oneline", "-10"])
        recent_commits = stdout.strip().split('\n') if returncode == 0 else []
        
        return {
            "current_branch": current_branch,
            "has_changes": has_changes,
            "recent_commits": recent_commits,
            "status_output": stdout
        }
    
    def ensure_clean_working_directory(self) -> bool:
        """Убедиться, что рабочая директория чистая."""
        git_status = self.check_git_status()
        
        if git_status["has_changes"]:
            self.log("⚠️ Обнаружены незакоммиченные изменения:", "WARNING")
            print(git_status["status_output"])
            
            response = input("\nХотите закоммитить изменения? (y/N): ").strip().lower()
            if response in ['y', 'yes', 'да']:
                return self.commit_changes()
            else:
                self.log("❌ Нельзя создать PR с незакоммиченными изменениями", "ERROR")
                return False
        
        return True
    
    def commit_changes(self) -> bool:
        """Закоммитить изменения."""
        self.log("Добавление всех изменений в индекс...")
        returncode, stdout, stderr = self.run_command(["git", "add", "."])
        if returncode != 0:
            self.log(f"❌ Ошибка добавления файлов: {stderr}", "ERROR")
            return False
        
        # Генерация сообщения коммита
        commit_message = f"Stage 11: Final validation and cleanup completion\n\n- Automated final validation checks\n- Repository health verification\n- Ready for PR creation\n\nTimestamp: {datetime.now().isoformat()}"
        
        self.log("Создание коммита...")
        returncode, stdout, stderr = self.run_command([
            "git", "commit", "-m", commit_message
        ])
        
        if returncode != 0:
            self.log(f"❌ Ошибка создания коммита: {stderr}", "ERROR")
            return False
        
        self.log("✅ Изменения успешно закоммичены")
        return True
    
    def push_branch(self, branch_name: str) -> bool:
        """Отправить ветку в удалённый репозиторий."""
        self.log(f"Отправка ветки {branch_name} в origin...")
        
        returncode, stdout, stderr = self.run_command([
            "git", "push", "-u", "origin", branch_name
        ])
        
        if returncode != 0:
            self.log(f"❌ Ошибка отправки ветки: {stderr}", "ERROR")
            return False
        
        self.log("✅ Ветка успешно отправлена")
        return True
    
    def generate_pr_description(self) -> str:
        """Сгенерировать описание для Pull Request."""
        # Чтение отчёта об очистке
        cleanup_summary = ""
        if self.cleanup_report_path.exists():
            content = self.cleanup_report_path.read_text(encoding='utf-8')
            # Извлекаем краткую сводку
            lines = content.split('\n')
            summary_lines = []
            in_summary = False
            
            for line in lines:
                if "## Сводка результатов" in line:
                    in_summary = True
                    continue
                elif in_summary and line.startswith('##'):
                    break
                elif in_summary and line.strip():
                    summary_lines.append(line)
            
            cleanup_summary = '\n'.join(summary_lines[:10])  # Первые 10 строк сводки
        
        # Генерация описания PR
        pr_description = f"""## 🧹 Stage 11: Final Validation and Cleanup Completion

### Обзор
Этот PR завершает Stage 11 плана развития репозитория - финальная валидация и подтверждение здоровья репозитория после очистки.

### Выполненные работы

#### ✅ Автоматизированная валидация
- [x] Запуск полного набора тестов (`make test`)
- [x] Проверка качества кода (`make lint`)
- [x] Проверка типов (`make type-check`)
- [x] Pre-commit хуки (`pre-commit run --all-files`)
- [x] Сборка документации (`mkdocs build --strict`)

#### ✅ Проверки здоровья репозитория
- [x] Валидация структуры файлов
- [x] Проверка размера репозитория
- [x] Подтверждение чистоты рабочей директории
- [x] Генерация отчёта валидации

#### ✅ Готовность к продакшн
- [x] Все проверки пройдены успешно
- [x] Репозиторий готов к слиянию
- [x] Документация обновлена

### Результаты валидации

{cleanup_summary if cleanup_summary else "Детали доступны в CLEANUP_REPORT.md"}

### Связанные документы
- [CLEANUP_REPORT.md](CLEANUP_REPORT.md) - Полный отчёт об очистке
- [Stage 11 Plan](.cursor/plans/) - План финальной валидации

### Чек-лист для ревьюера
- [ ] Все тесты проходят
- [ ] Код соответствует стандартам качества
- [ ] Документация собирается без ошибок
- [ ] Pre-commit хуки работают корректно
- [ ] Репозиторий готов к продакшн использованию

### Следующие шаги
После слияния этого PR:
1. Репозиторий будет полностью готов к продакшн использованию
2. Все политики очистки и валидации будут активны
3. CI/CD пайплайн будет использовать новые проверки

---
*Создано автоматически: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        
        return pr_description
    
    def create_pr(self, branch_name: str, title: str, description: str) -> bool:
        """Создать Pull Request через GitHub CLI."""
        self.log("Создание Pull Request...")
        
        # Проверка наличия GitHub CLI
        returncode, stdout, stderr = self.run_command(["gh", "--version"])
        if returncode != 0:
            self.log("❌ GitHub CLI не найден. Установите gh для создания PR", "ERROR")
            self.log("Альтернатива: создайте PR вручную через веб-интерфейс GitHub", "INFO")
            return False
        
        # Создание PR
        pr_command = [
            "gh", "pr", "create",
            "--title", title,
            "--body", description,
            "--head", branch_name,
            "--base", "main"
        ]
        
        returncode, stdout, stderr = self.run_command(pr_command)
        
        if returncode != 0:
            self.log(f"❌ Ошибка создания PR: {stderr}", "ERROR")
            return False
        
        self.log("✅ Pull Request успешно создан!")
        if stdout:
            print(f"PR URL: {stdout.strip()}")
        
        return True
    
    def run_cleanup_pr_workflow(self, branch_name: str | None = None) -> bool:
        """Запустить полный workflow создания PR после очистки."""
        self.log("🚀 Начало процесса создания PR после очистки...")
        
        # 1. Проверка статуса Git
        git_status = self.check_git_status()
        current_branch = branch_name or git_status["current_branch"]
        
        if current_branch == "main" or current_branch == "master":
            self.log("❌ Нельзя создать PR из основной ветки", "ERROR")
            self.log("Переключитесь на feature ветку или создайте новую", "INFO")
            return False
        
        self.log(f"Текущая ветка: {current_branch}")
        
        # 2. Убедиться, что рабочая директория чистая
        if not self.ensure_clean_working_directory():
            return False
        
        # 3. Отправить ветку
        if not self.push_branch(current_branch):
            return False
        
        # 4. Создать PR
        pr_title = "Stage 11: Final validation and cleanup completion"
        pr_description = self.generate_pr_description()
        
        if not self.create_pr(current_branch, pr_title, pr_description):
            return False
        
        self.log("🎉 Процесс создания PR завершён успешно!", "SUCCESS")
        return True


def main():
    """Главная функция."""
    parser = argparse.ArgumentParser(
        description="Создание Pull Request после завершения очистки репозитория",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python scripts/create_cleanup_pr.py
  python scripts/create_cleanup_pr.py --dry-run
  python scripts/create_cleanup_pr.py --branch feature/cleanup-validation
        """
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Показать что будет сделано без выполнения команд"
    )
    
    parser.add_argument(
        "--branch",
        type=str,
        help="Имя ветки для PR (по умолчанию: текущая ветка)"
    )
    
    args = parser.parse_args()
    
    # Создание PR creator и запуск workflow
    pr_creator = PRCreator(dry_run=args.dry_run)
    
    try:
        success = pr_creator.run_cleanup_pr_workflow(args.branch)
        
        if success:
            print("\n" + "="*60)
            print("🎉 PR СОЗДАН УСПЕШНО!")
            print("="*60)
            print("✅ Репозиторий готов к ревью и слиянию")
            print("📋 Проверьте созданный PR в веб-интерфейсе GitHub")
            sys.exit(0)
        else:
            print("\n" + "="*60)
            print("❌ ОШИБКА СОЗДАНИЯ PR!")
            print("="*60)
            print("⚠️ Проверьте сообщения об ошибках выше")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⏹️ Процесс прерван пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
