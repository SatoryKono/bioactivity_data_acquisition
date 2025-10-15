#!/usr/bin/env python3
"""Скрипт для переключения конфигурации Semantic Scholar API.

Этот скрипт позволяет временно отключить Semantic Scholar API
из-за строгих лимитов rate limiting.
"""

import shutil
from pathlib import Path


def toggle_semantic_scholar(enable: bool = True) -> bool:
    """Переключает Semantic Scholar API в конфигурации."""
    
    # Получаем путь к корню проекта (на 3 уровня выше)
    project_root = Path(__file__).parent.parent.parent.parent
    config_dir = project_root / "configs"
    main_config = config_dir / "config.yaml"
    backup_config = config_dir / "config_with_semantic_scholar.yaml"
    no_scholar_config = config_dir / "config_no_semantic_scholar.yaml"
    
    if enable:
        # Включаем Semantic Scholar
        if backup_config.exists():
            shutil.copy2(backup_config, main_config)
            print("[OK] Semantic Scholar API включен")
            return True
        else:
            print("[ERROR] Резервная копия конфигурации с Semantic Scholar не найдена")
            print("Создайте резервную копию: scripts/toggle_semantic_scholar.py --backup")
            return False
    else:
        # Отключаем Semantic Scholar
        if main_config.exists():
            # Создаем резервную копию текущей конфигурации
            shutil.copy2(main_config, backup_config)
            print(f"[BACKUP] Создана резервная копия: {backup_config}")
        
        if no_scholar_config.exists():
            shutil.copy2(no_scholar_config, main_config)
            print("[OK] Semantic Scholar API отключен")
            return True
        else:
            print("[ERROR] Конфигурация без Semantic Scholar не найдена")
            return False


def show_status():
    """Показывает текущий статус конфигурации."""
    
    # Получаем путь к корню проекта (на 3 уровня выше)
    project_root = Path(__file__).parent.parent.parent.parent
    config_dir = project_root / "configs"
    main_config = config_dir / "config.yaml"
    backup_config = config_dir / "config_with_semantic_scholar.yaml"
    no_scholar_config = config_dir / "config_no_semantic_scholar.yaml"
    
    print("[STATUS] Статус конфигурации:")
    print(f"Основная конфигурация: {'[OK]' if main_config.exists() else '[MISSING]'}")
    print(f"Резервная копия: {'[OK]' if backup_config.exists() else '[MISSING]'}")
    print(f"Конфигурация без Semantic Scholar: {'[OK]' if no_scholar_config.exists() else '[MISSING]'}")
    
    if main_config.exists():
        try:
            with open(main_config, 'r', encoding='utf-8') as f:
                content = f.read()
                # Проверяем, есть ли незакомментированная секция semantic_scholar
                lines = content.split('\n')
                semantic_enabled = False
                for line in lines:
                    stripped = line.strip()
                    if stripped.startswith('semantic_scholar:'):
                        semantic_enabled = True
                        break
                    elif stripped.startswith('# semantic_scholar:'):
                        semantic_enabled = False
                        break
                
                if semantic_enabled:
                    print("Semantic Scholar API: [ENABLED]")
                else:
                    print("Semantic Scholar API: [DISABLED]")
        except Exception as e:
            print(f"[ERROR] Не удалось прочитать конфигурацию: {e}")


def main():
    """Основная функция."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Переключение Semantic Scholar API")
    parser.add_argument("--enable", action="store_true", help="Включить Semantic Scholar API")
    parser.add_argument("--disable", action="store_true", help="Отключить Semantic Scholar API")
    parser.add_argument("--status", action="store_true", help="Показать статус конфигурации")
    parser.add_argument("--backup", action="store_true", help="Создать резервную копию текущей конфигурации")
    
    args = parser.parse_args()
    
    print("[INFO] Управление конфигурацией Semantic Scholar API")
    print("=" * 60)
    
    if args.status:
        show_status()
        return
    
    if args.backup:
        project_root = Path(__file__).parent.parent.parent.parent
        config_dir = project_root / "configs"
        main_config = config_dir / "config.yaml"
        backup_config = config_dir / "config_with_semantic_scholar.yaml"
        
        if main_config.exists():
            shutil.copy2(main_config, backup_config)
            print(f"[OK] Создана резервная копия: {backup_config}")
        else:
            print("[ERROR] Основная конфигурация не найдена")
        return
    
    if args.enable and args.disable:
        print("[ERROR] Нельзя одновременно включить и отключить API")
        return
    
    if not args.enable and not args.disable:
        print("[INFO] Использование:")
        print("  --enable   - включить Semantic Scholar API")
        print("  --disable  - отключить Semantic Scholar API")
        print("  --status   - показать статус")
        print("  --backup   - создать резервную копию")
        print()
        show_status()
        return
    
    if args.enable:
        success = toggle_semantic_scholar(True)
        if success:
            print()
            print("[TIPS] Рекомендации при включении Semantic Scholar:")
            print("1. Получите API ключ: https://www.semanticscholar.org/product/api#api-key-form")
            print("2. Установите переменную: export SEMANTIC_SCHOLAR_API_KEY=your_key_here")
            print("3. Проверьте статус: python scripts/check_semantic_scholar_status.py")
    else:
        success = toggle_semantic_scholar(False)
        if success:
            print()
            print("[TIPS] Semantic Scholar отключен из-за строгих лимитов")
            print("Пайплайн будет работать только с другими API:")
            print("- ChEMBL")
            print("- Crossref") 
            print("- OpenAlex")
            print("- PubMed")
    
    print()
    show_status()


if __name__ == "__main__":
    main()
