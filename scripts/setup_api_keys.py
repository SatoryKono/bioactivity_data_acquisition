#!/usr/bin/env python3
"""
Скрипт для установки API ключей в переменные окружения
Setup script for API keys environment variables
"""

import argparse
import os
import sys
from pathlib import Path


def set_environment_variable(name: str, value: str, persistent: bool = False) -> None:
    """Установить переменную окружения."""
    if not value:
        print(f"Пропуск {name} (значение не указано)")
        return
    
    # Установка для текущей сессии
    os.environ[name] = value
    print(f"[OK] {name} установлен для текущей сессии")
    
    # Установка постоянно (если запрошено)
    if persistent:
        try:
            # Определяем файл профиля
            home = Path.home()
            if sys.platform == "win32":
                # Windows - используем реестр через setx
                import subprocess
                subprocess.run(["setx", name, value], check=True, capture_output=True)  # noqa: S607
                print(f"[OK] {name} установлен постоянно (Windows)")
            else:
                # Unix-like системы
                profile_files = [
                    home / ".bashrc",
                    home / ".zshrc", 
                    home / ".profile"
                ]
                
                for profile_file in profile_files:
                    if profile_file.exists():
                        # Проверяем, не установлена ли уже переменная
                        content = profile_file.read_text()
                        if f"export {name}=" not in content:
                            with open(profile_file, "a") as f:
                                f.write(f'\nexport {name}="{value}"\n')
                            print(f"[OK] {name} добавлен в {profile_file}")
                        else:
                            print(f"[WARN] {name} уже установлен в {profile_file}")
                        break
        except Exception as e:
            print(f"[ERROR] Ошибка при установке {name} постоянно: {e}")


def check_environment_variables() -> None:
    """Проверить установленные переменные окружения."""
    print("\nПроверка установленных переменных:")
    
    env_vars = [
        "SEMANTIC_SCHOLAR_API_KEY",
        "CHEMBL_API_TOKEN",
        "CROSSREF_API_KEY", 
        "OPENALEX_API_KEY",
        "PUBMED_API_KEY"
    ]
    
    for var in env_vars:
        value = os.environ.get(var)
        if value:
            display_value = value[:10] + "..." if len(value) > 10 else value
            print(f"  [OK] {var} = {display_value}")
        else:
            print(f"  [MISSING] {var} = не установлен")


def main():
    """Основная функция."""
    parser = argparse.ArgumentParser(
        description="Скрипт для установки API ключей в переменные окружения",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  # Установить только Semantic Scholar ключ (по умолчанию)
  python scripts/setup_api_keys.py

  # Установить все ключи
  python scripts/setup_api_keys.py --chembl "your_token" --crossref "your_key"

  # Установить постоянно
  python scripts/setup_api_keys.py --persistent

  # Установить кастомный Semantic Scholar ключ
  python scripts/setup_api_keys.py --semantic-scholar "your_custom_key"
        """
    )
    
    parser.add_argument(
        "--semantic-scholar", 
        default="o2N1y1RHYU3aqEj556Oyv4oBzZrHthM2bWda2lf4",
        help="API ключ для Semantic Scholar"
    )
    parser.add_argument("--chembl", default="", help="API токен для ChEMBL")
    parser.add_argument("--crossref", default="", help="API ключ для Crossref")
    parser.add_argument("--openalex", default="", help="API ключ для OpenAlex")
    parser.add_argument("--pubmed", default="", help="API ключ для PubMed")
    parser.add_argument(
        "--persistent", 
        action="store_true",
        help="Установить переменные постоянно"
    )
    
    args = parser.parse_args()
    
    print("Настройка API ключей...")
    
    # Установка API ключей
    set_environment_variable("SEMANTIC_SCHOLAR_API_KEY", args.semantic_scholar, args.persistent)
    set_environment_variable("CHEMBL_API_TOKEN", args.chembl, args.persistent)
    set_environment_variable("CROSSREF_API_KEY", args.crossref, args.persistent)
    set_environment_variable("OPENALEX_API_KEY", args.openalex, args.persistent)
    set_environment_variable("PUBMED_API_KEY", args.pubmed, args.persistent)
    
    # Проверка установленных переменных
    check_environment_variables()
    
    print("\nГотово! Теперь можно запускать команды:")
    if sys.platform == "win32":
        print("   bioactivity-data-acquisition get-document-data --config configs\\config_documents_full.yaml --limit 10")
    else:
        print("   bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --limit 10")
    
    if args.persistent:
        print("\n[WARN] Для применения постоянных переменных перезапустите терминал")


if __name__ == "__main__":
    main()
