#!/usr/bin/env python3
"""Скрипт для проверки лимитов и доступности API эндпоинтов."""

import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from library.logging_setup import get_logger

logger = get_logger(__name__)

console = Console()


class APILimitChecker:
    """Класс для проверки лимитов и доступности API."""
    
    def __init__(self, config_path: str = "configs/config.yaml"):
        """Инициализация с конфигурацией."""
        self.config_path = config_path
        self.config = self._load_config()
        self.results: list[dict[str, Any]] = []
        
    def _load_config(self) -> dict[str, Any]:
        """Загружает конфигурацию из YAML файла."""
        try:
            with open(self.config_path, encoding='utf-8') as f:
                config = yaml.safe_load(f)
                return config if config is not None else {}
        except Exception as e:
            console.print(f"[red]Ошибка загрузки конфигурации: {e}[/red]")
            return {}
    
    def _check_api_endpoint(self, name: str, config: dict[str, Any]) -> dict[str, Any]:
        """Проверяет доступность конкретного API эндпоинта."""
        result: dict[str, Any] = {
            "name": name,
            "status": "unknown",
            "response_time": None,
            "status_code": None,
            "rate_limit_info": {},
            "error": None,
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            base_url = config.get("http", {}).get("base_url", "")
            if not base_url:
                result["error"] = "Нет base_url в конфигурации"
                return result
            
            # Подготавливаем заголовки
            headers = {
                "User-Agent": "bioactivity-data-acquisition/0.1.0",
                "Accept": "application/json"
            }
            
            # Добавляем специфичные заголовки
            if "headers" in config.get("http", {}):
                headers.update(config["http"]["headers"])
            
            # Убираем placeholder'ы из заголовков
            for key, value in headers.items():
                if isinstance(value, str) and ("{" in value and "}" in value):
                    headers[key] = None  # type: ignore
            
            # Фильтруем None значения
            headers = {k: v for k, v in headers.items() if v is not None}
            
            # Делаем тестовый запрос
            timeout = config.get("http", {}).get("timeout_sec", 10)
            
            start_time = time.time()
            response = requests.get(base_url, headers=headers, timeout=timeout)
            response_time = time.time() - start_time
            
            result["response_time"] = round(response_time, 3)
            result["status_code"] = response.status_code
            
            # Проверяем статус
            if response.status_code == 200:
                result["status"] = "available"
            elif response.status_code == 429:
                result["status"] = "rate_limited"
            elif response.status_code in [401, 403]:
                result["status"] = "auth_required"
            elif response.status_code >= 500:
                result["status"] = "server_error"
            else:
                result["status"] = "error"
            
            # Извлекаем информацию о rate limits из заголовков
            rate_limit_headers = [
                "X-RateLimit-Limit",
                "X-RateLimit-Remaining", 
                "X-RateLimit-Reset",
                "X-RateLimit-Retry-After",
                "Retry-After"
            ]
            
            for header in rate_limit_headers:
                if header in response.headers:
                    result["rate_limit_info"][header] = response.headers[header]
            
            # Специфичная информация для разных API
            if name == "chembl":
                # chembl API часто возвращает XML при проблемах
                content_type = response.headers.get('content-type', '').lower()
                if 'xml' in content_type:
                    result["status"] = "xml_response"
                    result["error"] = "API вернул XML вместо JSON"
            
        except requests.exceptions.Timeout:
            result["status"] = "timeout"
            result["error"] = "Превышен таймаут запроса"
        except requests.exceptions.ConnectionError:
            result["status"] = "connection_error"
            result["error"] = "Ошибка подключения"
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
        
        return result
    
    def check_all_apis(self) -> list[dict[str, Any]]:
        """Проверяет все API из конфигурации."""
        sources = self.config.get("sources", {})
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Проверка API...", total=len(sources))
            
            for name, config in sources.items():
                progress.update(task, description=f"Проверка {name}...")
                result = self._check_api_endpoint(name, config)
                self.results.append(result)
                progress.advance(task)
        
        return self.results
    
    def display_results(self) -> None:
        """Отображает результаты проверки в красивом формате."""
        if not self.results:
            console.print("[yellow]Нет результатов для отображения[/yellow]")
            return
        
        # Создаем таблицу с результатами
        table = Table(title="Результаты проверки API лимитов")
        table.add_column("API", style="cyan", no_wrap=True)
        table.add_column("Статус", justify="center")
        table.add_column("Код ответа", justify="center")
        table.add_column("Время ответа (с)", justify="right")
        table.add_column("Rate Limit", style="magenta")
        table.add_column("Ошибка", style="red")
        
        for result in self.results:
            # Определяем цвет статуса
            status_colors = {
                "available": "green",
                "rate_limited": "yellow", 
                "auth_required": "orange3",
                "server_error": "red",
                "timeout": "red",
                "connection_error": "red",
                "xml_response": "orange3",
                "error": "red",
                "unknown": "grey"
            }
            
            status_color = status_colors.get(result["status"], "grey")
            status_text = result["status"].replace("_", " ").title()
            
            # Форматируем время ответа
            response_time = f"{result['response_time']:.3f}" if result["response_time"] else "N/A"
            
            # Форматируем код ответа
            status_code = str(result["status_code"]) if result["status_code"] else "N/A"
            
            # Форматируем информацию о rate limits
            rate_limit_info = ""
            if result["rate_limit_info"]:
                for key, value in result["rate_limit_info"].items():
                    rate_limit_info += f"{key}: {value}\n"
            
            # Форматируем ошибку
            error = result.get("error", "") or ""
            
            table.add_row(
                result["name"],
                f"[{status_color}]{status_text}[/{status_color}]",
                status_code,
                response_time,
                rate_limit_info.strip() or "N/A",
                error
            )
        
        console.print(table)
        
        # Дополнительная информация
        self._display_summary()
    
    def _display_summary(self) -> None:
        """Отображает сводку результатов."""
        total = len(self.results)
        available = len([r for r in self.results if r["status"] == "available"])
        error_statuses = ["error", "timeout", "connection_error"]
        errors = len([r for r in self.results if r["status"] in error_statuses])
        rate_limited = len([r for r in self.results if r["status"] == "rate_limited"])
        
        summary = f"""
Сводка:
• Всего API: {total}
• Доступны: {available}
• Ошибки: {errors}
• Rate Limited: {rate_limited}
"""
        
        console.print(Panel(summary, title="Сводка", border_style="blue"))
        
        # Рекомендации
        if errors > 0:
            console.print("\n[red]ВНИМАНИЕ: Найдены проблемы с API![/red]")
            console.print("Рекомендации:")
            console.print("• Проверьте подключение к интернету")
            console.print("• Убедитесь, что API ключи настроены правильно")
            console.print("• Проверьте, не заблокированы ли запросы файрволом")
        
        if rate_limited > 0:
            console.print("\n[yellow]ВНИМАНИЕ: Некоторые API имеют ограничения скорости![/yellow]")
            console.print("Рекомендации:")
            console.print("• Увеличьте интервалы между запросами")
            console.print("• Рассмотрите получение API ключей для увеличения лимитов")
    
    def save_results(self, filename: str | None = None) -> None:
        """Сохраняет результаты в JSON файл."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Получаем путь к корню проекта (на 3 уровня выше)
            project_root = Path(__file__).parent.parent.parent.parent
            reports_dir = project_root / "reports"
            reports_dir.mkdir(exist_ok=True)
            filename = str(reports_dir / f"api_limits_check_{timestamp}.json")
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, indent=2, ensure_ascii=False)
            console.print(f"[green]Результаты сохранены в {filename}[/green]")
        except Exception as e:
            console.print(f"[red]Ошибка сохранения: {e}[/red]")


def main() -> None:
    """Основная функция."""
    console.print("[bold blue]Проверка лимитов API[/bold blue]\n")
    
    checker = APILimitChecker()
    checker.check_all_apis()
    checker.display_results()
    
    # Сохраняем результаты
    checker.save_results()


if __name__ == "__main__":
    main()
