#!/usr/bin/env python3
"""Скрипт для проверки конкретных лимитов API с детальной информацией."""

import sys
import time

import requests
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from library.logging_setup import get_logger

logger = get_logger(__name__)

console = Console()


def check_chembl_limits():
    """Проверяет лимиты chembl API."""
    console.logger.info("\n[bold cyan]chembl API[/bold cyan]")
    
    # chembl API информация о лимитах
    chembl_info = {
        "base_url": "https://www.ebi.ac.uk/chembl/api/data",
        "rate_limits": {
            "requests_per_second": 20,
            "requests_per_minute": 1200,
            "requests_per_hour": 72000
        },
        "authentication": "Не требуется для базового использования",
        "documentation": "https://chembl.gitbook.io/chembl-interface-documentation/web-services/chembl-data-web-services"
    }
    
    # Тестовый запрос
    try:
        start_time = time.time()
        response = requests.get(
            f"{chembl_info['base_url']}/activity",
            params={"limit": 1},
            headers={"Accept": "application/json"},
            timeout=10
        )
        response_time = time.time() - start_time
        
        status = "Доступен" if response.status_code == 200 else f"Ошибка {response.status_code}"
        
        table = Table(title="chembl API - Статус")
        table.add_column("Параметр", style="cyan")
        table.add_column("Значение", style="green")
        
        table.add_row("Статус", status)
        table.add_row("Время ответа", f"{response_time:.3f}с")
        table.add_row("Запросов в секунду", str(chembl_info["rate_limits"]["requests_per_second"]))
        table.add_row("Запросов в минуту", str(chembl_info["rate_limits"]["requests_per_minute"]))
        table.add_row("Аутентификация", chembl_info["authentication"])
        
        console.logger.info(table)
        
    except Exception as e:
        console.logger.error(f"[red]Ошибка при проверке ChEMBL: {e}[/red]")


def check_pubmed_limits():
    """Проверяет лимиты PubMed API."""
    console.logger.info("\n[bold cyan]PubMed API[/bold cyan]")
    
    pubmed_info = {
        "base_url": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/",
        "rate_limits": {
            "without_key": {
                "requests_per_second": 3,
                "requests_per_minute": 180
            },
            "with_key": {
                "requests_per_second": 10,
                "requests_per_minute": 600
            }
        },
        "authentication": "API ключ опционален, но увеличивает лимиты",
        "documentation": "https://www.ncbi.nlm.nih.gov/books/NBK25501/"
    }
    
    # Тестовый запрос
    try:
        start_time = time.time()
        response = requests.get(
            f"{pubmed_info['base_url']}esearch.fcgi",
            params={
                "db": "pubmed",
                "term": "cancer",
                "retmax": 1,
                "retmode": "json"
            },
            timeout=10
        )
        response_time = time.time() - start_time
        
        status = "Доступен" if response.status_code == 200 else f"Ошибка {response.status_code}"
        
        table = Table(title="PubMed API - Статус")
        table.add_column("Параметр", style="cyan")
        table.add_column("Значение", style="green")
        
        table.add_row("Статус", status)
        table.add_row("Время ответа", f"{response_time:.3f}с")
        table.add_row("Без ключа (запросов/сек)", str(pubmed_info["rate_limits"]["without_key"]["requests_per_second"]))
        table.add_row("С ключом (запросов/сек)", str(pubmed_info["rate_limits"]["with_key"]["requests_per_second"]))
        table.add_row("Аутентификация", pubmed_info["authentication"])
        
        console.logger.info(table)
        
    except Exception as e:
        console.logger.error(f"[red]Ошибка при проверке PubMed: {e}[/red]")


def check_crossref_limits():
    """Проверяет лимиты Crossref API."""
    console.logger.info("\n[bold cyan]Crossref API[/bold cyan]")
    
    crossref_info = {
        "base_url": "https://api.crossref.org/works",
        "rate_limits": {
            "free": {
                "requests_per_second": 50,
                "requests_per_minute": 3000
            },
            "plus": {
                "requests_per_second": 100,
                "requests_per_minute": 6000
            }
        },
        "authentication": "Plus API Token для увеличенных лимитов",
        "documentation": "https://www.crossref.org/documentation/retrieve-metadata/rest-api/"
    }
    
    # Тестовый запрос
    try:
        start_time = time.time()
        response = requests.get(
            crossref_info["base_url"],
            params={"rows": 1},
            headers={"Accept": "application/json"},
            timeout=10
        )
        response_time = time.time() - start_time
        
        status = "Доступен" if response.status_code == 200 else f"Ошибка {response.status_code}"
        
        # Проверяем заголовки rate limit
        rate_limit_headers = {}
        for header in ["X-RateLimit-Limit", "X-RateLimit-Interval", "X-RateLimit-Reset"]:
            if header in response.headers:
                rate_limit_headers[header] = response.headers[header]
        
        table = Table(title="Crossref API - Статус")
        table.add_column("Параметр", style="cyan")
        table.add_column("Значение", style="green")
        
        table.add_row("Статус", status)
        table.add_row("Время ответа", f"{response_time:.3f}с")
        table.add_row("Free (запросов/сек)", str(crossref_info["rate_limits"]["free"]["requests_per_second"]))
        table.add_row("Plus (запросов/сек)", str(crossref_info["rate_limits"]["plus"]["requests_per_second"]))
        table.add_row("Аутентификация", crossref_info["authentication"])
        
        if rate_limit_headers:
            for header, value in rate_limit_headers.items():
                table.add_row(f"Заголовок {header}", value)
        
        console.logger.info(table)
        
    except Exception as e:
        console.logger.error(f"[red]Ошибка при проверке Crossref: {e}[/red]")


def check_openalex_limits():
    """Проверяет лимиты OpenAlex API."""
    console.logger.info("\n[bold cyan]OpenAlex API[/bold cyan]")
    
    openalex_info = {
        "base_url": "https://api.openalex.org/works",
        "rate_limits": {
            "requests_per_second": 10,
            "requests_per_minute": 600
        },
        "authentication": "Не требуется",
        "documentation": "https://docs.openalex.org/api"
    }
    
    # Тестовый запрос
    try:
        start_time = time.time()
        response = requests.get(
            openalex_info["base_url"],
            params={"per-page": 1},
            headers={"Accept": "application/json"},
            timeout=10
        )
        response_time = time.time() - start_time
        
        status = "Доступен" if response.status_code == 200 else f"Ошибка {response.status_code}"
        
        table = Table(title="OpenAlex API - Статус")
        table.add_column("Параметр", style="cyan")
        table.add_column("Значение", style="green")
        
        table.add_row("Статус", status)
        table.add_row("Время ответа", f"{response_time:.3f}с")
        table.add_row("Запросов в секунду", str(openalex_info["rate_limits"]["requests_per_second"]))
        table.add_row("Запросов в минуту", str(openalex_info["rate_limits"]["requests_per_minute"]))
        table.add_row("Аутентификация", openalex_info["authentication"])
        
        console.logger.info(table)
        
    except Exception as e:
        console.logger.error(f"[red]Ошибка при проверке OpenAlex: {e}[/red]")


def check_semantic_scholar_limits():
    """Проверяет лимиты Semantic Scholar API."""
    console.logger.info("\n[bold cyan]Semantic Scholar API[/bold cyan]")
    
    scholar_info = {
        "base_url": "https://api.semanticscholar.org/graph/v1/paper",
        "rate_limits": {
            "requests_per_second": 100,
            "requests_per_minute": 6000
        },
        "authentication": "Не требуется",
        "documentation": "https://www.semanticscholar.org/product/api"
    }
    
    # Тестовый запрос
    try:
        start_time = time.time()
        response = requests.get(
            f"{scholar_info['base_url']}/search",
            params={"query": "machine learning", "limit": 1},
            headers={"Accept": "application/json"},
            timeout=10
        )
        response_time = time.time() - start_time
        
        status = "Доступен" if response.status_code == 200 else f"Ошибка {response.status_code}"
        
        table = Table(title="Semantic Scholar API - Статус")
        table.add_column("Параметр", style="cyan")
        table.add_column("Значение", style="green")
        
        table.add_row("Статус", status)
        table.add_row("Время ответа", f"{response_time:.3f}с")
        table.add_row("Запросов в секунду", str(scholar_info["rate_limits"]["requests_per_second"]))
        table.add_row("Запросов в минуту", str(scholar_info["rate_limits"]["requests_per_minute"]))
        table.add_row("Аутентификация", scholar_info["authentication"])
        
        console.logger.info(table)
        
    except Exception as e:
        console.logger.error(f"[red]Ошибка при проверке Semantic Scholar: {e}[/red]")


def display_recommendations():
    """Отображает рекомендации по оптимизации использования API."""
    recommendations = """
Рекомендации по оптимизации API:

1. **ChEMBL API**:
   • Используйте пагинацию для больших запросов
   • Кэшируйте результаты для часто используемых запросов
   • Мониторьте заголовки ответов на предмет rate limiting

2. **PubMed API**:
   • Получите API ключ для увеличения лимитов с 3 до 10 запросов/сек
   • Используйте batch запросы когда возможно
   • Добавьте задержки между запросами

3. **Crossref API**:
   • Рассмотрите получение Plus API Token
   • Используйте фильтры для уменьшения размера ответов
   • Кэшируйте результаты поиска

4. **OpenAlex API**:
   • Используйте параметр per-page для контроля размера ответа
   • Мониторьте использование через заголовки ответов

5. **Semantic Scholar API**:
   • Самый либеральный API с лимитом 100 запросов/сек
   • Подходит для массовых запросов

Общие рекомендации:
• Реализуйте экспоненциальный backoff при получении 429 ошибок
• Логируйте все API запросы для мониторинга
• Используйте пулы соединений для повышения производительности
• Настройте таймауты согласно документации API
"""
    
    console.logger.info(Panel(recommendations, title="Рекомендации", border_style="green"))


def main():
    """Основная функция."""
    console.logger.info("[bold blue]Детальная проверка лимитов API[/bold blue]\n")
    
    # Проверяем все API
    check_chembl_limits()
    check_pubmed_limits()
    check_crossref_limits()
    check_openalex_limits()
    check_semantic_scholar_limits()
    
    # Отображаем рекомендации
    display_recommendations()


if __name__ == "__main__":
    main()
