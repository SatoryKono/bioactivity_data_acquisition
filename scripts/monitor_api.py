#!/usr/bin/env python3
"""Мониторинг API в реальном времени."""

import argparse
import time
import json
from datetime import datetime
from typing import Dict, List

import requests


class APIMonitor:
    """Класс для мониторинга API."""
    
    def __init__(self, api_name: str, url: str, params: dict = None, headers: dict = None):
        self.api_name = api_name
        self.url = url
        self.params = params or {}
        self.headers = headers or {}
        self.history = []
        
    def check_status(self) -> Dict:
        """Проверяет текущий статус API."""
        start_time = time.time()
        
        try:
            response = requests.get(
                self.url, 
                params=self.params, 
                headers=self.headers, 
                timeout=10
            )
            response_time = time.time() - start_time
            
            status = {
                "timestamp": datetime.now().isoformat(),
                "api": self.api_name,
                "status_code": response.status_code,
                "response_time": round(response_time, 3),
                "available": response.status_code == 200,
                "rate_limited": response.status_code == 429,
                "error": None
            }
            
            # Извлекаем rate limit информацию
            rate_limit_info = {}
            for header in response.headers:
                if 'rate' in header.lower() or 'limit' in header.lower():
                    rate_limit_info[header] = response.headers[header]
            
            status["rate_limit_headers"] = rate_limit_info
            
        except Exception as e:
            status = {
                "timestamp": datetime.now().isoformat(),
                "api": self.api_name,
                "status_code": None,
                "response_time": None,
                "available": False,
                "rate_limited": False,
                "error": str(e)
            }
        
        self.history.append(status)
        return status
    
    def get_stats(self, last_n: int = 10) -> Dict:
        """Возвращает статистику за последние N проверок."""
        recent = self.history[-last_n:] if len(self.history) >= last_n else self.history
        
        if not recent:
            return {}
        
        available_count = sum(1 for s in recent if s["available"])
        avg_response_time = sum(s["response_time"] or 0 for s in recent) / len(recent)
        error_count = sum(1 for s in recent if s["error"])
        
        return {
            "api": self.api_name,
            "checks": len(recent),
            "availability": round(available_count / len(recent) * 100, 1),
            "avg_response_time": round(avg_response_time, 3),
            "errors": error_count,
            "last_status": recent[-1] if recent else None
        }


def monitor_api(api_name: str, interval: int = 30, duration: int = None):
    """Мониторит API с заданным интервалом."""
    
    # Определяем конфигурацию API
    apis = {
        "chembl": {
            "url": "https://www.ebi.ac.uk/chembl/api/data/activity",
            "params": {"limit": 1},
            "headers": {"Accept": "application/json"}
        },
        "pubmed": {
            "url": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            "params": {"db": "pubmed", "term": "cancer", "retmax": 1, "retmode": "json"},
            "headers": {"Accept": "application/json"}
        },
        "crossref": {
            "url": "https://api.crossref.org/works",
            "params": {"rows": 1},
            "headers": {"Accept": "application/json"}
        },
        "openalex": {
            "url": "https://api.openalex.org/works",
            "params": {"per-page": 1},
            "headers": {"Accept": "application/json"}
        },
        "semantic": {
            "url": "https://api.semanticscholar.org/graph/v1/paper/search",
            "params": {"query": "machine learning", "limit": 1},
            "headers": {"Accept": "application/json"}
        }
    }
    
    if api_name not in apis:
        print(f"Неизвестный API: {api_name}")
        print(f"Доступные API: {', '.join(apis.keys())}")
        return
    
    config = apis[api_name]
    monitor = APIMonitor(api_name, **config)
    
    print(f"Мониторинг {api_name.upper()} API каждые {interval} секунд")
    if duration:
        print(f"Продолжительность: {duration} секунд")
    print("Нажмите Ctrl+C для остановки\n")
    
    start_time = time.time()
    check_count = 0
    
    try:
        while True:
            check_count += 1
            status = monitor.check_status()
            
            # Отображаем текущий статус
            timestamp = datetime.now().strftime("%H:%M:%S")
            if status["available"]:
                print(f"[{timestamp}] OK {api_name.upper()} - OK ({status['response_time']}с)")
            elif status["rate_limited"]:
                print(f"[{timestamp}] WARNING {api_name.upper()} - RATE LIMITED")
            elif status["error"]:
                print(f"[{timestamp}] ERROR {api_name.upper()} - ERROR: {status['error']}")
            else:
                print(f"[{timestamp}] ERROR {api_name.upper()} - ERROR {status['status_code']}")
            
            # Показываем статистику каждые 10 проверок
            if check_count % 10 == 0:
                stats = monitor.get_stats()
                print(f"\nСтатистика за последние 10 проверок:")
                print(f"  Доступность: {stats['availability']}%")
                print(f"  Среднее время ответа: {stats['avg_response_time']}с")
                print(f"  Ошибки: {stats['errors']}\n")
            
            # Проверяем, не истекло ли время
            if duration and (time.time() - start_time) >= duration:
                break
                
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print(f"\nМониторинг остановлен пользователем")
    
    # Финальная статистика
    stats = monitor.get_stats()
    print(f"\nФинальная статистика:")
    print(f"  Всего проверок: {stats['checks']}")
    print(f"  Доступность: {stats['availability']}%")
    print(f"  Среднее время ответа: {stats['avg_response_time']}с")
    print(f"  Ошибки: {stats['errors']}")
    
    # Сохраняем историю
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"reports/monitor_{api_name}_{timestamp}.json"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(monitor.history, f, indent=2, ensure_ascii=False)
        print(f"История сохранена в {filename}")
    except Exception as e:
        print(f"Ошибка сохранения истории: {e}")


def main():
    """Основная функция."""
    parser = argparse.ArgumentParser(description="Мониторинг API в реальном времени")
    parser.add_argument("api", choices=["chembl", "pubmed", "crossref", "openalex", "semantic"],
                       help="API для мониторинга")
    parser.add_argument("-i", "--interval", type=int, default=30,
                       help="Интервал проверки в секундах (по умолчанию: 30)")
    parser.add_argument("-d", "--duration", type=int,
                       help="Продолжительность мониторинга в секундах")
    
    args = parser.parse_args()
    
    monitor_api(args.api, args.interval, args.duration)


if __name__ == "__main__":
    main()
