#!/usr/bin/env python3
"""Комплексная проверка здоровья API."""

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

import requests


def check_api_health(name: str, url: str, params: dict = None, headers: dict = None) -> dict:
    """Проверяет здоровье конкретного API."""
    start_time = time.time()
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response_time = time.time() - start_time
        
        # Определяем статус здоровья
        if response.status_code == 200:
            health_status = "healthy"
        elif response.status_code == 429:
            health_status = "rate_limited"
        elif response.status_code in [401, 403]:
            health_status = "auth_required"
        elif response.status_code >= 500:
            health_status = "server_error"
        else:
            health_status = "unhealthy"
        
        # Извлекаем информацию о rate limits
        rate_limit_info = {}
        for header in response.headers:
            if any(keyword in header.lower() for keyword in ['rate', 'limit', 'retry']):
                rate_limit_info[header] = response.headers[header]
        
        return {
            "api": name,
            "health_status": health_status,
            "status_code": response.status_code,
            "response_time": round(response_time, 3),
            "timestamp": datetime.now().isoformat(),
            "rate_limit_info": rate_limit_info,
            "error": None
        }
        
    except requests.exceptions.Timeout:
        return {
            "api": name,
            "health_status": "timeout",
            "status_code": None,
            "response_time": None,
            "timestamp": datetime.now().isoformat(),
            "rate_limit_info": {},
            "error": "Request timeout"
        }
    except requests.exceptions.ConnectionError:
        return {
            "api": name,
            "health_status": "connection_error",
            "status_code": None,
            "response_time": None,
            "timestamp": datetime.now().isoformat(),
            "rate_limit_info": {},
            "error": "Connection error"
        }
    except Exception as e:
        return {
            "api": name,
            "health_status": "error",
            "status_code": None,
            "response_time": None,
            "timestamp": datetime.now().isoformat(),
            "rate_limit_info": {},
            "error": str(e)
        }


def check_all_apis() -> list[dict]:
    """Проверяет все API."""
    apis = {
        "chembl": {
            "url": "https://www.ebi.ac.uk/chembl/api/data/activity",
            "params": {"limit": 1},
            "headers": {"Accept": "application/json"}
        },
        "PubMed": {
            "url": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            "params": {"db": "pubmed", "term": "cancer", "retmax": 1, "retmode": "json"},
            "headers": {"Accept": "application/json"}
        },
        "Crossref": {
            "url": "https://api.crossref.org/works",
            "params": {"rows": 1},
            "headers": {"Accept": "application/json"}
        },
        "OpenAlex": {
            "url": "https://api.openalex.org/works",
            "params": {"per-page": 1},
            "headers": {"Accept": "application/json"}
        },
        "Semantic Scholar": {
            "url": "https://api.semanticscholar.org/graph/v1/paper/search",
            "params": {"query": "machine learning", "limit": 1},
            "headers": {"Accept": "application/json"}
        }
    }
    
    results = []
    for name, config in apis.items():
        print(f"Проверка {name}...")
        result = check_api_health(name, **config)
        results.append(result)
        time.sleep(1)  # Небольшая задержка между запросами
    
    return results


def display_health_summary(results: list[dict]):
    """Отображает сводку по здоровью API."""
    print("\n" + "="*60)
    print("СВОДКА ПО ЗДОРОВЬЮ API")
    print("="*60)
    
    healthy_count = sum(1 for r in results if r["health_status"] == "healthy")
    total_count = len(results)
    
    print(f"Всего API: {total_count}")
    print(f"Здоровые: {healthy_count}")
    print(f"Проблемы: {total_count - healthy_count}")
    print(f"Общий статус: {'ЗДОРОВ' if healthy_count == total_count else 'ЕСТЬ ПРОБЛЕМЫ'}")
    
    print("\nДетали:")
    print("-" * 60)
    
    for result in results:
        status_icon = {
            "healthy": "OK",
            "rate_limited": "RATE_LIMIT",
            "auth_required": "AUTH_REQUIRED",
            "server_error": "SERVER_ERROR",
            "timeout": "TIMEOUT",
            "connection_error": "CONNECTION_ERROR",
            "error": "ERROR",
            "unhealthy": "UNHEALTHY"
        }.get(result["health_status"], "UNKNOWN")
        
        response_time = f"{result['response_time']}с" if result["response_time"] else "N/A"
        status_code = result["status_code"] or "N/A"
        
        print(f"{result['api']:15} | {status_icon:12} | {status_code:3} | {response_time:>8}")
        
        if result["error"]:
            print(f"                  | Ошибка: {result['error']}")
        
        if result["rate_limit_info"]:
            for header, value in result["rate_limit_info"].items():
                print(f"                  | {header}: {value}")


def generate_recommendations(results: list[dict]) -> list[str]:
    """Генерирует рекомендации на основе результатов проверки."""
    recommendations = []
    
    # Анализируем результаты
    unhealthy_apis = [r for r in results if r["health_status"] != "healthy"]
    slow_apis = [r for r in results if r["response_time"] and r["response_time"] > 5.0]
    rate_limited_apis = [r for r in results if r["health_status"] == "rate_limited"]
    
    if unhealthy_apis:
        recommendations.append("Некоторые API недоступны или работают некорректно")
        for api in unhealthy_apis:
            if api["health_status"] == "connection_error":
                msg = f"- {api['api']}: Проверьте подключение к интернету"
                recommendations.append(msg)
            elif api["health_status"] == "timeout":
                msg = f"- {api['api']}: Увеличьте таймауты или проверьте скорость сети"
                recommendations.append(msg)
            elif api["health_status"] == "auth_required":
                msg = f"- {api['api']}: Настройте аутентификацию или получите API ключ"
                recommendations.append(msg)
            elif api["health_status"] == "server_error":
                msg = f"- {api['api']}: Проблемы на стороне сервера, попробуйте позже"
                recommendations.append(msg)
    
    if slow_apis:
        recommendations.append("Некоторые API отвечают медленно:")
        for api in slow_apis:
            recommendations.append(f"- {api['api']}: {api['response_time']}с (рассмотрите кэширование)")
    
    if rate_limited_apis:
        recommendations.append("Некоторые API ограничивают количество запросов:")
        for api in rate_limited_apis:
            recommendations.append(f"- {api['api']}: Увеличьте интервалы между запросами")
    
    if not recommendations:
        recommendations.append("Все API работают нормально")
    
    return recommendations


def save_results(results: list[dict], recommendations: list[str]):
    """Сохраняет результаты в файл."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Получаем путь к корню проекта (на 3 уровня выше)
    project_root = Path(__file__).parent.parent.parent.parent
    reports_dir = project_root / "reports"
    reports_dir.mkdir(exist_ok=True)
    filename = reports_dir / f"api_health_check_{timestamp}.json"
    
    data = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total_apis": len(results),
            "healthy_apis": sum(1 for r in results if r["health_status"] == "healthy"),
            "unhealthy_apis": sum(1 for r in results if r["health_status"] != "healthy")
        },
        "results": results,
        "recommendations": recommendations
    }
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"\nРезультаты сохранены в {filename}")
    except Exception as e:
        print(f"\nОшибка сохранения результатов: {e}")


def main():
    """Основная функция."""
    parser = argparse.ArgumentParser(description="Комплексная проверка здоровья API")
    parser.add_argument("--save", action="store_true", help="Сохранить результаты в файл")
    parser.add_argument("--verbose", action="store_true", help="Подробный вывод")
    
    args = parser.parse_args()
    
    print("Комплексная проверка здоровья API")
    print("=" * 40)
    
    # Проверяем все API
    results = check_all_apis()
    
    # Отображаем сводку
    display_health_summary(results)
    
    # Генерируем рекомендации
    recommendations = generate_recommendations(results)
    
    print("\nРЕКОМЕНДАЦИИ:")
    print("-" * 40)
    for rec in recommendations:
        print(f"• {rec}")
    
    # Сохраняем результаты если нужно
    if args.save:
        save_results(results, recommendations)


if __name__ == "__main__":
    main()
