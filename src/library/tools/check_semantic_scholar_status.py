#!/usr/bin/env python3
"""Быстрая проверка статуса Semantic Scholar API.

Этот скрипт помогает быстро проверить, работает ли Semantic Scholar API
и есть ли проблемы с rate limiting.
"""

import os
import requests
import sys
import time
from typing import Optional


def check_api_status(api_key: Optional[str] = None, test_pmid: str = "7154002") -> dict:
    """Проверяет статус Semantic Scholar API."""
    
    if not api_key:
        api_key = os.environ.get('SEMANTIC_SCHOLAR_API_KEY')
    
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'bioactivity-data-acquisition/0.1.0'
    }
    
    if api_key:
        headers['x-api-key'] = api_key
    
    params = {
        'fields': 'title,abstract,year'
    }
    
    try:
        start_time = time.time()
        response = requests.get(
            f'https://api.semanticscholar.org/graph/v1/paper/PMID:{test_pmid}',
            headers=headers,
            params=params,
            timeout=30
        )
        response_time = (time.time() - start_time) * 1000
        
        result = {
            'status_code': response.status_code,
            'response_time_ms': response_time,
            'api_key_used': bool(api_key),
            'success': response.status_code == 200,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        if response.status_code == 200:
            try:
                data = response.json()
                result['data_received'] = {
                    'has_title': bool(data.get('title')),
                    'has_abstract': bool(data.get('abstract')),
                    'year': data.get('year')
                }
                result['message'] = "[OK] API работает нормально"
            except ValueError:
                result['data_received'] = {'json_parse_error': True}
                result['message'] = "[WARN] Ответ получен, но не в JSON формате"
                
        elif response.status_code == 429:
            result['message'] = "[ERROR] Rate limited (429)"
            try:
                error_data = response.json()
                result['error_details'] = error_data
            except ValueError:
                result['error_details'] = {'raw_response': response.text}
                
        elif response.status_code == 401:
            result['message'] = "[ERROR] Недействительный API ключ (401)"
            result['error_details'] = response.text
            
        else:
            result['message'] = f"[ERROR] Неожиданный статус: {response.status_code}"
            result['error_details'] = response.text
        
        return result
        
    except requests.exceptions.RequestException as e:
        return {
            'success': False,
            'message': f"[ERROR] Ошибка подключения: {str(e)}",
            'error_type': type(e).__name__,
            'api_key_used': bool(api_key),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }


def test_multiple_requests(api_key: Optional[str] = None, num_requests: int = 5) -> dict:
    """Тестирует множественные запросы для проверки rate limiting."""
    
    if not api_key:
        api_key = os.environ.get('SEMANTIC_SCHOLAR_API_KEY')
    
    print(f"[TEST] Тестируем {num_requests} запросов подряд...")
    
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'bioactivity-data-acquisition/0.1.0'
    }
    
    if api_key:
        headers['x-api-key'] = api_key
    
    test_pmids = ["7154002", "6991692", "6772788", "7035668", "6793726"]
    results = []
    
    for i in range(min(num_requests, len(test_pmids))):
        pmid = test_pmids[i]
        print(f"  Запрос {i+1}/{num_requests} (PMID: {pmid})...", end=" ")
        
        try:
            start_time = time.time()
            response = requests.get(
                f'https://api.semanticscholar.org/graph/v1/paper/PMID:{pmid}',
                headers=headers,
                params={'fields': 'title'},
                timeout=10
            )
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                print(f"[OK] ({response_time:.0f}ms)")
                results.append({
                    'request': i+1,
                    'pmid': pmid,
                    'status_code': response.status_code,
                    'response_time_ms': response_time,
                    'success': True
                })
            elif response.status_code == 429:
                print(f"[ERROR] Rate limited")
                results.append({
                    'request': i+1,
                    'pmid': pmid,
                    'status_code': response.status_code,
                    'success': False,
                    'error': 'Rate limited'
                })
                break
            else:
                print(f"[WARN] {response.status_code}")
                results.append({
                    'request': i+1,
                    'pmid': pmid,
                    'status_code': response.status_code,
                    'success': False,
                    'error': f'Status {response.status_code}'
                })
            
            # Небольшая пауза между запросами
            time.sleep(0.5)
            
        except Exception as e:
            print(f"[ERROR] {e}")
            results.append({
                'request': i+1,
                'pmid': pmid,
                'success': False,
                'error': str(e)
            })
            break
    
    return {
        'test_results': results,
        'total_requests': len(results),
        'successful_requests': sum(1 for r in results if r.get('success', False)),
        'rate_limited': any(r.get('status_code') == 429 for r in results)
    }


def main():
    """Основная функция."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Быстрая проверка Semantic Scholar API")
    parser.add_argument("--test-limits", action="store_true", help="Тестировать лимиты API")
    parser.add_argument("--requests", type=int, default=5, help="Количество запросов для тестирования")
    parser.add_argument("--pmid", type=str, default="7154002", help="PMID для тестирования")
    
    args = parser.parse_args()
    
    print("[INFO] Проверка Semantic Scholar API")
    print("=" * 50)
    
    api_key = os.environ.get('SEMANTIC_SCHOLAR_API_KEY')
    if api_key:
        print(f"[KEY] API ключ: {api_key[:10]}...")
    else:
        print("[WARN] API ключ не настроен")
        print("   Установите: export SEMANTIC_SCHOLAR_API_KEY=your_key_here")
    
    print()
    
    if args.test_limits:
        # Тестируем лимиты
        result = test_multiple_requests(api_key, args.requests)
        
        print(f"\n[RESULTS] Результаты тестирования:")
        print(f"Всего запросов: {result['total_requests']}")
        print(f"Успешных: {result['successful_requests']}")
        print(f"Rate limited: {'Да' if result['rate_limited'] else 'Нет'}")
        
        if result['rate_limited']:
            print("\n[TIPS] Рекомендации:")
            if not api_key:
                print("- Получите API ключ: https://www.semanticscholar.org/product/api#api-key-form")
            else:
                print("- API ключ настроен, но все еще rate limited")
                print("- Попробуйте увеличить интервалы между запросами")
                print("- Проверьте, не превышаете ли вы лимиты в других процессах")
    else:
        # Одиночная проверка
        result = check_api_status(api_key, args.pmid)
        
        print(f"[RESULTS] Результат проверки:")
        print(f"Статус: {result['message']}")
        print(f"Время ответа: {result.get('response_time_ms', 0):.0f}ms")
        print(f"API ключ: {'Используется' if result.get('api_key_used') else 'Не используется'}")
        
        if 'data_received' in result:
            data = result['data_received']
            print(f"Данные: Заголовок: {'[OK]' if data.get('has_title') else '[NO]'}, "
                  f"Аннотация: {'[OK]' if data.get('has_abstract') else '[NO]'}, "
                  f"Год: {data.get('year', 'N/A')}")
        
        if 'error_details' in result:
            print(f"Детали ошибки: {result['error_details']}")
    
    print(f"\n[TIME] Время проверки: {time.strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
