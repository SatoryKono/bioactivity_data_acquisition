#!/usr/bin/env python3
"""Мониторинг PubMed E-utilities API для отслеживания лимитов и статуса.

Этот скрипт помогает мониторить использование PubMed E-utilities API
и предупреждает о приближении к лимитам.
"""

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

import requests


class PubMedMonitor:
    """Монитор для PubMed E-utilities API."""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.environ.get('PUBMED_API_KEY')
        self.base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
        self.session = requests.Session()
        
        # Настройка заголовков
        self.headers = {
            'Accept': 'application/json',
            'User-Agent': 'bioactivity-data-acquisition/0.1.0'
        }
    
    def test_api_call(self) -> Dict[str, Any]:
        """Выполняет тестовый запрос к PubMed E-utilities."""
        
        # Тестовый PMID
        test_pmid = "7154002"
        
        params = {
            'db': 'pubmed',
            'id': test_pmid,
            'retmode': 'json'
        }
        
        # Добавляем API ключ если есть
        if self.api_key:
            params['api_key'] = self.api_key
        
        try:
            response = self.session.get(
                f"{self.base_url}/esummary.fcgi",
                params=params,
                headers=self.headers,
                timeout=30
            )
            
            result = {
                'timestamp': datetime.now().isoformat(),
                'status_code': response.status_code,
                'success': response.status_code == 200,
                'response_time_ms': response.elapsed.total_seconds() * 1000,
                'api_key_used': bool(self.api_key),
            }
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    result['data_received'] = {
                        'has_result': 'result' in data,
                        'pmid_found': test_pmid in str(data),
                    }
                except:
                    result['data_received'] = {'json_parse_error': True}
            else:
                result['error'] = response.text
                
                # Анализируем ошибку rate limiting
                if response.status_code == 429:
                    try:
                        error_data = response.json()
                        result['rate_limit_info'] = {
                            'limit': error_data.get('limit'),
                            'count': error_data.get('count'),
                            'api_key': error_data.get('api-key'),
                        }
                    except:
                        pass
            
            return result
            
        except requests.exceptions.RequestException as e:
            return {
                'timestamp': datetime.now().isoformat(),
                'success': False,
                'error': str(e),
                'error_type': type(e).__name__,
                'api_key_used': bool(self.api_key),
            }
    
    def check_rate_limits(self) -> Dict[str, Any]:
        """Проверяет текущие лимиты API."""
        
        result = self.test_api_call()
        
        if not result['success']:
            return result
        
        # Анализ результатов
        analysis = {
            'api_key_configured': bool(self.api_key),
            'current_status': 'healthy' if result['success'] else 'error',
            'response_time_ms': result.get('response_time_ms', 0),
        }
        
        # Если есть информация о rate limiting
        if 'rate_limit_info' in result:
            rate_info = result['rate_limit_info']
            if 'limit' in rate_info and 'count' in rate_info:
                limit = int(rate_info['limit'])
                count = int(rate_info['count'])
                usage_percent = (count / limit) * 100
                
                analysis.update({
                    'rate_limit_total': limit,
                    'rate_limit_current': count,
                    'usage_percent': round(usage_percent, 1),
                    'status': 'critical' if usage_percent > 90 else 'warning' if usage_percent > 70 else 'good'
                })
        
        return analysis
    
    def test_rate_limits_aggressively(self) -> Dict[str, Any]:
        """Агрессивно тестирует лимиты API."""
        
        print("🧪 Агрессивное тестирование лимитов PubMed API...")
        
        test_pmid = "7154002"
        results = []
        
        # Тестируем быстрое выполнение запросов
        for i in range(5):
            params = {
                'db': 'pubmed',
                'id': test_pmid,
                'retmode': 'json'
            }
            
            if self.api_key:
                params['api_key'] = self.api_key
            
            try:
                start_time = time.time()
                response = self.session.get(
                    f"{self.base_url}/esummary.fcgi",
                    params=params,
                    headers=self.headers,
                    timeout=10
                )
                response_time = (time.time() - start_time) * 1000
                
                result = {
                    'request_number': i + 1,
                    'status_code': response.status_code,
                    'response_time_ms': response_time,
                    'success': response.status_code == 200,
                }
                
                if response.status_code == 429:
                    try:
                        error_data = response.json()
                        result['rate_limit_info'] = error_data
                    except:
                        pass
                
                results.append(result)
                
                print(f"  Запрос {i+1}: {'✅ OK' if result['success'] else '❌ FAIL'} "
                      f"({result['response_time_ms']:.0f}ms)")
                
                if response.status_code == 429:
                    print(f"    🚫 Rate limited: {response.text}")
                    break
                
                # Небольшая пауза между запросами
                time.sleep(0.1)
                
            except Exception as e:
                print(f"  Запрос {i+1}: ❌ ERROR - {e}")
                results.append({
                    'request_number': i + 1,
                    'success': False,
                    'error': str(e)
                })
                break
        
        return {
            'test_results': results,
            'total_requests': len(results),
            'successful_requests': sum(1 for r in results if r.get('success', False)),
            'rate_limited': any(r.get('status_code') == 429 for r in results),
        }
    
    def monitor_continuous(self, interval_seconds: int = 60, duration_minutes: int = 10):
        """Непрерывный мониторинг API."""
        
        print(f"🔍 Начинаем мониторинг PubMed E-utilities API")
        print(f"⏱️ Интервал: {interval_seconds} секунд")
        print(f"⏳ Продолжительность: {duration_minutes} минут")
        print(f"🔑 API ключ: {'Настроен' if self.api_key else 'Не настроен'}")
        print("=" * 60)
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        results = []
        
        try:
            while time.time() < end_time:
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"[{timestamp}] Проверяем API...", end=" ")
                
                result = self.check_rate_limits()
                results.append(result)
                
                if result['current_status'] == 'healthy':
                    print("✅ OK", end="")
                    
                    if 'response_time_ms' in result:
                        print(f" ({result['response_time_ms']:.0f}ms)", end="")
                    
                    if 'usage_percent' in result:
                        status_emoji = {
                            'good': '🟢',
                            'warning': '🟡', 
                            'critical': '🔴'
                        }
                        print(f" {status_emoji.get(result['status'], '⚪')} {result['usage_percent']}%", end="")
                else:
                    print("❌ ERROR")
                    if 'error' in result:
                        print(f"   Ошибка: {result['error']}")
                
                print()  # Новая строка
                
                # Ждем до следующей проверки
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\n⏹️ Мониторинг прерван пользователем")
        
        # Сохраняем результаты
        self._save_results(results)
        self._print_summary(results)
    
    def _save_results(self, results: list):
        """Сохраняет результаты мониторинга в файл."""
        
        # Получаем путь к корню проекта (на 3 уровня выше)
        project_root = Path(__file__).parent.parent.parent.parent
        reports_dir = project_root / "reports"
        reports_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = reports_dir / f"monitor_pubmed_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        print(f"📊 Результаты сохранены в {filename}")
    
    def _print_summary(self, results: list):
        """Выводит сводку результатов мониторинга."""
        
        if not results:
            return
        
        total_checks = len(results)
        successful_checks = sum(1 for r in results if r.get('current_status') == 'healthy')
        success_rate = (successful_checks / total_checks) * 100
        
        print("\n" + "=" * 60)
        print("📈 СВОДКА МОНИТОРИНГА")
        print("=" * 60)
        print(f"Всего проверок: {total_checks}")
        print(f"Успешных: {successful_checks}")
        print(f"Процент успеха: {success_rate:.1f}%")
        
        # Среднее время ответа
        response_times = [r.get('response_time_ms', 0) for r in results if 'response_time_ms' in r]
        if response_times:
            avg_response_time = sum(response_times) / len(response_times)
            print(f"Среднее время ответа: {avg_response_time:.0f}ms")
        
        # Анализ использования лимитов
        usage_percents = [r.get('usage_percent', 0) for r in results if 'usage_percent' in r]
        if usage_percents:
            max_usage = max(usage_percents)
            avg_usage = sum(usage_percents) / len(usage_percents)
            print(f"Максимальное использование: {max_usage:.1f}%")
            print(f"Среднее использование: {avg_usage:.1f}%")
        
        print("\n💡 РЕКОМЕНДАЦИИ:")
        
        if success_rate < 95:
            print("⚠️ Низкий процент успеха - проверьте подключение к интернету")
        
        if response_times and avg_response_time > 5000:
            print("⚠️ Медленные ответы API - возможны проблемы с сетью")
        
        if usage_percents and max_usage > 80:
            print("⚠️ Высокое использование лимитов - рассмотрите получение API ключа")
        
        if not self.api_key:
            print("💡 Получите API ключ для увеличения лимитов:")
            print("   https://www.ncbi.nlm.nih.gov/account/")
            print("   Лимиты: 3 запроса/сек без ключа, 10 запросов/сек с ключом")


def main():
    """Основная функция."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Мониторинг PubMed E-utilities API")
    parser.add_argument("--single", action="store_true", help="Выполнить одну проверку")
    parser.add_argument("--test-limits", action="store_true", help="Агрессивно протестировать лимиты")
    parser.add_argument("--interval", type=int, default=60, help="Интервал между проверками (секунды)")
    parser.add_argument("--duration", type=int, default=10, help="Продолжительность мониторинга (минуты)")
    
    args = parser.parse_args()
    
    monitor = PubMedMonitor()
    
    if args.test_limits:
        print("🧪 Агрессивное тестирование лимитов...")
        result = monitor.test_rate_limits_aggressively()
        
        print(f"\n📊 Результаты тестирования:")
        print(f"Всего запросов: {result['total_requests']}")
        print(f"Успешных: {result['successful_requests']}")
        print(f"Rate limited: {'Да' if result['rate_limited'] else 'Нет'}")
        
        if not monitor.api_key and result['rate_limited']:
            print("\n💡 Рекомендация: Получите API ключ для увеличения лимитов")
        
    elif args.single:
        print("🔍 Выполняем одну проверку API...")
        result = monitor.check_rate_limits()
        
        print(f"Статус: {'✅ OK' if result['current_status'] == 'healthy' else '❌ ERROR'}")
        
        if 'response_time_ms' in result:
            print(f"Время ответа: {result['response_time_ms']:.0f}ms")
        
        if 'usage_percent' in result:
            print(f"Использование лимитов: {result['usage_percent']:.1f}%")
        
        if 'error' in result:
            print(f"Ошибка: {result['error']}")
    else:
        monitor.monitor_continuous(args.interval, args.duration)


if __name__ == "__main__":
    main()
