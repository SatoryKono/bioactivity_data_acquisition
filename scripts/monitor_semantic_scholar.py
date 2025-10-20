#!/usr/bin/env python3
"""Мониторинг Semantic Scholar API для отслеживания лимитов и статуса.

Этот скрипт помогает мониторить использование Semantic Scholar API
и предупреждает о приближении к лимитам.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests  # type: ignore[import-not-found]

# Добавляем путь к src для импорта пакета library
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from library.logging_setup import get_logger

logger = get_logger(__name__)


class SemanticScholarMonitor:
    """Монитор для Semantic Scholar API."""
    
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ.get('SEMANTIC_SCHOLAR_API_KEY')
        self.base_url = "https://api.semanticscholar.org/graph/v1/paper"
        self.session = requests.Session()
        
        # Настройка заголовков
        self.headers = {
            'Accept': 'application/json',
            'User-Agent': 'bioactivity-data-acquisition/0.1.0'
        }
        
        if self.api_key:
            self.headers['x-api-key'] = self.api_key
    
    def test_api_call(self) -> dict[str, Any]:
        """Выполняет тестовый запрос к API."""
        
        # Тестовый PMID
        test_pmid = "PMID:7154002"
        
        try:
            response = self.session.get(
                f"{self.base_url}/{test_pmid}",
                headers=self.headers,
                params={'fields': 'title,abstract,year'},
                timeout=30
            )
            
            result = {
                'timestamp': datetime.now().isoformat(),
                'status_code': response.status_code,
                'success': response.status_code == 200,
                'response_time_ms': response.elapsed.total_seconds() * 1000,
            }
            
            # Анализ заголовков rate limiting
            rate_limit_headers = {
                'X-RateLimit-Limit': response.headers.get('X-RateLimit-Limit'),
                'X-RateLimit-Remaining': response.headers.get('X-RateLimit-Remaining'),
                'X-RateLimit-Reset': response.headers.get('X-RateLimit-Reset'),
                'Retry-After': response.headers.get('Retry-After'),
            }
            
            # Очищаем None значения
            result['rate_limits'] = {k: v for k, v in rate_limit_headers.items() if v is not None}
            
            if response.status_code == 200:
                data = response.json()
                result['data_received'] = {
                    'title': data.get('title', 'N/A'),
                    'year': data.get('year', 'N/A'),
                    'has_abstract': bool(data.get('abstract'))
                }
            else:
                result['error'] = response.text
            
            return result
            
        except requests.exceptions.RequestException as e:
            return {
                'timestamp': datetime.now().isoformat(),
                'success': False,
                'error': str(e),
                'error_type': type(e).__name__
            }
    
    def check_rate_limits(self) -> dict[str, Any]:
        """Проверяет текущие лимиты API."""
        
        result = self.test_api_call()
        
        if not result['success']:
            return result
        
        # Анализ rate limits
        rate_limits = result.get('rate_limits', {})
        
        analysis = {
            'api_key_configured': bool(self.api_key),
            'current_status': 'healthy' if result['success'] else 'error',
            'response_time_ms': result.get('response_time_ms', 0),
        }
        
        if 'X-RateLimit-Limit' in rate_limits and 'X-RateLimit-Remaining' in rate_limits:
            limit = int(rate_limits['X-RateLimit-Limit'])
            remaining = int(rate_limits['X-RateLimit-Remaining'])
            usage_percent = ((limit - remaining) / limit) * 100
            
            analysis.update({
                'rate_limit_total': limit,
                'rate_limit_remaining': remaining,
                'usage_percent': round(usage_percent, 1),
                'status': 'critical' if usage_percent > 90 else 'warning' if usage_percent > 70 else 'good'
            })
        
        return analysis
    
    def monitor_continuous(self, interval_seconds: int = 60, duration_minutes: int = 10):
        """Непрерывный мониторинг API."""
        
        logger.info("🔍 Начинаем мониторинг Semantic Scholar API")
        logger.info(f"⏱️ Интервал: {interval_seconds} секунд")
        logger.info(f"⏳ Продолжительность: {duration_minutes} минут")
        logger.info(f"🔑 API ключ: {'Настроен' if self.api_key else 'Не настроен'}")
        logger.info("=" * 60)
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        results = []
        
        try:
            while time.time() < end_time:
                timestamp = datetime.now().strftime("%H:%M:%S")
                logger.info(f"[{timestamp}] Проверяем API...")
                
                result = self.check_rate_limits()
                results.append(result)
                
                if result['current_status'] == 'healthy':
                    logger.info("✅ OK")
                    
                    if 'response_time_ms' in result:
                        logger.info(f" ({result['response_time_ms']:.0f}ms)")
                    
                    if 'usage_percent' in result:
                        status_emoji = {
                            'good': '🟢',
                            'warning': '🟡', 
                            'critical': '🔴'
                        }
                        logger.info(f" {status_emoji.get(result['status'], '⚪')} {result['usage_percent']}%")
                else:
                    logger.error("❌ ERROR")
                    if 'error' in result:
                        logger.error(f"   Ошибка: {result['error']}")
                
                print()  # Новая строка
                
                # Ждем до следующей проверки
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("\n⏹️ Мониторинг прерван пользователем")
        
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
        filename = reports_dir / f"monitor_semantic_scholar_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📊 Результаты сохранены в {filename}")
    
    def _print_summary(self, results: list):
        """Выводит сводку результатов мониторинга."""
        
        if not results:
            return
        
        total_checks = len(results)
        successful_checks = sum(1 for r in results if r['current_status'] == 'healthy')
        success_rate = (successful_checks / total_checks) * 100
        
        logger.info("\n" + "=" * 60)
        logger.info("📈 СВОДКА МОНИТОРИНГА")
        logger.info("=" * 60)
        logger.info(f"Всего проверок: {total_checks}")
        logger.info(f"Успешных: {successful_checks}")
        logger.info(f"Процент успеха: {success_rate:.1f}%")
        
        # Среднее время ответа
        response_times = [r.get('response_time_ms', 0) for r in results if 'response_time_ms' in r]
        if response_times:
            avg_response_time = sum(response_times) / len(response_times)
            logger.info(f"Среднее время ответа: {avg_response_time:.0f}ms")
        
        # Анализ использования лимитов
        usage_percents = [r.get('usage_percent', 0) for r in results if 'usage_percent' in r]
        if usage_percents:
            max_usage = max(usage_percents)
            avg_usage = sum(usage_percents) / len(usage_percents)
            logger.info(f"Максимальное использование: {max_usage:.1f}%")
            logger.info(f"Среднее использование: {avg_usage:.1f}%")
        
        logger.info("\n💡 РЕКОМЕНДАЦИИ:")
        
        if success_rate < 95:
            logger.info("⚠️ Низкий процент успеха - проверьте подключение к интернету")
        
        if response_times and avg_response_time > 5000:
            logger.info("⚠️ Медленные ответы API - возможны проблемы с сетью")
        
        if usage_percents and max_usage > 80:
            logger.info("⚠️ Высокое использование лимитов - рассмотрите получение API ключа")
        
        if not self.api_key:
            logger.info("💡 Получите API ключ для увеличения лимитов: https://www.semanticscholar.org/product/api#api-key-form")
def main():
    """Основная функция."""
    # logger уже инициализирован на уровне модуля

    parser = argparse.ArgumentParser(description="Мониторинг Semantic Scholar API")
    parser.add_argument("--single", action="store_true", help="Выполнить одну проверку")
    parser.add_argument("--interval", type=int, default=60, help="Интервал между проверками (секунды)")
    parser.add_argument("--duration", type=int, default=10, help="Продолжительность мониторинга (минуты)")
    
    args = parser.parse_args()
    
    monitor = SemanticScholarMonitor()
    
    if args.single:
        logger.info("🔍 Выполняем одну проверку API...")
        result = monitor.check_rate_limits()
        
        logger.info(f"Статус: {'✅ OK' if result['current_status'] == 'healthy' else '❌ ERROR'}")
        
        if 'response_time_ms' in result:
            logger.info(f"Время ответа: {result['response_time_ms']:.0f}ms")
        
        if 'usage_percent' in result:
            logger.info(f"Использование лимитов: {result['usage_percent']:.1f}%")
        
        if 'error' in result:
            logger.error(f"Ошибка: {result['error']}")
    else:
        monitor.monitor_continuous(args.interval, args.duration)


if __name__ == "__main__":
    main()
