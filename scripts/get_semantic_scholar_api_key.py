#!/usr/bin/env python3
"""Скрипт для получения API ключа Semantic Scholar.

Semantic Scholar предоставляет API ключи для увеличения лимитов запросов.
Без ключа: 100 запросов в 5 минут
С ключом: 100 запросов в 5 минут (но с более высоким приоритетом)

Инструкции по получению ключа:
1. Перейдите на https://www.semanticscholar.org/product/api#api-key-form
2. Заполните форму запроса
3. Получите ключ по email
4. Установите переменную окружения SEMANTIC_SCHOLAR_API_KEY

Этот скрипт поможет вам проверить статус вашего ключа.
"""

import os
import sys
from pathlib import Path

import requests

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from library.logging_setup import get_logger

logger = get_logger(__name__)


def check_api_key_status(api_key: str | None = None) -> bool:
    """Проверяет статус API ключа Semantic Scholar."""
    
    if not api_key:
        api_key = os.environ.get('SEMANTIC_SCHOLAR_API_KEY')
    
    if not api_key:
        logger.info("❌ API ключ Semantic Scholar не найден.")
        logger.info("Установите переменную окружения SEMANTIC_SCHOLAR_API_KEY")
        return False
    
    # Тестовый запрос к API
    headers = {
        'x-api-key': api_key,
        'User-Agent': 'bioactivity-data-acquisition/0.1.0'
    }
    
    try:
        response = requests.get(
            'https://api.semanticscholar.org/graph/v1/paper/PMID:7154002',
            headers=headers,
            params={'fields': 'title'},
            timeout=30
        )
        
        if response.status_code == 200:
            logger.info("✅ API ключ Semantic Scholar работает корректно!")
            logger.info(f"Статус: {response.status_code}")
            
            # Проверяем заголовки rate limiting
            rate_limit = response.headers.get('X-RateLimit-Limit')
            rate_remaining = response.headers.get('X-RateLimit-Remaining')
            
            if rate_limit:
                logger.info(f"Лимит запросов: {rate_limit}")
            if rate_remaining:
                logger.info(f"Осталось запросов: {rate_remaining}")
                
            return True
            
        elif response.status_code == 401:
            logger.info("❌ API ключ недействителен или неверен")
            return False
            
        elif response.status_code == 429:
            logger.info("⚠️ Превышен лимит запросов")
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                logger.info(f"Попробуйте снова через {retry_after} секунд")
            return False
            
        else:
            logger.info(f"❌ Неожиданный статус: {response.status_code}")
            logger.info(f"Ответ: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка при проверке API ключа: {e}")
        return False


def main():
    """Основная функция."""
    logger.info("🔍 Проверка API ключа Semantic Scholar")
    logger.info("=" * 50)
    
    api_key = os.environ.get('SEMANTIC_SCHOLAR_API_KEY')
    
    if api_key:
        logger.info(f"Найден API ключ: {api_key[:10]}...")
    else:
        logger.info("API ключ не найден в переменных окружения")
    
    logger.info("\nИнструкции по получению API ключа:")
    logger.info("1. Перейдите на https://www.semanticscholar.org/product/api#api-key-form")
    logger.info("2. Заполните форму запроса")
    logger.info("3. Получите ключ по email")
    logger.info("4. Установите переменную окружения:")
    logger.info("   export SEMANTIC_SCHOLAR_API_KEY=your_key_here")
    logger.info("   # или в Windows:")
    logger.info("   set SEMANTIC_SCHOLAR_API_KEY=your_key_here")
    
    if api_key:
        logger.info("\nПроверяем ключ...")
        if check_api_key_status(api_key):
            logger.info("\n🎉 API ключ готов к использованию!")
            logger.info("Обновите конфигурацию, добавив ключ в заголовки:")
            logger.info("semantic_scholar:")
            logger.info("  http:")
            logger.info("    headers:")
            logger.info("      x-api-key: '{SEMANTIC_SCHOLAR_API_KEY}'")
        else:
            logger.info("\n❌ Проблема с API ключом")
            sys.exit(1)
    else:
        logger.info("\n💡 Получите API ключ и повторите проверку")
        sys.exit(0)


if __name__ == "__main__":
    main()
