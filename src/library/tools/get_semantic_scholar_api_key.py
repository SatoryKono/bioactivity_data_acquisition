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
import requests
import sys
from typing import Optional


def check_api_key_status(api_key: Optional[str] = None) -> bool:
    """Проверяет статус API ключа Semantic Scholar."""
    
    if not api_key:
        api_key = os.environ.get('SEMANTIC_SCHOLAR_API_KEY')
    
    if not api_key:
        print("❌ API ключ Semantic Scholar не найден.")
        print("Установите переменную окружения SEMANTIC_SCHOLAR_API_KEY")
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
            print("✅ API ключ Semantic Scholar работает корректно!")
            print(f"Статус: {response.status_code}")
            
            # Проверяем заголовки rate limiting
            rate_limit = response.headers.get('X-RateLimit-Limit')
            rate_remaining = response.headers.get('X-RateLimit-Remaining')
            
            if rate_limit:
                print(f"Лимит запросов: {rate_limit}")
            if rate_remaining:
                print(f"Осталось запросов: {rate_remaining}")
                
            return True
            
        elif response.status_code == 401:
            print("❌ API ключ недействителен или неверен")
            return False
            
        elif response.status_code == 429:
            print("⚠️ Превышен лимит запросов")
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                print(f"Попробуйте снова через {retry_after} секунд")
            return False
            
        else:
            print(f"❌ Неожиданный статус: {response.status_code}")
            print(f"Ответ: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка при проверке API ключа: {e}")
        return False


def main():
    """Основная функция."""
    print("🔍 Проверка API ключа Semantic Scholar")
    print("=" * 50)
    
    api_key = os.environ.get('SEMANTIC_SCHOLAR_API_KEY')
    
    if api_key:
        print(f"Найден API ключ: {api_key[:10]}...")
    else:
        print("API ключ не найден в переменных окружения")
    
    print("\nИнструкции по получению API ключа:")
    print("1. Перейдите на https://www.semanticscholar.org/product/api#api-key-form")
    print("2. Заполните форму запроса")
    print("3. Получите ключ по email")
    print("4. Установите переменную окружения:")
    print("   export SEMANTIC_SCHOLAR_API_KEY=your_key_here")
    print("   # или в Windows:")
    print("   set SEMANTIC_SCHOLAR_API_KEY=your_key_here")
    
    if api_key:
        print("\nПроверяем ключ...")
        if check_api_key_status(api_key):
            print("\n🎉 API ключ готов к использованию!")
            print("Обновите конфигурацию, добавив ключ в заголовки:")
            print("semantic_scholar:")
            print("  http:")
            print("    headers:")
            print("      x-api-key: '{SEMANTIC_SCHOLAR_API_KEY}'")
        else:
            print("\n❌ Проблема с API ключом")
            sys.exit(1)
    else:
        print("\n💡 Получите API ключ и повторите проверку")
        sys.exit(0)


if __name__ == "__main__":
    main()
