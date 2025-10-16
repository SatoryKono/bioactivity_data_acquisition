#!/usr/bin/env python3
"""Скрипт для получения API ключа PubMed.

PubMed E-utilities предоставляет API ключи для увеличения лимитов запросов.
Без ключа: 3 запроса в секунду
С ключом: 10 запросов в секунду

Инструкции по получению ключа:
1. Перейдите на https://ncbiinsights.ncbi.nlm.nih.gov/2017/11/02/new-api-keys-for-the-e-utilities/
2. Создайте учетную запись My NCBI
3. Получите API ключ в настройках
4. Установите переменную окружения PUBMED_API_KEY

Этот скрипт поможет вам проверить статус вашего ключа.
"""

import os
import requests
import sys
import time
from typing import Optional


def check_api_key_status(api_key: Optional[str] = None) -> bool:
    """Проверяет статус API ключа PubMed."""
    
    if not api_key:
        api_key = os.environ.get('PUBMED_API_KEY')
    
    if not api_key:
        print("❌ API ключ PubMed не найден.")
        print("Установите переменную окружения PUBMED_API_KEY")
        return False
    
    # Тестовый запрос к E-utilities
    params = {
        'db': 'pubmed',
        'id': '7154002',  # Тестовый PMID
        'retmode': 'json',
        'api_key': api_key
    }
    
    headers = {
        'User-Agent': 'bioactivity-data-acquisition/0.1.0',
        'Accept': 'application/json'
    }
    
    try:
        response = requests.get(
            'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi',
            params=params,
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            print("✅ API ключ PubMed работает корректно!")
            print(f"Статус: {response.status_code}")
            
            # Проверяем содержимое ответа
            try:
                data = response.json()
                if 'result' in data:
                    print("✅ Получены данные из PubMed")
                else:
                    print("⚠️ Ответ получен, но структура данных неожиданная")
            except ValueError:
                print("⚠️ Ответ получен, но не в JSON формате")
                
            return True
            
        elif response.status_code == 429:
            print("⚠️ Превышен лимит запросов")
            print("Попробуйте снова через несколько секунд")
            
            # Проверяем текст ошибки для получения информации о лимитах
            if 'limit' in response.text and 'count' in response.text:
                print("Информация о лимитах:")
                print(response.text)
            
            return False
            
        elif response.status_code == 403:
            print("❌ API ключ недействителен или неверен")
            return False
            
        else:
            print(f"❌ Неожиданный статус: {response.status_code}")
            print(f"Ответ: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка при проверке API ключа: {e}")
        return False


def test_rate_limits(api_key: str) -> None:
    """Тестирует лимиты API с ключом и без."""
    
    print("\n🧪 Тестирование лимитов API...")
    
    # Тест без ключа
    print("Тест без API ключа:")
    for i in range(4):  # Попытаемся сделать 4 запроса
        params = {
            'db': 'pubmed',
            'id': '7154002',
            'retmode': 'json'
        }
        
        try:
            response = requests.get(
                'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi',
                params=params,
                headers={'User-Agent': 'bioactivity-data-acquisition/0.1.0'},
                timeout=10
            )
            
            if response.status_code == 200:
                print(f"  Запрос {i+1}: ✅ OK")
            elif response.status_code == 429:
                print(f"  Запрос {i+1}: ❌ Rate limited")
                break
            else:
                print(f"  Запрос {i+1}: ⚠️ Статус {response.status_code}")
            
            time.sleep(1)  # Ждем 1 секунду между запросами
            
        except Exception as e:
            print(f"  Запрос {i+1}: ❌ Ошибка: {e}")
            break
    
    time.sleep(2)  # Пауза перед тестом с ключом
    
    # Тест с ключом
    print("\nТест с API ключом:")
    for i in range(4):  # Попытаемся сделать 4 запроса
        params = {
            'db': 'pubmed',
            'id': '7154002',
            'retmode': 'json',
            'api_key': api_key
        }
        
        try:
            response = requests.get(
                'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi',
                params=params,
                headers={'User-Agent': 'bioactivity-data-acquisition/0.1.0'},
                timeout=10
            )
            
            if response.status_code == 200:
                print(f"  Запрос {i+1}: ✅ OK")
            elif response.status_code == 429:
                print(f"  Запрос {i+1}: ❌ Rate limited")
                break
            else:
                print(f"  Запрос {i+1}: ⚠️ Статус {response.status_code}")
            
            time.sleep(0.1)  # Ждем 0.1 секунды между запросами
            
        except Exception as e:
            print(f"  Запрос {i+1}: ❌ Ошибка: {e}")
            break


def main():
    """Основная функция."""
    print("🔍 Проверка API ключа PubMed")
    print("=" * 50)
    
    api_key = os.environ.get('PUBMED_API_KEY')
    
    if api_key:
        print(f"Найден API ключ: {api_key[:10]}...")
    else:
        print("API ключ не найден в переменных окружения")
    
    print("\nИнструкции по получению API ключа:")
    print("1. Перейдите на https://www.ncbi.nlm.nih.gov/account/")
    print("2. Создайте учетную запись My NCBI (если нет)")
    print("3. Войдите в свой аккаунт")
    print("4. Перейдите в Settings → API Key Management")
    print("5. Создайте новый API ключ")
    print("6. Установите переменную окружения:")
    print("   export PUBMED_API_KEY=your_key_here")
    print("   # или в Windows:")
    print("   set PUBMED_API_KEY=your_key_here")
    
    if api_key:
        print("\nПроверяем ключ...")
        if check_api_key_status(api_key):
            print("\n🎉 API ключ готов к использованию!")
            print("Обновите конфигурацию, добавив ключ в заголовки:")
            print("pubmed:")
            print("  http:")
            print("    headers:")
            print("      api_key: '{PUBMED_API_KEY}'")
            
            # Тестируем лимиты
            test_rate_limits(api_key)
        else:
            print("\n❌ Проблема с API ключом")
            sys.exit(1)
    else:
        print("\n💡 Получите API ключ и повторите проверку")
        sys.exit(0)


if __name__ == "__main__":
    main()
