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
import sys
import time
from pathlib import Path

import requests

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from library.logging_setup import get_logger

logger = get_logger(__name__)


def check_api_key_status(api_key: str | None = None) -> bool:
    """Проверяет статус API ключа PubMed."""
    
    if not api_key:
        api_key = os.environ.get('PUBMED_API_KEY')
    
    if not api_key:
        logger.info("❌ API ключ PubMed не найден.")
        logger.info("Установите переменную окружения PUBMED_API_KEY")
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
            logger.info("✅ API ключ PubMed работает корректно!")
            logger.info(f"Статус: {response.status_code}")
            
            # Проверяем содержимое ответа
            try:
                data = response.json()
                if 'result' in data:
                    logger.info("✅ Получены данные из PubMed")
                else:
                    logger.info("⚠️ Ответ получен, но структура данных неожиданная")
            except ValueError:
                logger.info("⚠️ Ответ получен, но не в JSON формате")
                
            return True
            
        elif response.status_code == 429:
            logger.info("⚠️ Превышен лимит запросов")
            logger.info("Попробуйте снова через несколько секунд")
            
            # Проверяем текст ошибки для получения информации о лимитах
            if 'limit' in response.text and 'count' in response.text:
                logger.info("Информация о лимитах:")
                logger.info(response.text)
            
            return False
            
        elif response.status_code == 403:
            logger.info("❌ API ключ недействителен или неверен")
            return False
            
        else:
            logger.info(f"❌ Неожиданный статус: {response.status_code}")
            logger.info(f"Ответ: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка при проверке API ключа: {e}")
        return False


def test_rate_limits(api_key: str) -> None:
    """Тестирует лимиты API с ключом и без."""
    
    logger.info("\n🧪 Тестирование лимитов API...")
    
    # Тест без ключа
    logger.info("Тест без API ключа:")
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
                logger.info(f"  Запрос {i+1}: ✅ OK")
            elif response.status_code == 429:
                logger.info(f"  Запрос {i+1}: ❌ Rate limited")
                break
            else:
                logger.info(f"  Запрос {i+1}: ⚠️ Статус {response.status_code}")
            
            time.sleep(1)  # Ждем 1 секунду между запросами
            
        except Exception as e:
            logger.error(f"  Запрос {i+1}: ❌ Ошибка: {e}")
            break
    
    time.sleep(2)  # Пауза перед тестом с ключом
    
    # Тест с ключом
    logger.info("\nТест с API ключом:")
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
                logger.info(f"  Запрос {i+1}: ✅ OK")
            elif response.status_code == 429:
                logger.info(f"  Запрос {i+1}: ❌ Rate limited")
                break
            else:
                logger.info(f"  Запрос {i+1}: ⚠️ Статус {response.status_code}")
            
            time.sleep(0.1)  # Ждем 0.1 секунды между запросами
            
        except Exception as e:
            logger.error(f"  Запрос {i+1}: ❌ Ошибка: {e}")
            break


def main():
    """Основная функция."""
    logger.info("🔍 Проверка API ключа PubMed")
    logger.info("=" * 50)
    
    api_key = os.environ.get('PUBMED_API_KEY')
    
    if api_key:
        logger.info(f"Найден API ключ: {api_key[:10]}...")
    else:
        logger.info("API ключ не найден в переменных окружения")
    
    logger.info("\nИнструкции по получению API ключа:")
    logger.info("1. Перейдите на https://www.ncbi.nlm.nih.gov/account/")
    logger.info("2. Создайте учетную запись My NCBI (если нет)")
    logger.info("3. Войдите в свой аккаунт")
    logger.info("4. Перейдите в Settings → API Key Management")
    logger.info("5. Создайте новый API ключ")
    logger.info("6. Установите переменную окружения:")
    logger.info("   export PUBMED_API_KEY=your_key_here")
    logger.info("   # или в Windows:")
    logger.info("   set PUBMED_API_KEY=your_key_here")
    
    if api_key:
        logger.info("\nПроверяем ключ...")
        if check_api_key_status(api_key):
            logger.info("\n🎉 API ключ готов к использованию!")
            logger.info("Обновите конфигурацию, добавив ключ в заголовки:")
            logger.info("pubmed:")
            logger.info("  http:")
            logger.info("    headers:")
            logger.info("      api_key: '{PUBMED_API_KEY}'")
            
            # Тестируем лимиты
            test_rate_limits(api_key)
        else:
            logger.info("\n❌ Проблема с API ключом")
            sys.exit(1)
    else:
        logger.info("\n💡 Получите API ключ и повторите проверку")
        sys.exit(0)


if __name__ == "__main__":
    main()
