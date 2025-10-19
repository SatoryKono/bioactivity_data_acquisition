#!/usr/bin/env python3
"""
Автоматическое дополнение файла маппинга ChEMBL -> UniProt
с использованием маппинга через UniProt API.

Этот скрипт:
1. Анализирует существующий файл маппинга
2. Находит недостающие записи
3. Получает UniProt ID через ChEMBL API и UniProt API
4. Обновляет файл маппинга
5. Генерирует отчет о покрытии
"""

import pandas as pd
import requests
import time
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UniProtMappingUpdater:
    """Класс для автоматического обновления маппинга ChEMBL -> UniProt."""
    
    def __init__(self, mapping_file: str = "data/mappings/chembl_uniprot_mapping.csv"):
        self.mapping_file = Path(mapping_file)
        self.uniprot_base_url = "https://rest.uniprot.org/uniprotkb"
        self.chembl_base_url = "https://www.ebi.ac.uk/chembl/api/data"
        self.rate_limit_delay = 0.1
        self.max_retries = 3
        self.timeout = 30
        
        # Создаем директорию для маппинга, если не существует
        self.mapping_file.parent.mkdir(parents=True, exist_ok=True)
        
    def load_existing_mapping(self) -> pd.DataFrame:
        """Загружает существующий файл маппинга."""
        if self.mapping_file.exists():
            df = pd.read_csv(self.mapping_file)
            logger.info(f"Загружен существующий маппинг: {len(df)} записей")
            return df
        else:
            logger.info("Создаем новый файл маппинга")
            return pd.DataFrame(columns=['target_chembl_id', 'uniprot_id', 'confidence_score', 'source'])
    
    def make_request_with_retry(self, url: str, params: Dict = None) -> Optional[Dict]:
        """Выполняет HTTP запрос с повторными попытками."""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, params=params, timeout=self.timeout)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limit
                    wait_time = 2 ** attempt
                    logger.warning(f"Rate limit, ждем {wait_time} секунд...")
                    time.sleep(wait_time)
                else:
                    logger.warning(f"HTTP {response.status_code} для {url}")
                    
            except Exception as e:
                logger.error(f"Ошибка запроса (попытка {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
        
        return None
    
    def get_uniprot_id_from_chembl_api(self, chembl_id: str) -> Optional[Tuple[str, str, float]]:
        """Получает UniProt ID из ChEMBL API."""
        url = f"{self.chembl_base_url}/target/{chembl_id}"
        params = {'format': 'json'}
        
        data = self.make_request_with_retry(url, params)
        if not data:
            return None
        
        # Ищем UniProt ID в cross_references
        target_components = data.get('target_components', [])
        
        for component in target_components:
            xrefs = component.get('target_component_xrefs', [])
            
            for xref in xrefs:
                if xref.get('xref_src_db') == 'UniProt':
                    uniprot_id = xref.get('xref_id')
                    if uniprot_id:
                        return uniprot_id, "chembl_api_direct", 0.95
        
        return None
    
    def get_uniprot_id_from_uniprot_search(self, chembl_id: str) -> Optional[Tuple[str, str, float]]:
        """Ищет UniProt ID через поиск в UniProt API."""
        search_queries = [
            f'database:(type:chembl {chembl_id})',
            f'xref:chembl-{chembl_id}',
            f'xref:{chembl_id}',
        ]
        
        for query in search_queries:
            url = f"{self.uniprot_base_url}/search"
            params = {
                'query': query,
                'format': 'json',
                'size': 1
            }
            
            data = self.make_request_with_retry(url, params)
            if not data:
                continue
                
            results = data.get('results', [])
            if results:
                entry = results[0]
                primary_id = entry.get('primaryAccession')
                
                if primary_id:
                    confidence = 0.90 if 'database:' in query else 0.85
                    return primary_id, "uniprot_api_search", confidence
        
        return None
    
    def find_uniprot_id(self, chembl_id: str) -> Optional[Tuple[str, str, float]]:
        """Находит UniProt ID для ChEMBL ID используя различные методы."""
        logger.info(f"Ищем UniProt ID для {chembl_id}")
        
        # Метод 1: Прямой запрос к ChEMBL API
        result = self.get_uniprot_id_from_chembl_api(chembl_id)
        if result:
            return result
        
        # Метод 2: Поиск в UniProt API
        result = self.get_uniprot_id_from_uniprot_search(chembl_id)
        if result:
            return result
        
        logger.warning(f"Не удалось найти UniProt ID для {chembl_id}")
        return None
    
    def update_mapping_for_targets(self, target_df: pd.DataFrame, force_update: bool = False) -> Dict:
        """Обновляет маппинг для списка таргетов."""
        logger.info("Начинаем обновление маппинга")
        
        # Загружаем существующий маппинг
        mapping_df = self.load_existing_mapping()
        
        # Определяем, какие записи нужно обновить
        target_chembl_ids = set(target_df['target_chembl_id'].tolist())
        existing_chembl_ids = set(mapping_df['target_chembl_id'].tolist())
        
        if force_update:
            ids_to_process = target_chembl_ids
        else:
            ids_to_process = target_chembl_ids - existing_chembl_ids
        
        logger.info(f"Обрабатываем {len(ids_to_process)} записей")
        
        stats = {
            'processed': 0,
            'new_mappings': 0,
            'updated_mappings': 0,
            'failed_mappings': 0
        }
        
        for i, chembl_id in enumerate(ids_to_process, 1):
            logger.info(f"Обрабатываем {chembl_id} ({i}/{len(ids_to_process)})")
            
            # Ищем UniProt ID
            result = self.find_uniprot_id(chembl_id)
            
            if result:
                uniprot_id, source, confidence = result
                
                # Проверяем, существует ли уже запись
                existing_record = mapping_df[mapping_df['target_chembl_id'] == chembl_id]
                
                if existing_record.empty:
                    # Новая запись
                    new_row = {
                        'target_chembl_id': chembl_id,
                        'uniprot_id': uniprot_id,
                        'confidence_score': confidence,
                        'source': source
                    }
                    mapping_df = pd.concat([mapping_df, pd.DataFrame([new_row])], ignore_index=True)
                    stats['new_mappings'] += 1
                    logger.info(f"Добавлена новая запись: {chembl_id} -> {uniprot_id}")
                else:
                    # Обновляем существующую запись
                    idx = mapping_df[mapping_df['target_chembl_id'] == chembl_id].index[0]
                    mapping_df.loc[idx, 'uniprot_id'] = uniprot_id
                    mapping_df.loc[idx, 'source'] = source
                    mapping_df.loc[idx, 'confidence_score'] = confidence
                    stats['updated_mappings'] += 1
                    logger.info(f"Обновлена запись: {chembl_id} -> {uniprot_id}")
            else:
                stats['failed_mappings'] += 1
                logger.warning(f"Не удалось найти маппинг для {chembl_id}")
            
            stats['processed'] += 1
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
        
        # Сохраняем обновленный маппинг
        mapping_df = mapping_df.sort_values('target_chembl_id').reset_index(drop=True)
        mapping_df.to_csv(self.mapping_file, index=False)
        logger.info(f"Маппинг сохранен: {self.mapping_file}")
        
        return stats
    
    def generate_mapping_report(self, target_df: pd.DataFrame) -> str:
        """Генерирует отчет о маппинге."""
        mapping_df = self.load_existing_mapping()
        
        target_chembl_ids = set(target_df['target_chembl_id'].tolist())
        mapping_chembl_ids = set(mapping_df['target_chembl_id'].tolist())
        
        coverage = len(target_chembl_ids & mapping_chembl_ids) / len(target_chembl_ids) * 100
        missing_ids = target_chembl_ids - mapping_chembl_ids
        
        # Статистика качества
        quality_stats = {
            'total_mappings': len(mapping_df),
            'sources': mapping_df['source'].value_counts().to_dict() if not mapping_df.empty else {},
            'confidence_stats': mapping_df['confidence_score'].describe().to_dict() if not mapping_df.empty else {},
        }
        
        report = f"""
=== ОТЧЕТ О МАППИНГЕ CHEMBL -> UNIPROT ===

ПОКРЫТИЕ:
- Всего таргетов: {len(target_chembl_ids)}
- С маппингом: {len(target_chembl_ids & mapping_chembl_ids)}
- Без маппинга: {len(missing_ids)}
- Покрытие: {coverage:.1f}%

КАЧЕСТВО МАППИНГА:
- Всего записей: {quality_stats['total_mappings']}

ИСТОЧНИКИ:
"""
        for source, count in quality_stats['sources'].items():
            report += f"- {source}: {count} записей\n"
        
        if quality_stats['confidence_stats']:
            report += f"""
УВЕРЕННОСТЬ:
- Среднее: {quality_stats['confidence_stats'].get('mean', 0):.3f}
- Медиана: {quality_stats['confidence_stats'].get('50%', 0):.3f}
- Мин: {quality_stats['confidence_stats'].get('min', 0):.3f}
- Макс: {quality_stats['confidence_stats'].get('max', 0):.3f}
"""
        else:
            report += "\nУВЕРЕННОСТЬ: Нет данных"
        
        if missing_ids:
            report += f"\nОТСУТСТВУЮЩИЕ МАППИНГИ:\n"
            for chembl_id in sorted(missing_ids):
                report += f"- {chembl_id}\n"
        
        return report

def main():
    """Основная функция."""
    print("=== АВТОМАТИЧЕСКОЕ ОБНОВЛЕНИЕ МАППИНГА CHEMBL -> UNIPROT ===")
    
    # Создаем экземпляр обновлятеля
    updater = UniProtMappingUpdater()
    
    # Загружаем данные о таргетах
    try:
        target_df = pd.read_csv("data/output/_targets/target_o.csv")
        print(f"Загружено {len(target_df)} записей о таргетах")
    except FileNotFoundError:
        print("Ошибка: Файл с данными о таргетах не найден")
        return
    
    # Генерируем отчет
    print("\n=== ОТЧЕТ ДО ОБНОВЛЕНИЯ ===")
    report = updater.generate_mapping_report(target_df)
    print(report)
    
    # Обновляем маппинг
    print("\n=== ОБНОВЛЕНИЕ МАППИНГА ===")
    stats = updater.update_mapping_for_targets(target_df)
    
    print(f"\n=== РЕЗУЛЬТАТ ОБНОВЛЕНИЯ ===")
    print(f"Обработано записей: {stats['processed']}")
    print(f"Добавлено новых: {stats['new_mappings']}")
    print(f"Обновлено: {stats['updated_mappings']}")
    print(f"Не удалось найти: {stats['failed_mappings']}")
    
    # Финальный отчет
    print("\n=== ОТЧЕТ ПОСЛЕ ОБНОВЛЕНИЯ ===")
    final_report = updater.generate_mapping_report(target_df)
    print(final_report)
    
    # Показываем содержимое файла маппинга
    mapping_df = updater.load_existing_mapping()
    print("\n=== СОДЕРЖИМОЕ ФАЙЛА МАППИНГА ===")
    if not mapping_df.empty:
        print(f"Всего записей: {len(mapping_df)}")
        print("\nПервые 10 записей:")
        print(mapping_df.head(10).to_string(index=False))
    else:
        print("Файл маппинга пуст")

if __name__ == "__main__":
    main()
