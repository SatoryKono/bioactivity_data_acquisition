#!/usr/bin/env python3
"""Демонстрационный скрипт для тестирования корреляционного анализа документов."""

import sys
from pathlib import Path

# Добавляем src в путь для импорта
src_dir = Path(__file__).parent.parent
sys.path.insert(0, str(src_dir))

from library.documents.config import load_document_config
from library.documents.pipeline import run_document_etl, read_document_input, write_document_outputs


def main():
    """Запуск демонстрации корреляционного анализа документов."""
    
    print("=== Демонстрация корреляционного анализа документов ===\n")
    
    # Загружаем конфигурацию с включенным корреляционным анализом
    config_path = Path("configs/config_documents_full.yaml")
    if not config_path.exists():
        print(f"Ошибка: Файл конфигурации {config_path} не найден")
        return 1
    
    try:
        config = load_document_config(config_path)
        print(f"[OK] Конфигурация загружена из {config_path}")
        print(f"[OK] Корреляционный анализ: {'включен' if config.postprocess.correlation.enabled else 'отключен'}")
        print(f"[OK] QC анализ: {'включен' if config.postprocess.qc.enabled else 'отключен'}")
        print()
        
        # Читаем входные данные
        input_path = config.io.input.documents_csv
        if not input_path.exists():
            print(f"Ошибка: Входной файл {input_path} не найден")
            return 1
            
        print(f"[OK] Читаем входные данные из {input_path}")
        input_frame = read_document_input(input_path)
        print(f"[OK] Загружено {len(input_frame)} документов")
        print()
        
        # Ограничиваем количество для демонстрации
        demo_limit = 10
        if len(input_frame) > demo_limit:
            input_frame = input_frame.head(demo_limit)
            print(f"[OK] Ограничиваем до {demo_limit} документов для демонстрации")
            print()
        
        # Запускаем ETL pipeline
        print("[RUN] Запускаем ETL pipeline с корреляционным анализом...")
        result = run_document_etl(config, input_frame)
        
        print("[OK] ETL завершен")
        print(f"[OK] Обработано документов: {len(result.documents)}")
        print(f"[OK] QC метрики: {len(result.qc)} записей")
        
        if result.correlation_analysis:
            print("[OK] Корреляционный анализ выполнен")
            print(f"[OK] Корреляционные отчеты: {len(result.correlation_reports) if result.correlation_reports else 0}")
            print(f"[OK] Инсайты: {len(result.correlation_insights) if result.correlation_insights else 0}")
        else:
            print("[WARN] Корреляционный анализ не выполнен")
        print()
        
        # Сохраняем результаты
        output_dir = Path("data/output/correlation_demo")
        date_tag = "demo"
        
        print(f"[SAVE] Сохраняем результаты в {output_dir}...")
        outputs = write_document_outputs(result, output_dir, date_tag)
        
        print("[OK] Результаты сохранены:")
        for name, path in outputs.items():
            print(f"  - {name}: {path}")
        print()
        
        # Показываем краткую статистику
        if result.correlation_insights:
            print("[STATS] Краткая статистика корреляций:")
            high_severity = [i for i in result.correlation_insights if i.get('severity') == 'high']
            medium_severity = [i for i in result.correlation_insights if i.get('severity') == 'medium']
            
            print(f"  - Высокая важность: {len(high_severity)} инсайтов")
            print(f"  - Средняя важность: {len(medium_severity)} инсайтов")
            
            if high_severity:
                print("\n[CRITICAL] Критические корреляции:")
                for insight in high_severity[:3]:  # Показываем первые 3
                    print(f"  - {insight['message']}")
        
        print("\n[SUCCESS] Демонстрация завершена успешно!")
        return 0
        
    except Exception as e:
        print(f"[ERROR] Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
