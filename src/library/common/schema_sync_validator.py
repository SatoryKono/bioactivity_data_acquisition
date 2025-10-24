"""
Валидатор синхронизации схем Pandera с column_order в конфигах.

Проверяет при запуске пайплайна:
1. Все поля из column_order есть в схеме Pandera
2. Порядок полей в column_order совпадает с порядком в схеме
3. Логирует warning при расхождениях
4. Опционально: fail fast при критических расхождениях (strict mode)
"""

import logging
from typing import Dict, List, Any
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)


class SchemaSyncValidator:
    """Валидатор синхронизации схем Pandera с конфигурационными файлами."""
    
    def __init__(self, strict_mode: bool = False):
        """
        Инициализация валидатора.
        
        Args:
            strict_mode: Если True, пайплайн останавливается при критических расхождениях
        """
        self.strict_mode = strict_mode
        self.errors: List[str] = []
        self.warnings: List[str] = []
    
    def validate_entity_schema_sync(self, entity_type: str, config_path: Path, schema_module: Any) -> bool:
        """
        Валидирует синхронизацию схемы для конкретной сущности.
        
        Args:
            entity_type: Тип сущности (documents, testitems)
            config_path: Путь к конфигурационному файлу
            schema_module: Модуль схемы Pandera
            
        Returns:
            True если синхронизация корректна, False если есть проблемы
        """
        try:
            # Загружаем column_order из конфига
            config_columns = self._load_config_column_order(config_path)
            if not config_columns:
                self.errors.append(f"Не удалось загрузить column_order из {config_path}")
                return False
            
            # Загружаем колонки из схемы Pandera
            schema_columns = self._load_schema_columns(schema_module)
            if not schema_columns:
                self.errors.append(f"Не удалось загрузить колонки из схемы {entity_type}")
                return False
            
            # Проверяем синхронизацию
            is_synced = self._check_schema_sync(entity_type, config_columns, schema_columns)
            
            if is_synced:
                logger.info(f"Схема {entity_type} синхронизирована корректно: {len(config_columns)} колонок")
            else:
                logger.warning(f"Обнаружены расхождения в схеме {entity_type}")
                if self.strict_mode:
                    logger.error(f"Strict mode: пайплайн остановлен из-за расхождений в схеме {entity_type}")
                    return False
            
            return is_synced
            
        except Exception as e:
            error_msg = f"Ошибка валидации схемы {entity_type}: {e}"
            self.errors.append(error_msg)
            logger.error(error_msg)
            return False
    
    def _load_config_column_order(self, config_path: Path) -> List[str]:
        """Загружает column_order из конфигурационного файла."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            column_order = config.get('determinism', {}).get('column_order', [])
            return column_order
        except Exception as e:
            logger.error(f"Ошибка загрузки конфига {config_path}: {e}")
            return []
    
    def _load_schema_columns(self, schema_module: Any) -> List[str]:
        """Загружает колонки из схемы Pandera."""
        try:
            if hasattr(schema_module, 'get_schema'):
                schema = schema_module.get_schema()
                return list(schema.columns.keys())
            else:
                logger.error("Схема не содержит метод get_schema()")
                return []
        except Exception as e:
            logger.error(f"Ошибка загрузки схемы: {e}")
            return []
    
    def _check_schema_sync(self, entity_type: str, config_columns: List[str], schema_columns: List[str]) -> bool:
        """
        Проверяет синхронизацию между конфигом и схемой.
        
        Args:
            entity_type: Тип сущности
            config_columns: Колонки из конфига
            schema_columns: Колонки из схемы
            
        Returns:
            True если синхронизация корректна
        """
        config_set = set(config_columns)
        schema_set = set(schema_columns)
        
        # Проверяем, что все колонки из конфига есть в схеме
        missing_in_schema = config_set - schema_set
        if missing_in_schema:
            error_msg = f"{entity_type}: Колонки из column_order отсутствуют в схеме Pandera: {sorted(missing_in_schema)}"
            self.errors.append(error_msg)
            logger.error(error_msg)
        
        # Проверяем, что все колонки из схемы есть в конфиге (предупреждение)
        missing_in_config = schema_set - config_set
        if missing_in_config:
            warning_msg = f"{entity_type}: Колонки из схемы отсутствуют в column_order: {sorted(missing_in_config)}"
            self.warnings.append(warning_msg)
            logger.warning(warning_msg)
        
        # Проверяем порядок колонок
        common_columns = config_set & schema_set
        if common_columns:
            config_common = [col for col in config_columns if col in common_columns]
            schema_common = [col for col in schema_columns if col in common_columns]
            
            if config_common != schema_common:
                warning_msg = f"{entity_type}: Порядок общих колонок отличается между конфигом и схемой"
                self.warnings.append(warning_msg)
                logger.warning(warning_msg)
        
        # Возвращаем True только если нет критических ошибок
        return len(missing_in_schema) == 0
    
    def validate_all_schemas(self, configs_dir: Path) -> bool:
        """
        Валидирует синхронизацию всех схем.
        
        Args:
            configs_dir: Директория с конфигурационными файлами
            
        Returns:
            True если все схемы синхронизированы корректно
        """
        logger.info("Начинаем валидацию синхронизации схем Pandera с конфигами")
        
        all_valid = True
        
        # Валидируем документы
        doc_config_path = configs_dir / "config_document.yaml"
        if doc_config_path.exists():
            try:
                from library.schemas.document_schema_normalized import DocumentNormalizedSchema
                is_valid = self.validate_entity_schema_sync("documents", doc_config_path, DocumentNormalizedSchema)
                all_valid = all_valid and is_valid
            except ImportError as e:
                error_msg = f"Не удалось импортировать DocumentNormalizedSchema: {e}"
                self.errors.append(error_msg)
                logger.error(error_msg)
                all_valid = False
        else:
            warning_msg = f"Конфиг документов не найден: {doc_config_path}"
            self.warnings.append(warning_msg)
            logger.warning(warning_msg)
        
        # Валидируем теститемы
        testitem_config_path = configs_dir / "config_testitem.yaml"
        if testitem_config_path.exists():
            try:
                from library.schemas.testitem_schema_normalized import TestitemNormalizedSchema
                is_valid = self.validate_entity_schema_sync("testitems", testitem_config_path, TestitemNormalizedSchema)
                all_valid = all_valid and is_valid
            except ImportError as e:
                error_msg = f"Не удалось импортировать TestitemNormalizedSchema: {e}"
                self.errors.append(error_msg)
                logger.error(error_msg)
                all_valid = False
        else:
            warning_msg = f"Конфиг теститемов не найден: {testitem_config_path}"
            self.warnings.append(warning_msg)
            logger.warning(warning_msg)
        
        # Выводим итоговую статистику
        self._log_validation_summary()
        
        return all_valid
    
    def _log_validation_summary(self):
        """Выводит итоговую статистику валидации."""
        logger.info("=" * 80)
        logger.info("ИТОГИ ВАЛИДАЦИИ СИНХРОНИЗАЦИИ СХЕМ")
        logger.info("=" * 80)
        
        if self.errors:
            logger.error(f"Ошибок: {len(self.errors)}")
            for error in self.errors:
                logger.error(f"  ERROR: {error}")
        else:
            logger.info("Ошибок: 0")
        
        if self.warnings:
            logger.warning(f"Предупреждений: {len(self.warnings)}")
            for warning in self.warnings:
                logger.warning(f"  WARNING: {warning}")
        else:
            logger.info("Предупреждений: 0")
        
        if not self.errors and not self.warnings:
            logger.info("Все схемы синхронизированы корректно!")
        
        logger.info("=" * 80)
    
    def get_validation_report(self) -> Dict[str, Any]:
        """
        Возвращает отчет о валидации.
        
        Returns:
            Словарь с результатами валидации
        """
        return {
            "errors": self.errors,
            "warnings": self.warnings,
            "has_errors": len(self.errors) > 0,
            "has_warnings": len(self.warnings) > 0,
            "strict_mode": self.strict_mode
        }


def validate_schema_sync(configs_dir: Path, strict_mode: bool = False) -> bool:
    """
    Удобная функция для валидации синхронизации схем.
    
    Args:
        configs_dir: Директория с конфигурационными файлами
        strict_mode: Если True, пайплайн останавливается при критических расхождениях
        
    Returns:
        True если все схемы синхронизированы корректно
    """
    validator = SchemaSyncValidator(strict_mode=strict_mode)
    return validator.validate_all_schemas(configs_dir)


if __name__ == "__main__":
    # Пример использования
    import sys
    from pathlib import Path
    
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Путь к конфигам
    project_root = Path(__file__).parent.parent.parent
    configs_dir = project_root / "configs"
    
    # Валидация
    is_valid = validate_schema_sync(configs_dir, strict_mode=False)
    
    if is_valid:
        print("Валидация прошла успешно!")
        sys.exit(0)
    else:
        print("Обнаружены проблемы с синхронизацией схем!")
        sys.exit(1)
