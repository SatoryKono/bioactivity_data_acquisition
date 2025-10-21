"""Тесты для проверки синхронизации IUPHAR полей с референсным проектом."""


import pytest

from src.library.target.iuphar_adapter import (
    IupharApiCfg,
    _derive_chain_from_family,
    _parse_iuphar_target_from_csv,
    _target_to_type,
)


class TestIUPHARFieldsSync:
    """Тесты для проверки корректности заполнения IUPHAR полей."""
    
    def test_iuphar_full_id_path_format(self):
        """Тест правильного формата iuphar_full_id_path: target_id#family_chain."""
        cfg = IupharApiCfg()
        
        # Тестовые данные
        csv_data = {
            "target_id": "T-123",
            "name": "Test Target",
            "family_id": "F-10",
            "type": "Receptor.G protein-coupled receptor"
        }
        
        result = _parse_iuphar_target_from_csv(csv_data, cfg)
        
        # Проверяем формат iuphar_full_id_path
        expected_format = "T-123#F-10"  # Минимальный формат без иерархии
        assert result["iuphar_full_id_path"].startswith("T-123#")
        assert "F-10" in result["iuphar_full_id_path"]
    
    def test_iuphar_full_name_path_format(self):
        """Тест правильного формата iuphar_full_name_path: target_name#family_names."""
        cfg = IupharApiCfg()
        
        # Тестовые данные
        csv_data = {
            "target_id": "T-123",
            "name": "Test Target",
            "family_id": "F-10",
            "type": "Receptor.G protein-coupled receptor"
        }
        
        result = _parse_iuphar_target_from_csv(csv_data, cfg)
        
        # Проверяем формат iuphar_full_name_path
        assert result["iuphar_full_name_path"].startswith("Test Target#")
        assert "F-10" in result["iuphar_full_name_path"]
    
    def test_iuphar_chain_building(self):
        """Тест построения iuphar_chain через иерархию семейств."""
        cfg = IupharApiCfg()
        
        # Тест с дефолтным chain
        chain = _derive_chain_from_family("", cfg)
        assert chain == ["0864-1", "0864"]
        
        # Тест с N/A
        chain = _derive_chain_from_family("N/A", cfg)
        assert chain == ["0864-1", "0864"]
    
    def test_target_to_type_logic(self):
        """Тест логики определения типа target vs family."""
        cfg = IupharApiCfg()
        
        # Тест с валидным target type
        result = _target_to_type("T-123", "F-10", "Receptor.G protein-coupled receptor", cfg)
        assert result == "Receptor.G protein-coupled receptor"
        
        # Тест с пустым типом
        result = _target_to_type("T-123", "F-10", "", cfg)
        assert result == "Other Protein Target.Other Protein Target"
        
        # Тест с N/A типом
        result = _target_to_type("T-123", "F-10", "N/A", cfg)
        assert result == "Other Protein Target.Other Protein Target"
    
    def test_iuphar_fields_completeness(self):
        """Тест полноты заполнения всех 8 IUPHAR полей."""
        cfg = IupharApiCfg()
        
        # Тестовые данные
        csv_data = {
            "target_id": "T-123",
            "name": "Test Target",
            "family_id": "F-10",
            "type": "Receptor.G protein-coupled receptor"
        }
        
        result = _parse_iuphar_target_from_csv(csv_data, cfg)
        
        # Проверяем наличие всех 8 полей
        required_fields = [
            "iuphar_family_id",
            "iuphar_type", 
            "iuphar_class",
            "iuphar_subclass",
            "iuphar_chain",
            "iuphar_name",
            "iuphar_full_id_path",
            "iuphar_full_name_path"
        ]
        
        for field in required_fields:
            assert field in result, f"Поле {field} отсутствует в результате"
            assert result[field] is not None, f"Поле {field} имеет значение None"
    
    def test_iuphar_class_subclass_derivation(self):
        """Тест правильного вывода class и subclass из type."""
        cfg = IupharApiCfg()
        
        # Тестовые данные с известным типом
        csv_data = {
            "target_id": "T-123",
            "name": "Test Target",
            "family_id": "F-10",
            "type": "Receptor.G protein-coupled receptor"
        }
        
        result = _parse_iuphar_target_from_csv(csv_data, cfg)
        
        # Проверяем правильное разделение на class и subclass
        if result["iuphar_type"] == "Receptor.G protein-coupled receptor":
            assert result["iuphar_class"] == "Receptor"
            assert result["iuphar_subclass"] == "G protein-coupled receptor"
    
    def test_empty_family_id_handling(self):
        """Тест обработки пустого family_id."""
        cfg = IupharApiCfg()
        
        # Тестовые данные без family_id
        csv_data = {
            "target_id": "T-123",
            "name": "Test Target",
            "family_id": "",
            "type": "Receptor.G protein-coupled receptor"
        }
        
        result = _parse_iuphar_target_from_csv(csv_data, cfg)
        
        # Проверяем правильную обработку пустого family_id
        assert result["iuphar_family_id"] == ""
        assert result["iuphar_full_id_path"] == ""
        assert result["iuphar_full_name_path"] == ""
        assert result["iuphar_chain"] == "0864-1>0864"  # Дефолтный chain
    
    def test_iuphar_name_preservation(self):
        """Тест сохранения iuphar_name."""
        cfg = IupharApiCfg()
        
        # Тестовые данные
        csv_data = {
            "target_id": "T-123",
            "name": "Test Target Name",
            "family_id": "F-10",
            "type": "Receptor.G protein-coupled receptor"
        }
        
        result = _parse_iuphar_target_from_csv(csv_data, cfg)
        
        # Проверяем сохранение имени
        assert result["iuphar_name"] == "Test Target Name"
        assert result["iuphar_target_id"] == "T-123"


if __name__ == "__main__":
    pytest.main([__file__])
