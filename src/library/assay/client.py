"""Client for the ChEMBL Assay API."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from typing import Any

import pandas as pd

from library.clients.base import BaseApiClient
from library.config import APIClientConfig

logger = logging.getLogger(__name__)


class AssayChEMBLClient(BaseApiClient):
    """HTTP client for the ChEMBL assay endpoint."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def fetch_by_assay_id(self, assay_id: str) -> dict[str, Any]:
        """Retrieve an assay by its ChEMBL assay identifier."""
        
        # ChEMBL API иногда возвращает XML вместо JSON, поэтому добавляем дополнительные заголовки
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        try:
            payload = self._request("GET", f"assay/{assay_id}?format=json", headers=headers)
            return self._parse_assay(payload)
        except Exception as e:
            # В случае ошибки возвращаем пустую запись с информацией об ошибке
            logger.warning(f"Failed to fetch assay {assay_id}: {e}")
            return self._create_empty_assay_record(assay_id, str(e))

    def fetch_assays_batch(self, assay_ids: list[str], batch_size: int = 100) -> list[dict[str, Any]]:
        """Retrieve multiple assays in batches for better performance."""
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        all_assays = []
        
        # Разбиваем на батчи для оптимизации
        for i in range(0, len(assay_ids), batch_size):
            batch = assay_ids[i:i + batch_size]
            batch_str = ",".join(batch)
            
            try:
                params = {
                    "assay_chembl_id__in": batch_str,
                    "format": "json",
                    "limit": len(batch)
                }
                
                payload = self._request("GET", "assay", headers=headers, params=params)
                
                # Обрабатываем ответ
                if "assays" in payload:
                    for assay_data in payload["assays"]:
                        parsed_assay = self._parse_assay(assay_data)
                        all_assays.append(parsed_assay)
                else:
                    # Если ответ не содержит массив assays, обрабатываем как одиночный ассе
                    parsed_assay = self._parse_assay(payload)
                    all_assays.append(parsed_assay)
                    
            except Exception as e:
                logger.warning(f"Failed to fetch batch {batch}: {e}")
                # Создаем пустые записи для неудачных ассев
                for assay_id in batch:
                    empty_record = self._create_empty_assay_record(assay_id, str(e))
                    all_assays.append(empty_record)
        
        return all_assays

    def fetch_by_target_id(
        self, 
        target_chembl_id: str, 
        filters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Retrieve assays by target ChEMBL identifier with optional filters."""
        
        params = {
            "target_chembl_id": target_chembl_id,
            "format": "json",
            "limit": 200
        }
        
        if filters:
            params.update(filters)
        
        # Применяем профили фильтрации по умолчанию
        if "relationship_type" not in params:
            params["relationship_type"] = "D"  # Direct relationship
        if "assay_type" not in params:
            params["assay_type"] = "B,F"  # Binding and Functional
        
        assays = []
        offset = 0
        
        while True:
            params["offset"] = offset
            try:
                response = self._request("GET", "assay", params=params)
                page_data = response.get("assays", [])
                
                if not page_data:
                    break
                    
                for assay in page_data:
                    assay_data = self._parse_assay(assay)
                    assays.append(assay_data)
                
                offset += 200
                
                # Проверяем, есть ли еще страницы
                if len(page_data) < 200:
                    break
                    
            except Exception as e:
                logger.error(f"Failed to fetch assays page at offset {offset}: {e}")
                break
        
        return assays

    def fetch_source_info(self, src_id: int) -> dict[str, Any]:
        """Retrieve source information by source ID."""
        
        try:
            response = self._request("GET", f"source/{src_id}")
            return {
                "src_id": src_id,
                "src_name": response.get("src_description"),
                "src_short_name": response.get("src_short_name"),
                "src_url": response.get("src_url")
            }
        except Exception as e:
            logger.warning(f"Failed to fetch source {src_id}: {e}")
            return {
                "src_id": src_id,
                "src_name": None,
                "src_short_name": None,
                "src_url": None
            }

    def get_chembl_status(self) -> dict[str, Any]:
        """Get ChEMBL status and release information."""
        
        try:
            response = self._request("GET", "status")
            return {
                "chembl_release": response.get("chembl_release"),
                "status": response.get("status"),
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get ChEMBL status: {e}")
            raise

    def _parse_assay(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse assay data from ChEMBL API response."""
        
        try:
            assay = dict(payload)
            logger.debug(f"Parsing assay data: {assay.get('assay_chembl_id', 'unknown')}")
        except Exception as e:
            logger.error(f"Error parsing assay payload: {e}")
            raise
        
        # Ensure all required fields are present with proper defaults
        record: dict[str, Any | None] = {
            # Ключи и идентификаторы
            "assay_chembl_id": assay.get("assay_chembl_id"),
            "src_id": assay.get("src_id"),
            "src_assay_id": assay.get("src_assay_id"),
            
            # Классификация ассая
            "assay_type": assay.get("assay_type"),
            "assay_type_description": assay.get("assay_type_description"),
            "bao_format": assay.get("bao_format"),
            "bao_label": assay.get("bao_label"),
            "assay_category": self._parse_list_field(assay.get("assay_category")),
            "assay_classifications": self._parse_list_field(assay.get("assay_classifications")),
            
            # Связь с таргетом
            "target_chembl_id": assay.get("target_chembl_id"),
            "relationship_type": assay.get("relationship_type"),
            "confidence_score": assay.get("confidence_score"),
            
            # Биологический контекст
            "assay_organism": assay.get("assay_organism"),
            "assay_tax_id": assay.get("assay_tax_id"),
            "assay_cell_type": assay.get("assay_cell_type"),
            "assay_tissue": assay.get("assay_tissue"),
            "assay_strain": assay.get("assay_strain"),
            "assay_subcellular_fraction": assay.get("assay_subcellular_fraction"),
            
            # Описание и протокол
            "description": assay.get("description"),
            "assay_parameters": self._parse_parameters_field(assay.get("assay_parameters")),
            "assay_format": assay.get("assay_format"),
            
            # Техслужебные поля
            "source_system": "ChEMBL",
            "extracted_at": datetime.utcnow().isoformat(),
            "hash_row": None,  # Will be calculated later
            "hash_business_key": None,  # Will be calculated later
        }
        
        # Calculate hashes
        record["hash_business_key"] = self._calculate_business_key_hash(record)
        record["hash_row"] = self._calculate_row_hash(record)
        
        return record

    def _parse_list_field(self, value: Any) -> list[str] | None:
        """Parse list field from ChEMBL response."""
        if value is None or (hasattr(value, '__len__') and len(value) == 0) or (not hasattr(value, '__len__') and pd.isna(value)):
            return None
        
        if isinstance(value, list):
            # Дедупликация и сортировка для детерминизма
            try:
                unique_items = list(set(str(item).strip() for item in value if str(item).strip()))
                return sorted(unique_items) if len(unique_items) > 0 else None
            except Exception as e:
                logger.warning(f"Error parsing list field {value}: {e}")
                return None
        
        if isinstance(value, str):
            # Парсинг строки как JSON или разделение по разделителям
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    unique_items = list(set(str(item).strip() for item in parsed if str(item).strip()))
                    return sorted(unique_items) if len(unique_items) > 0 else None
            except (json.JSONDecodeError, TypeError):
                pass
            
            # Разделение по запятым или точкам с запятой
            items = [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]
            unique_items = list(set(items))
            return sorted(unique_items) if len(unique_items) > 0 else None
        
        return None

    def _parse_parameters_field(self, value: Any) -> dict[str, Any] | str | None:
        """Parse assay parameters field."""
        if value is None or (hasattr(value, '__len__') and len(value) == 0) or (not hasattr(value, '__len__') and pd.isna(value)):
            return None
        
        if isinstance(value, dict):
            return value
        
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                # Если не JSON, возвращаем как строку
                return value
        
        return str(value) if value is not None else None

    def _calculate_business_key_hash(self, record: dict[str, Any]) -> str:
        """Calculate hash for business key (assay_chembl_id)."""
        business_key = record.get("assay_chembl_id", "")
        return hashlib.sha256(str(business_key).encode()).hexdigest()[:16]

    def _calculate_row_hash(self, record: dict[str, Any]) -> str:
        """Calculate hash for entire row for deduplication."""
        # Создаем строку из всех значений, исключая хеши
        row_data = {k: v for k, v in record.items() if not k.startswith("hash_")}
        row_string = json.dumps(row_data, sort_keys=True, default=str)
        return hashlib.sha256(row_string.encode()).hexdigest()[:16]

    def _create_empty_assay_record(self, assay_id: str, error_msg: str) -> dict[str, Any]:
        """Создает пустую запись для случая ошибки."""
        return {
            # Ключи и идентификаторы
            "assay_chembl_id": assay_id,
            "src_id": None,
            "src_assay_id": None,
            
            # Классификация ассая
            "assay_type": None,
            "assay_type_description": None,
            "bao_format": None,
            "bao_label": None,
            "assay_category": None,
            "assay_classifications": None,
            
            # Связь с таргетом
            "target_chembl_id": None,
            "relationship_type": None,
            "confidence_score": None,
            
            # Биологический контекст
            "assay_organism": None,
            "assay_tax_id": None,
            "assay_cell_type": None,
            "assay_tissue": None,
            "assay_strain": None,
            "assay_subcellular_fraction": None,
            
            # Описание и протокол
            "description": None,
            "assay_parameters": None,
            "assay_format": None,
            
            # Техслужебные поля
            "source_system": "ChEMBL",
            "extracted_at": datetime.utcnow().isoformat(),
            "hash_row": None,
            "hash_business_key": None,
        }
