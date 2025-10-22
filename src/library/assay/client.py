"""Client for the ChEMBL Assay API."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Generator
from datetime import datetime
from typing import Any

import pandas as pd
# Унифицированная логика обработки пустых значений доступна через library.utils.empty_value_handler

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
        """Retrieve multiple assays in batches for better performance (deprecated).

        Deprecated: use fetch_assays_batch_streaming for memory-efficient streaming.
        """
        logger.warning(
            "fetch_assays_batch is deprecated; use fetch_assays_batch_streaming for streaming batches"
        )
        
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

    def fetch_assays_batch_streaming(
        self,
        assay_ids: list[str],
        batch_size: int = 100,
    ) -> Generator[tuple[list[str], list[dict[str, Any]]], None, None]:
        """Stream assays in batches as a generator.

        Yields tuples: (requested_ids, list of parsed assay records).
        """

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        if not assay_ids:
            return

        for i in range(0, len(assay_ids), batch_size):
            batch = assay_ids[i : i + batch_size]
            batch_str = ",".join(batch)

            try:
                params = {
                    "assay_chembl_id__in": batch_str,
                    "format": "json",
                    "limit": len(batch),
                }

                payload = self._request("GET", "assay", headers=headers, params=params)

                batch_results: list[dict[str, Any]] = []
                if isinstance(payload, dict) and "assays" in payload:
                    for assay_data in payload["assays"]:
                        parsed_assay = self._parse_assay(assay_data)
                        batch_results.append(parsed_assay)
                else:
                    parsed_assay = self._parse_assay(payload)
                    batch_results.append(parsed_assay)

                yield (batch, batch_results)

            except Exception as e:
                logger.warning(f"Failed to fetch batch {batch}: {e}")
                # Yield empty parsed records for each id in the failed batch
                failed_results: list[dict[str, Any]] = []
                for assay_id in batch:
                    failed_results.append(self._create_empty_assay_record(assay_id, str(e)))
                yield (batch, failed_results)

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

    def fetch_assays_with_variants(
        self, 
        filters: dict[str, Any] | None = None,
        batch_size: int = 200
    ) -> list[dict[str, Any]]:
        """Retrieve assays with variants using variant_sequence__isnull=false filter."""
        
        params = {
            "variant_sequence__isnull": "false",
            "format": "json",
            "limit": batch_size
        }
        
        if filters:
            params.update(filters)
        
        logger.info(f"Fetching variant assays with params: {params}")
        
        assays = []
        offset = 0
        
        while True:
            params["offset"] = offset
            try:
                response = self._request("GET", "assay", params=params)
                page_data = response.get("assays", [])
                
                logger.info(f"Received {len(page_data)} assays at offset {offset}")
                
                # Log response structure for debugging
                if offset == 0:  # Only log for first page to avoid spam
                    logger.info(f"Response keys: {list(response.keys())}")
                    if page_data:
                        logger.info(f"First assay keys: {list(page_data[0].keys())}")
                        # Check for variant_sequence in first assay
                        first_assay = page_data[0]
                        if "variant_sequence" in first_assay:
                            logger.info(f"First assay has variant_sequence: {first_assay['variant_sequence']}")
                        else:
                            logger.info("First assay has no variant_sequence field")
                
                if not page_data:
                    logger.info(f"No more data at offset {offset}, stopping pagination")
                    break
                    
                for assay in page_data:
                    assay_data = self._parse_assay(assay)
                    assays.append(assay_data)
                
                offset += batch_size
                
                # Check if there are more pages
                if len(page_data) < batch_size:
                    logger.info(f"Received fewer items than batch size ({len(page_data)} < {batch_size}), stopping pagination")
                    break
                    
            except Exception as e:
                logger.error(f"Failed to fetch variant assays page at offset {offset}: {e}")
                break
        
        logger.info(f"Total variant assays fetched: {len(assays)}")
        return assays


    def fetch_target_components_batch(
        self, 
        target_ids: list[str], 
        batch_size: int = 50
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch target components in batches by target_chembl_id."""
        
        if not target_ids:
            return {}
        
        components_map = {}
        
        # Process in batches to avoid URL length limits
        for i in range(0, len(target_ids), batch_size):
            batch = target_ids[i:i + batch_size]
            batch_str = ",".join(batch)
            
            try:
                params = {
                    "target_chembl_id__in": batch_str,
                    "format": "json",
                    "limit": len(batch)
                }
                
                response = self._request("GET", "target_component", params=params)
                page_data = response.get("target_components", [])
                
                for component in page_data:
                    target_id = component.get("target_chembl_id")
                    if target_id:
                        if target_id not in components_map:
                            components_map[target_id] = []
                        components_map[target_id].append(component)
                        
            except Exception as e:
                logger.warning(f"Failed to fetch target components batch {batch}: {e}")
                # Create empty records for failed targets
                for target_id in batch:
                    components_map[target_id] = []
        
        return components_map


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
            assay_id = assay.get('assay_chembl_id', 'unknown')
            logger.debug(f"Parsing assay data: {assay_id}")
            
            # Log variant-related fields for debugging
            variant_seq = assay.get("variant_sequence")
            if variant_seq:
                logger.debug(f"Assay {assay_id} has variant_sequence: {variant_seq}")
            else:
                logger.debug(f"Assay {assay_id} has no variant_sequence field")
                
        except Exception as e:
            logger.error(f"Error parsing assay payload: {e}")
            raise
        
        # Ensure all required fields are present with proper defaults
        record: dict[str, Any | None] = {
            # Ключи и идентификаторы
            "assay_chembl_id": assay.get("assay_chembl_id"),
            "src_id": assay.get("src_id"),
            "src_name": pd.NA,  # Will be enriched later
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
            
            # Variant fields from variant_sequence (extracted from /assay endpoint)
            "variant_id": assay.get("variant_id") or assay.get("variant_chembl_id") or assay.get("variant_sequence_id"),
            "variant_text": assay.get("variant_text") or assay.get("variant_description") or assay.get("variant_comment"),
            "variant_sequence_id": assay.get("variant_sequence_id") or assay.get("variant_sequence") or assay.get("variant_seq_id"),
            
            # Extract nested variant_sequence fields
            "isoform": self._extract_variant_sequence_field(assay, "isoform"),
            "mutation": self._extract_variant_sequence_field(assay, "mutation"),
            "sequence": self._extract_variant_sequence_field(assay, "sequence"),
            "variant_accession": self._extract_variant_sequence_field(assay, "accession"),
            "variant_sequence_accession": self._extract_variant_sequence_field(assay, "accession"),
            "variant_sequence_mutation": self._extract_variant_sequence_field(assay, "mutation"),
            "variant_organism": self._extract_variant_sequence_field(assay, "organism"),
            
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
            "assay_parameters_json": "[]",  # Will be enriched later
            "assay_format": assay.get("assay_format"),
            
            # Техслужебные поля
            "source_system": "ChEMBL",
            "extracted_at": datetime.utcnow().isoformat(),
            "hash_row": pd.NA,  # Will be calculated later
            "hash_business_key": pd.NA,  # Will be calculated later
        }
        
        # Calculate hashes
        record["hash_business_key"] = self._calculate_business_key_hash(record)
        record["hash_row"] = self._calculate_row_hash(record)
        
        return record

    def _extract_variant_sequence_field(self, assay: dict[str, Any], field_name: str) -> Any:
        """Extract field from nested variant_sequence object."""
        variant_seq = assay.get("variant_sequence")
        if variant_seq and isinstance(variant_seq, dict):
            return variant_seq.get(field_name)
        return pd.NA

    def _parse_list_field(self, value: Any) -> list[str] | None:
        """Parse list field from ChEMBL response."""
        if value is None or (hasattr(value, '__len__') and len(value) == 0) or (not hasattr(value, '__len__') and pd.isna(value)):
            return pd.NA
        
        if isinstance(value, list):
            # Дедупликация и сортировка для детерминизма
            try:
                unique_items = list(set(str(item).strip() for item in value if str(item).strip()))
                return sorted(unique_items) if len(unique_items) > 0 else pd.NA
            except Exception as e:
                logger.warning(f"Error parsing list field {value}: {e}")
                return pd.NA
        
        if isinstance(value, str):
            # Парсинг строки как JSON или разделение по разделителям
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    unique_items = list(set(str(item).strip() for item in parsed if str(item).strip()))
                    return sorted(unique_items) if len(unique_items) > 0 else pd.NA
            except (json.JSONDecodeError, TypeError):
                pass
            
            # Разделение по запятым или точкам с запятой
            items = [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]
            unique_items = list(set(items))
            return sorted(unique_items) if len(unique_items) > 0 else pd.NA
        
        return pd.NA

    def _parse_parameters_field(self, value: Any) -> dict[str, Any] | str | None:
        """Parse assay parameters field."""
        if value is None or (hasattr(value, '__len__') and len(value) == 0) or (not hasattr(value, '__len__') and pd.isna(value)):
            return pd.NA
        
        if isinstance(value, dict):
            return value
        
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                # Если не JSON, возвращаем как строку
                return value
        
        return str(value) if value is not None else pd.NA

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
            "src_id": pd.NA,
            "src_name": pd.NA,
            "src_assay_id": pd.NA,
            
            # Классификация ассая
            "assay_type": pd.NA,
            "assay_type_description": pd.NA,
            "bao_format": pd.NA,
            "bao_label": pd.NA,
            "assay_category": pd.NA,
            "assay_classifications": pd.NA,
            
            # Связь с таргетом
            "target_chembl_id": pd.NA,
            "relationship_type": pd.NA,
            "confidence_score": pd.NA,
            
            # Variant fields
            "variant_id": pd.NA,
            "variant_text": pd.NA,
            "variant_sequence_id": pd.NA,
            "isoform": pd.NA,
            "mutation": pd.NA,
            "sequence": pd.NA,
            "variant_accession": pd.NA,
            "variant_organism": pd.NA,
            
            # Биологический контекст
            "assay_organism": pd.NA,
            "assay_tax_id": pd.NA,
            "assay_cell_type": pd.NA,
            "assay_tissue": pd.NA,
            "assay_strain": pd.NA,
            "assay_subcellular_fraction": pd.NA,
            
            # Описание и протокол
            "description": pd.NA,
            "assay_parameters": pd.NA,
            "assay_parameters_json": "[]",
            "assay_format": pd.NA,
            
            # Техслужебные поля
            "source_system": "ChEMBL",
            "extracted_at": datetime.utcnow().isoformat(),
            "hash_row": pd.NA,
            "hash_business_key": pd.NA,
        }
