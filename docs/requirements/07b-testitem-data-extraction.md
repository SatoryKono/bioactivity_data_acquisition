# Оптимальный подход к извлечению данных testitem из PubChem

## Executive Summary

### Обзор

PubChem служит опциональным, но важным источником обогащения данных testitem (молекул) в ETL пайплайнах биоактивности. ChEMBL остаётся **primary источником**, а PubChem обеспечивает:

- **Дополнительные структурные идентификаторы** (canonical SMILES, InChI, InChIKey)

- **Альтернативные молекулярные свойства** для верификации ChEMBL данных

- **Синонимы и номенклатуру** (IUPAC names, registry IDs)

- **Cross-references** на другие базы данных

### Ключевые метрики успеха

| Метрика | Target | Критичность |
|---------|--------|-------------|
| **CID resolution rate** | ≥85% | HIGH |

| **Properties enrichment rate** | ≥80% | HIGH |

| **Cache hit rate** | ≥60% | MEDIUM |

| **Avg request time** | <2.0 sec | MEDIUM |

| **Requests per second** | <5.0 (API limit) | CRITICAL |

| **Pipeline failure rate** | 0% (graceful degradation) | CRITICAL |

### Архитектурные принципы

1. ✅ **Optional by Design** — PubChem enrichment никогда не блокирует pipeline

2. ✅ **Graceful Degradation** — продолжение при любых PubChem сбоях

3. ✅ **Deterministic Output** — одинаковый input → одинаковый output

4. ✅ **Cache Aggressively** — минимизация API calls через многоуровневое кэширование

5. ✅ **Batch When Possible** — использование batch endpoints для эффективности

6. ✅ **Monitor Actively** — сбор метрик для проактивного выявления проблем

---

## 2. Рекомендуемая архитектура (Best of Both Worlds)

### 2.1 Компонентная модель

```text

┌──────────────────────────────────────────────────────────────┐
│               Testitem ETL Pipeline                           │
│                                                                │
│  ┌────────────────────────────────────────────────────────┐  │
│  │         ChEMBL Extraction (Primary Source)             │  │
│  │  • Molecule metadata                                    │  │
│  │  • Molecular properties                                 │  │
│  │  • Structure identifiers                                │  │
│  │  • Cross-references                                     │  │
│  └────────────────┬───────────────────────────────────────┘  │
│                   │                                            │
│                   ▼                                            │
│  ┌────────────────────────────────────────────────────────┐  │
│  │     PubChem Enrichment Layer (Optional)                │  │
│  │                                                          │  │
│  │  ┌──────────────────────────────────────────────────┐  │  │
│  │  │ CID Resolution Strategy                          │  │  │
│  │  │  1. Cache lookup                                 │  │  │

│  │  │  2. Direct CID                                   │  │  │

│  │  │  3. ChEMBL xrefs                                 │  │  │

│  │  │  4. InChIKey lookup                              │  │  │

│  │  │  5. SMILES lookup                                │  │  │

│  │  │  6. Name-based search                            │  │  │

│  │  └──────────────────────────────────────────────────┘  │  │
│  │                                                          │  │
│  │  ┌──────────────────────────────────────────────────┐  │  │
│  │  │ Batch Properties Fetcher (Proj1 approach)        │  │  │
│  │  │  • 100 CIDs per batch via comma-separated URL    │  │  │
│  │  │  • Fallback to individual on batch failure       │  │  │
│  │  └──────────────────────────────────────────────────┘  │  │
│  │                                                          │  │
│  │  ┌──────────────────────────────────────────────────┐  │  │
│  │  │ Smart Multi-Level Caching (Proj2 approach)       │  │  │
│  │  │  • Level 1: In-memory TTL cache                  │  │  │
│  │  │  • Level 2: Persistent CID mapping               │  │  │
│  │  │  • Level 3: File-based HTTP cache (debug)        │  │  │
│  │  └──────────────────────────────────────────────────┘  │  │
│  │                                                          │  │
│  │  ┌──────────────────────────────────────────────────┐  │  │
│  │  │ Parallel Workers (Proj2 approach)                │  │  │
│  │  │  • ThreadPoolExecutor for CID resolution         │  │  │
│  │  │  • Configurable worker count                     │  │  │
│  │  └──────────────────────────────────────────────────┘  │  │
│  └────────────────┬───────────────────────────────────────┘  │
│                   │                                            │
│                   ▼                                            │
│  ┌────────────────────────────────────────────────────────┐  │
│  │        HTTP Client with Resilience                     │  │
│  │                                                          │  │
│  │  • Rate Limiting: 5 requests/second max               │  │
│  │  • Retry + Exponential Backoff with Jitter            │  │

│  │  • Service Outage Detection & Tracking (Proj2)        │  │
│  │  • Connection Pooling                                  │  │
│  │  • Timeout Management                                  │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                                │
└──────────────────────────────────────────────────────────────┘

```text

### 2.2 Ключевые преимущества объединённого подхода

Оптимальное решение синтезирует лучшие практики из двух проектов:

| Компонент | Источник | Преимущество |
|-----------|----------|--------------|
| **Batch API calls** | Проект 1 | Эффективность: 100x меньше запросов |

| **Persistent CID cache** | Проект 2 | Долговременное кэширование (30 дней) |

| **Parallel processing** | Проект 2 | Скорость: 4x ускорение через ThreadPool |

| **File-based HTTP cache** | Проект 1 | Отладка: простой аудит запросов |

| **Service outage tracking** | Проект 2 | Надёжность: умный cooldown при сбоях |

| **Graceful degradation** | Оба | Устойчивость: продолжение при сбоях |

### 2.3 Почему не использовать только один подход

**Проект 1 (bioactivity_data_acquisition5):**

- ✅ Простая архитектура, легко понять

- ✅ Batch API для эффективности

- ✅ File cache для отладки

- ❌ Нет persistent CID cache → повторные lookups

- ❌ Sequential processing → медленнее

- ❌ Базовое error handling

**Проект 2 (ChEMBL_data_acquisition6):**

- ✅ Advanced caching strategy

- ✅ Parallel processing

- ✅ Sophisticated error handling

- ❌ Нет batch API → больше запросов

- ❌ Over-engineered для простых задач

- ❌ Сложная конфигурация

**Оптимальный подход:** лучшее из обоих миров.

---

## 3. PubChem API Integration

### 3.1 Используемые Endpoints

PubChem PUG-REST API предоставляет несколько endpoints для работы с молекулами. Рекомендуемые endpoints с приоритизацией:

| Endpoint | Назначение | Batch Support | Приоритет | Rate Limit |
|----------|-----------|---------------|-----------|------------|
| `/compound/cid/{cids}/property/...` | Основные свойства | ✅ Yes (comma-sep) | **HIGH** | 5 req/sec |

| `/compound/inchikey/{key}/cids/JSON` | CID по InChIKey | ❌ No | **HIGH** | 5 req/sec |

| `/compound/smiles/{smiles}/cids/JSON` | CID по SMILES | ❌ No | MEDIUM | 5 req/sec |
| `/compound/name/{name}/cids/JSON` | CID по названию | ❌ No | LOW | 5 req/sec |
| `/compound/cid/{cid}/synonyms/JSON` | Синонимы | ❌ No | LOW | 5 req/sec |
| `/compound/cid/{cid}/xrefs/RegistryID,RN/JSON` | Cross-references | ❌ No | LOW | 5 req/sec |
| `/compound/cid/{cid}/JSON` | Full record (fallback) | ❌ No | FALLBACK | 5 req/sec |

#### 3.1.1 Properties Endpoint (Primary)

**URL Pattern:**

```text

https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{cids}/property/{properties}/JSON

```text

**Batch Example:**

```text

/compound/cid/2244,3672,5353740/property/MolecularFormula,MolecularWeight,CanonicalSMILES,IsomericSMILES,InChI,InChIKey/JSON

```text

**Response Structure:**

```json

{
  "PropertyTable": {
    "Properties": [
      {
        "CID": 2244,
        "MolecularFormula": "C9H8O4",
        "MolecularWeight": 180.16,
        "CanonicalSMILES": "CC(=O)OC1=CC=CC=C1C(=O)O",
        "IsomericSMILES": "CC(=O)OC1=CC=CC=C1C(=O)O",
        "InChI": "InChI=1S/C9H8O4/c1-6(10)13-8-5-3-2-4-7(8)9(11)12/h2-5H,1H3,(H,11,12)",
        "InChIKey": "BSYNRYMUTXBXSQ-UHFFFAOYSA-N"
      }
    ]
  }
}

```text

**Max CIDs per batch:** 100 (рекомендуется для стабильности)

#### 3.1.2 CID Resolution Endpoints

**InChIKey Lookup:**

```text

/compound/inchikey/BSYNRYMUTXBXSQ-UHFFFAOYSA-N/cids/JSON
→ {"IdentifierList": {"CID": [2244]}}

```text

**SMILES Lookup:**

```text

/compound/smiles/CC(=O)OC1=CC=CC=C1C(=O)O/cids/JSON
→ {"IdentifierList": {"CID": [2244]}}

```text

**Name Lookup:**

```text

/compound/name/aspirin/cids/JSON
→ {"IdentifierList": {"CID": [2244]}}

```text

### 3.2 Рекомендуемый HTTP Client

Оптимальный клиент объединяет лучшие практики из обоих проектов:

```python

"""Optimal PubChem HTTP client combining best practices."""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from cachetools import TTLCache
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

class ServiceOutageTracker:
    """Tracks PubChem service availability (from Proj2)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._unavailable_until: float | None = None
        self._reason: str | None = None

    def mark_unavailable(self, seconds: float, reason: str = "rate_limit"):
        """Mark service unavailable for given seconds."""
        with self._lock:
            self._unavailable_until = datetime.now(timezone.utc).timestamp() + seconds
            self._reason = reason
            logger.warning(
                "pubchem_service_unavailable",
                unavailable_seconds=seconds,
                reason=reason
            )

    def is_available(self) -> bool:
        """Check if service is available."""
        with self._lock:
            if self._unavailable_until is None:
                return True
            if datetime.now(timezone.utc).timestamp() >= self._unavailable_until:
                logger.info("pubchem_service_restored", previous_reason=self._reason)
                self._unavailable_until = None
                self._reason = None
                return True
            return False

    def remaining_cooldown(self) -> float:
        """Get remaining cooldown seconds."""
        with self._lock:
            if self._unavailable_until is None:
                return 0.0
            remaining = self._unavailable_until - datetime.now(timezone.utc).timestamp()
            return max(0.0, remaining)

class OptimalPubChemClient:
    """
    Optimal PubChem client combining best practices from both projects.

    Features:

    - Batch requests для properties (Proj1)
    - Service outage tracking (Proj2)
    - Multi-level caching (both)
    - Parallel processing ready (Proj2)
    - Graceful degradation (both)
    """

    def __init__(
        self,
        base_url: str = "https://pubchem.ncbi.nlm.nih.gov/rest/pug",
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        cache_dir: Path | None = None,
        memory_cache_ttl: int = 3600,
        user_agent: str = "BioactivityETL/1.0 (contact@example.org)"
    ):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout

        # HTTP session with retry logic (Proj1 approach)

        self.session = self._create_session(max_retries, backoff_factor, user_agent)

        # Multi-level caching

        self._memory_cache = TTLCache(maxsize=1000, ttl=memory_cache_ttl)  # Proj2

        self._memory_cache_lock = threading.Lock()
        self._file_cache_dir = cache_dir  # Proj1 (optional for debugging)

        if self._file_cache_dir:
            self._file_cache_dir.mkdir(parents=True, exist_ok=True)

        # Service outage tracking (Proj2)

        self._outage_tracker = ServiceOutageTracker()

        # Metrics

        self._metrics = {
            'total_requests': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'rate_limit_hits': 0,
            'errors': 0
        }
        self._metrics_lock = threading.Lock()

    def _create_session(
        self,
        max_retries: int,
        backoff_factor: float,
        user_agent: str
    ) -> requests.Session:
        """Create requests session with retry logic."""
        session = requests.Session()

        # Retry strategy

        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )

        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=10,
            pool_maxsize=20
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Headers

        session.headers.update({
            'Accept': 'application/json',
            'User-Agent': user_agent
        })

        return session

    def _cache_key(self, path: str) -> str:
        """Generate cache key from request path."""
        key_source = f"{self.base_url}/{path.lstrip('/')}"
        return hashlib.sha256(key_source.encode('utf-8')).hexdigest()

    def _check_memory_cache(self, key: str) -> dict[str, Any] | None:
        """Check memory cache (Proj2 approach)."""
        with self._memory_cache_lock:
            return self._memory_cache.get(key)

    def _store_memory_cache(self, key: str, data: dict[str, Any]) -> None:
        """Store in memory cache."""
        with self._memory_cache_lock:
            self._memory_cache[key] = data

    def _check_file_cache(self, key: str) -> dict[str, Any] | None:
        """Check file cache (Proj1 approach, optional for debugging)."""
        if not self._file_cache_dir:
            return None

        cache_file = self._file_cache_dir / f"{key}.json"
        if not cache_file.exists():
            return None

        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.debug("file_cache_read_error", key=key, error=str(e))
            return None

    def _store_file_cache(self, key: str, data: dict[str, Any]) -> None:
        """Store in file cache."""
        if not self._file_cache_dir:
            return

        cache_file = self._file_cache_dir / f"{key}.json"
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.debug("file_cache_write_error", key=key, error=str(e))

    def _increment_metric(self, metric_name: str) -> None:
        """Increment metric counter."""
        with self._metrics_lock:
            self._metrics[metric_name] = self._metrics.get(metric_name, 0) + 1

    def _request(self, path: str) -> dict[str, Any]:
        """
        Make HTTP request with caching and error handling.

        Implements:

        - Multi-level cache checking
        - Service outage awareness
        - Rate limit handling
        - Graceful error handling
        """
        cache_key = self._cache_key(path)

        # Check memory cache first (fastest)

        cached = self._check_memory_cache(cache_key)
        if cached is not None:
            self._increment_metric('cache_hits')
            logger.debug("cache_hit", source="memory", path=path)
            return cached

        # Check file cache (if enabled)

        cached = self._check_file_cache(cache_key)
        if cached is not None:
            self._increment_metric('cache_hits')

            # Promote to memory cache

            self._store_memory_cache(cache_key, cached)
            logger.debug("cache_hit", source="file", path=path)
            return cached

        self._increment_metric('cache_misses')

        # Check service availability (Proj2 approach)

        if not self._outage_tracker.is_available():
            cooldown = self._outage_tracker.remaining_cooldown()
            logger.warning(
                "pubchem_service_cooldown",
                remaining_seconds=cooldown,
                path=path
            )
            raise ServiceUnavailableError(
                f"PubChem service unavailable for {cooldown:.1f}s"
            )

        # Make HTTP request

        url = f"{self.base_url}/{path.lstrip('/')}"
        self._increment_metric('total_requests')

        try:
            response = self.session.get(url, timeout=self.timeout)

            # Handle rate limiting (429)

            if response.status_code == 429:
                self._increment_metric('rate_limit_hits')
                retry_after = self._parse_retry_after(response)
                self._outage_tracker.mark_unavailable(retry_after, "rate_limit")
                raise RateLimitError(f"Rate limited, retry after {retry_after}s")

            # Handle service errors (503, 504)

            if response.status_code in (503, 504):
                self._increment_metric('errors')
                self._outage_tracker.mark_unavailable(60.0, f"http_{response.status_code}")
                raise ServiceUnavailableError(f"Service error: {response.status_code}")

            response.raise_for_status()
            data = response.json()

            # Store in caches

            self._store_memory_cache(cache_key, data)
            self._store_file_cache(cache_key, data)

            return data

        except requests.RequestException as e:
            self._increment_metric('errors')
            logger.error("pubchem_request_error", url=url, error=str(e))
            raise

    def _parse_retry_after(self, response: requests.Response) -> float:
        """Parse Retry-After header."""
        retry_after = response.headers.get('Retry-After')
        if not retry_after:
            return 60.0  # Default 1 minute

        try:
            return float(retry_after)
        except ValueError:

            # Could be HTTP date, but for simplicity use default

            return 60.0

    def fetch_properties_batch(
        self,
        cids: list[str],
        batch_size: int = 100
    ) -> dict[str, dict[str, Any]]:
        """
        Fetch properties for multiple CIDs using batch API (Proj1 approach).

        Args:
            cids: List of PubChem CIDs
            batch_size: Number of CIDs per batch (max 100 recommended)

        Returns:
            Dict mapping CID to properties
        """
        if not cids:
            return {}

        results = {}

        # Process in batches

        for i in range(0, len(cids), batch_size):
            batch = cids[i:i + batch_size]
            batch_str = ",".join(batch)

            try:
                path = (
                    f"compound/cid/{batch_str}/property/"
                    f"MolecularFormula,MolecularWeight,CanonicalSMILES,"
                    f"IsomericSMILES,InChI,InChIKey/JSON"
                )

                data = self._request(path)

                # Parse batch results

                if "PropertyTable" in data and "Properties" in data["PropertyTable"]:
                    for prop_data in data["PropertyTable"]["Properties"]:
                        cid = str(prop_data.get("CID", ""))
                        if cid:
                            results[cid] = self._parse_properties(prop_data)

                logger.info(
                    "batch_fetch_success",
                    batch_size=len(batch),
                    results_count=len(results)
                )

            except Exception as e:
                logger.warning(
                    "batch_fetch_failed",
                    batch=batch,
                    error=str(e)
                )

                # Fallback to individual requests

                for cid in batch:
                    try:
                        individual_result = self.fetch_properties(cid)
                        if individual_result:
                            results[cid] = individual_result
                    except Exception as e2:
                        logger.debug(
                            "individual_fetch_failed",
                            cid=cid,
                            error=str(e2)
                        )

        return results

    def fetch_properties(self, cid: str) -> dict[str, Any]:
        """Fetch properties for single CID."""
        try:
            path = (
                f"compound/cid/{cid}/property/"
                f"MolecularFormula,MolecularWeight,CanonicalSMILES,"
                f"IsomericSMILES,InChI,InChIKey/JSON"
            )
            data = self._request(path)

            if "PropertyTable" in data and "Properties" in data["PropertyTable"]:
                properties = data["PropertyTable"]["Properties"]
                if properties:
                    return self._parse_properties(properties[0])

            return {}

        except Exception as e:
            logger.warning("fetch_properties_failed", cid=cid, error=str(e))
            return {}

    def _parse_properties(self, prop_data: dict[str, Any]) -> dict[str, Any]:
        """Parse properties from PubChem response."""
        return {
            "pubchem_cid": prop_data.get("CID"),
            "pubchem_molecular_formula": prop_data.get("MolecularFormula"),
            "pubchem_molecular_weight": prop_data.get("MolecularWeight"),
            "pubchem_canonical_smiles": prop_data.get("CanonicalSMILES"),
            "pubchem_isomeric_smiles": prop_data.get("IsomericSMILES"),
            "pubchem_inchi": prop_data.get("InChI"),
            "pubchem_inchi_key": prop_data.get("InChIKey")
        }

    def fetch_cids_by_inchikey(self, inchikey: str) -> list[str]:
        """Fetch CIDs by InChIKey."""
        try:
            path = f"compound/inchikey/{inchikey}/cids/JSON"
            data = self._request(path)

            if "IdentifierList" in data and "CID" in data["IdentifierList"]:
                return [str(cid) for cid in data["IdentifierList"]["CID"]]

            return []

        except Exception as e:
            logger.debug("inchikey_lookup_failed", inchikey=inchikey, error=str(e))
            return []

    def fetch_cids_by_smiles(self, smiles: str) -> list[str]:
        """Fetch CIDs by SMILES."""
        try:
            encoded_smiles = quote(smiles, safe="")
            path = f"compound/smiles/{encoded_smiles}/cids/JSON"
            data = self._request(path)

            if "IdentifierList" in data and "CID" in data["IdentifierList"]:
                return [str(cid) for cid in data["IdentifierList"]["CID"]]

            return []

        except Exception as e:
            logger.debug("smiles_lookup_failed", smiles=smiles, error=str(e))
            return []

    def fetch_cids_by_name(self, name: str) -> list[str]:
        """Fetch CIDs by compound name."""
        try:
            encoded_name = quote(name, safe="")
            path = f"compound/name/{encoded_name}/cids/JSON"
            data = self._request(path)

            if "IdentifierList" in data and "CID" in data["IdentifierList"]:
                return [str(cid) for cid in data["IdentifierList"]["CID"]]

            return []

        except Exception as e:
            logger.debug("name_lookup_failed", name=name, error=str(e))
            return []

    def get_metrics(self) -> dict[str, Any]:
        """Get client metrics."""
        with self._metrics_lock:
            metrics = self._metrics.copy()

        # Add computed metrics

        total = metrics.get('total_requests', 0)
        if total > 0:
            metrics['cache_hit_rate'] = metrics.get('cache_hits', 0) / (
                metrics.get('cache_hits', 0) + metrics.get('cache_misses', 0)
            ) if (metrics.get('cache_hits', 0) + metrics.get('cache_misses', 0)) > 0 else 0.0
            metrics['error_rate'] = metrics.get('errors', 0) / total

        return metrics

class ServiceUnavailableError(Exception):
    """Raised when PubChem service is unavailable."""
    pass

class RateLimitError(Exception):
    """Raised when rate limit is hit."""
    pass

```text

### 3.3 Rate Limiting Strategy

PubChem имеет **soft limit 5 requests/second**. Превышение приводит к 429 responses.

**Рекомендуемая стратегия:**

```python

from time import sleep, monotonic

class RateLimiter:
    """Simple rate limiter for PubChem API."""

    def __init__(self, max_calls: int = 5, period: float = 1.0):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self._lock = threading.Lock()

    def wait_if_needed(self) -> None:
        """Wait if rate limit would be exceeded."""
        with self._lock:
            now = monotonic()

            # Remove old calls outside the period

            self.calls = [call_time for call_time in self.calls if now - call_time < self.period]

            if len(self.calls) >= self.max_calls:

                # Need to wait

                oldest_call = self.calls[0]
                wait_time = self.period - (now - oldest_call)
                if wait_time > 0:
                    logger.debug("rate_limit_wait", wait_seconds=wait_time)
                    sleep(wait_time)

            # Record this call

            self.calls.append(monotonic())

```text

---

## 4. Оптимальная стратегия поиска CID

### 4.1 Приоритизированный каскад

CID (PubChem Compound ID) — это ключевой идентификатор для получения данных из PubChem. Стратегия поиска использует каскадный подход с приоритизацией источников:

```python

"""Optimal CID resolution strategy combining both projects."""

from __future__ import annotations

import logging
from typing import Any
from enum import Enum

import pandas as pd

logger = logging.getLogger(__name__)

class CIDSource(str, Enum):
    """Source of CID resolution."""
    CACHE = "cache"
    DIRECT = "direct"
    XREF = "xref"
    INCHIKEY = "inchikey"
    SMILES_CANONICAL = "smiles_canonical"
    SMILES_ISOMERIC = "smiles_isomeric"
    NAME = "name"
    FAILED = "failed"

def resolve_pubchem_cid(
    molecule_data: dict[str, Any],
    cid_cache: dict[str, str],
    pubchem_client: OptimalPubChemClient
) -> tuple[str | None, CIDSource]:
    """
    Каскадный поиск PubChem CID с кэшированием.

    Порядок попыток (от самого быстрого к самому медленному):
    1. Cache lookup (проект 2 - мгновенно)
    2. Прямой pubchem_cid из данных (уже известен)
    3. ChEMBL cross-references (xref_sources)
    4. InChIKey lookup (проект 1 - самый надёжный)
    5. Canonical SMILES (приоритет)
    6. Isomeric SMILES (fallback)
    7. Name-based search (последняя надежда, самый медленный)

    Args:
        molecule_data: Данные молекулы из ChEMBL
        cid_cache: Persistent cache mapping identifiers -> CID
        pubchem_client: HTTP client для API запросов

    Returns:
        Tuple of (CID or None, source)

    Example:
        >>> cid, source = resolve_pubchem_cid(mol_data, cache, client)
        >>> if cid:
        ...     print(f"Found CID {cid} via {source}")
    """
    molecule_id = molecule_data.get('molecule_chembl_id', 'unknown')

    # Strategy 1: Cache lookup (Proj2 - fastest)

    # Check multiple cache keys for this molecule

    cache_keys = _generate_cache_keys(molecule_data)
    for cache_key in cache_keys:
        if cache_key in cid_cache:
            cached_cid = cid_cache[cache_key]
            if cached_cid:
                logger.debug(
                    "cid_resolution_cache_hit",
                    molecule_id=molecule_id,
                    cid=cached_cid,
                    cache_key=cache_key
                )
                return cached_cid, CIDSource.CACHE

    # Strategy 2: Direct CID (already in data)

    if "pubchem_cid" in molecule_data and molecule_data["pubchem_cid"]:
        cid = str(molecule_data["pubchem_cid"]).strip()
        if cid.isdigit():
            logger.debug("cid_resolution_direct", molecule_id=molecule_id, cid=cid)
            _update_cache(cid_cache, cache_keys, cid)
            return cid, CIDSource.DIRECT

    # Strategy 3: ChEMBL cross-references (Proj1 approach)

    cid = _extract_cid_from_xrefs(molecule_data)
    if cid:
        logger.debug("cid_resolution_xref", molecule_id=molecule_id, cid=cid)
        _update_cache(cid_cache, cache_keys, cid)
        return cid, CIDSource.XREF

    # Strategy 4: InChIKey lookup (Proj1 - most reliable)

    inchikey = _extract_inchikey(molecule_data)
    if inchikey and len(inchikey) == 27:
        try:
            cids = pubchem_client.fetch_cids_by_inchikey(inchikey)
            if cids:
                cid = cids[0]
                logger.info(
                    "cid_resolution_inchikey",
                    molecule_id=molecule_id,
                    cid=cid,
                    inchikey=inchikey
                )
                _update_cache(cid_cache, cache_keys, cid)
                return cid, CIDSource.INCHIKEY
        except Exception as e:
            logger.debug(
                "cid_resolution_inchikey_failed",
                molecule_id=molecule_id,
                inchikey=inchikey,
                error=str(e)
            )

    # Strategy 5: Canonical SMILES lookup (priority)

    canonical_smiles = _extract_canonical_smiles(molecule_data)
    if canonical_smiles:
        try:
            cids = pubchem_client.fetch_cids_by_smiles(canonical_smiles)
            if cids:
                cid = cids[0]
                logger.info(
                    "cid_resolution_canonical_smiles",
                    molecule_id=molecule_id,
                    cid=cid
                )
                _update_cache(cid_cache, cache_keys, cid)
                return cid, CIDSource.SMILES_CANONICAL
        except Exception as e:
            logger.debug(
                "cid_resolution_canonical_smiles_failed",
                molecule_id=molecule_id,
                error=str(e)
            )

    # Strategy 6: Isomeric SMILES lookup (fallback)

    isomeric_smiles = _extract_isomeric_smiles(molecule_data)
    if isomeric_smiles and isomeric_smiles != canonical_smiles:
        try:
            cids = pubchem_client.fetch_cids_by_smiles(isomeric_smiles)
            if cids:
                cid = cids[0]
                logger.info(
                    "cid_resolution_isomeric_smiles",
                    molecule_id=molecule_id,
                    cid=cid
                )
                _update_cache(cid_cache, cache_keys, cid)
                return cid, CIDSource.SMILES_ISOMERIC
        except Exception as e:
            logger.debug(
                "cid_resolution_isomeric_smiles_failed",
                molecule_id=molecule_id,
                error=str(e)
            )

    # Strategy 7: Name-based search (last resort, slowest)

    pref_name = molecule_data.get("pref_name")
    if pref_name and isinstance(pref_name, str) and len(pref_name) > 2:
        try:
            cids = pubchem_client.fetch_cids_by_name(pref_name)
            if cids:
                cid = cids[0]
                logger.info(
                    "cid_resolution_name",
                    molecule_id=molecule_id,
                    cid=cid,
                    name=pref_name
                )
                _update_cache(cid_cache, cache_keys, cid)
                return cid, CIDSource.NAME
        except Exception as e:
            logger.debug(
                "cid_resolution_name_failed",
                molecule_id=molecule_id,
                name=pref_name,
                error=str(e)
            )

    # All strategies failed

    logger.warning("cid_resolution_failed", molecule_id=molecule_id)
    return None, CIDSource.FAILED

def _generate_cache_keys(molecule_data: dict[str, Any]) -> list[str]:
    """Generate all possible cache keys for a molecule."""
    keys = []

    # InChIKey is best cache key (unique and stable)

    inchikey = _extract_inchikey(molecule_data)
    if inchikey:
        keys.append(f"inchikey:{inchikey}")

    # ChEMBL ID as backup

    chembl_id = molecule_data.get("molecule_chembl_id")
    if chembl_id:
        keys.append(f"chembl:{chembl_id}")

    # Canonical SMILES

    canonical_smiles = _extract_canonical_smiles(molecule_data)
    if canonical_smiles:
        keys.append(f"smiles:{canonical_smiles}")

    return keys

def _update_cache(
    cid_cache: dict[str, str],
    cache_keys: list[str],
    cid: str
) -> None:
    """Update cache with resolved CID."""
    for key in cache_keys:
        cid_cache[key] = cid

def _extract_inchikey(molecule_data: dict[str, Any]) -> str | None:
    """Extract InChIKey from molecule data."""

    # Try multiple field names

    for field in ["pubchem_inchi_key", "standard_inchikey", "inchikey", "standard_inchi_key"]:
        value = molecule_data.get(field)
        if value and isinstance(value, str):
            key = value.strip()
            if len(key) == 27:
                return key
    return None

def _extract_canonical_smiles(molecule_data: dict[str, Any]) -> str | None:
    """Extract canonical SMILES from molecule data."""
    for field in ["pubchem_canonical_smiles", "canonical_smiles", "standard_smiles"]:
        value = molecule_data.get(field)
        if value and isinstance(value, str):
            return value.strip()
    return None

def _extract_isomeric_smiles(molecule_data: dict[str, Any]) -> str | None:
    """Extract isomeric SMILES from molecule data."""
    for field in ["pubchem_isomeric_smiles", "isomeric_smiles", "smiles"]:
        value = molecule_data.get(field)
        if value and isinstance(value, str):
            return value.strip()
    return None

def _extract_cid_from_xrefs(molecule_data: dict[str, Any]) -> str | None:
    """Extract PubChem CID from ChEMBL cross-references (Proj1 approach)."""
    xref_sources = molecule_data.get("xref_sources")
    if not xref_sources or not isinstance(xref_sources, list):
        return None

    for xref in xref_sources:
        if not isinstance(xref, dict):
            continue

        xref_name = (xref.get("xref_name") or "").lower()
        xref_src = (xref.get("xref_src") or "").lower()
        xref_id = xref.get("xref_id")

        if xref_id and ("pubchem" in xref_name or "pubchem" in xref_src):

            # Extract digits from xref_id

            digits = "".join(ch for ch in str(xref_id) if ch.isdigit())
            if digits:
                return digits

    return None

```text

### 4.2 Кэш-стратегия (гибрид проектов)

Persistent CID cache (из проекта 2) с улучшениями:

```python

"""Persistent CID cache management."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

class PubChemCIDCache:
    """
    Persistent cache for PubChem CID mappings.

    Features:

    - JSON storage with metadata
    - TTL-based expiration (30 days default)
    - Schema versioning
    - Atomic writes (Proj2)
    - Statistics tracking
    """

    SCHEMA_VERSION = 1
    DEFAULT_TTL_HOURS = 720  # 30 days

    def __init__(self, cache_path: Path | str, ttl_hours: float = DEFAULT_TTL_HOURS):
        self.cache_path = Path(cache_path)
        self.ttl_hours = ttl_hours
        self.data: dict[str, dict[str, Any]] = {}
        self.metadata: dict[str, Any] = {}
        self._dirty = False

        self.load()

    def load(self) -> None:
        """Load cache from disk."""
        if not self.cache_path.exists():
            logger.info("pubchem_cache_not_found", path=str(self.cache_path))
            self._initialize_empty_cache()
            return

        try:
            with open(self.cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)

            # Validate structure

            if not isinstance(cache_data, dict):
                logger.warning("pubchem_cache_invalid_format")
                self._initialize_empty_cache()
                return

            self.metadata = cache_data.get('metadata', {})
            self.data = cache_data.get('values', {})

            # Validate schema version

            schema_version = self.metadata.get('schema_version')
            if schema_version != self.SCHEMA_VERSION:
                logger.warning(
                    "pubchem_cache_schema_mismatch",
                    expected=self.SCHEMA_VERSION,
                    found=schema_version
                )

            # Clean expired entries

            self._clean_expired()

            logger.info(
                "pubchem_cache_loaded",
                entries=len(self.data),
                path=str(self.cache_path)
            )

        except Exception as e:
            logger.error("pubchem_cache_load_error", error=str(e))
            self._initialize_empty_cache()

    def save(self, force: bool = False) -> None:
        """
        Save cache to disk.

        Args:
            force: Save even if not dirty
        """
        if not force and not self._dirty:
            return

        try:

            # Update metadata

            self.metadata.update({
                'schema_version': self.SCHEMA_VERSION,
                'updated_at': datetime.now(timezone.utc).isoformat(),
                'ttl_hours': self.ttl_hours,
                'entry_count': len(self.data)
            })

            cache_data = {
                'metadata': self.metadata,
                'values': self.data
            }

            # Atomic write (Proj2 approach)

            temp_path = self.cache_path.with_suffix('.tmp')
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)

            temp_path.replace(self.cache_path)
            self._dirty = False

            logger.info(
                "pubchem_cache_saved",
                entries=len(self.data),
                path=str(self.cache_path)
            )

        except Exception as e:
            logger.error("pubchem_cache_save_error", error=str(e))

    def get(self, key: str) -> str | None:
        """Get CID from cache."""
        entry = self.data.get(key)
        if not entry:
            return None

        # Check expiration

        if not self._is_entry_valid(entry):
            del self.data[key]
            self._dirty = True
            return None

        return entry.get('cid')

    def set(self, key: str, cid: str, source: str = "unknown") -> None:
        """Set CID in cache."""
        self.data[key] = {
            'cid': cid,
            'source': source,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        self._dirty = True

    def _is_entry_valid(self, entry: dict[str, Any]) -> bool:
        """Check if cache entry is still valid."""
        timestamp_str = entry.get('timestamp')
        if not timestamp_str:
            return False

        try:
            timestamp = datetime.fromisoformat(timestamp_str)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            age = datetime.now(timezone.utc) - timestamp
            return age.total_seconds() < (self.ttl_hours * 3600)

        except Exception:
            return False

    def _clean_expired(self) -> None:
        """Remove expired entries."""
        expired_keys = [
            key for key, entry in self.data.items()
            if not self._is_entry_valid(entry)
        ]

        for key in expired_keys:
            del self.data[key]

        if expired_keys:
            self._dirty = True
            logger.info("pubchem_cache_cleaned", expired_count=len(expired_keys))

    def _initialize_empty_cache(self) -> None:
        """Initialize empty cache structure."""
        self.metadata = {
            'schema_version': self.SCHEMA_VERSION,
            'created_at': datetime.now(timezone.utc).isoformat(),
            'ttl_hours': self.ttl_hours
        }
        self.data = {}
        self._dirty = True

    def get_statistics(self) -> dict[str, Any]:
        """Get cache statistics."""
        now = datetime.now(timezone.utc)

        # Count by source

        source_counts = {}
        age_distribution = {'<1d': 0, '1-7d': 0, '7-30d': 0, '>30d': 0}

        for entry in self.data.values():
            source = entry.get('source', 'unknown')
            source_counts[source] = source_counts.get(source, 0) + 1

            timestamp_str = entry.get('timestamp')
            if timestamp_str:
                try:
                    timestamp = datetime.fromisoformat(timestamp_str)
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.replace(tzinfo=timezone.utc)

                    age_days = (now - timestamp).total_seconds() / 86400

                    if age_days < 1:
                        age_distribution['<1d'] += 1
                    elif age_days < 7:
                        age_distribution['1-7d'] += 1
                    elif age_days < 30:
                        age_distribution['7-30d'] += 1
                    else:
                        age_distribution['>30d'] += 1
                except Exception:
                    pass

        return {
            'total_entries': len(self.data),
            'source_distribution': source_counts,
            'age_distribution': age_distribution,
            'ttl_hours': self.ttl_hours,
            'cache_path': str(self.cache_path)
        }

```text

**Структура cache файла:**

```json

{
  "metadata": {
    "schema_version": 1,
    "created_at": "2024-01-01T00:00:00+00:00",
    "updated_at": "2024-01-15T12:30:00+00:00",
    "ttl_hours": 720,
    "entry_count": 15234
  },
  "values": {
    "inchikey:BSYNRYMUTXBXSQ-UHFFFAOYSA-N": {
      "cid": "2244",
      "source": "inchikey",
      "timestamp": "2024-01-10T08:15:00+00:00"
    },
    "chembl:CHEMBL25": {
      "cid": "2244",
      "source": "xref",
      "timestamp": "2024-01-10T08:15:00+00:00"
    },
    "smiles:CC(=O)OC1=CC=CC=C1C(=O)O": {
      "cid": "2244",
      "source": "smiles_canonical",
      "timestamp": "2024-01-10T08:15:00+00:00"
    }
  }
}

```text

---

## 5. Извлекаемые поля (Unified Schema)

### 5.1 Обязательные поля (Core Properties)

Эти поля должны присутствовать для каждой успешно обогащённой молекулы:

| Поле | Тип | Nullable | Validation | Описание |
|------|-----|----------|------------|----------|
| `pubchem_cid` | INT | No | > 0, unique | PubChem Compound ID (primary key) |
| `pubchem_molecular_formula` | STRING | Yes | regex: `^[A-Z][a-z]?(\d+[A-Z][a-z]?)*\d*$` | Molecular formula (e.g., C9H8O4) |
| `pubchem_molecular_weight` | FLOAT | Yes | 50.0 ≤ x ≤ 2000.0 | Molecular weight in Daltons |
| `pubchem_canonical_smiles` | STRING | Yes | min_length: 5 | Canonical SMILES notation |
| `pubchem_inchi_key` | STRING | Yes | length: 27, format: `XXX-XXX-X` | InChI Key (standard hash) |

### 5.2 Опциональные поля (Extended Properties)

Дополнительные поля для расширенной аннотации:

| Поле | Тип | Nullable | Описание | Проект |
|------|-----|----------|----------|--------|
| `pubchem_isomeric_smiles` | STRING | Yes | Isomeric SMILES (стереохимия) | Оба |
| `pubchem_inchi` | STRING | Yes | Full InChI notation (длинная строка) | Оба |
| `pubchem_iupac_name` | STRING | Yes | IUPAC systematic name | Проект 2 |
| `pubchem_registry_id` | STRING | Yes | Registry identifier | Оба |
| `pubchem_rn` | STRING | Yes | RN (Registration Number) | Оба |
| `pubchem_synonyms` | LIST[STRING] | Yes | List of synonyms | Проект 1 |

### 5.3 Metadata поля (Tracking & Audit)

Системные поля для отслеживания процесса обогащения:

| Поле | Тип | Nullable | Описание |
|------|-----|----------|----------|
| `pubchem_enriched_at` | TIMESTAMP | No | UTC timestamp обогащения |
| `pubchem_cid_source` | ENUM | No | Источник CID: cache/direct/xref/inchikey/smiles/name/failed |
| `pubchem_fallback_used` | BOOLEAN | No | True если использовался fallback strategy |
| `pubchem_enrichment_attempt` | INT | No | Номер попытки обогащения (для retry tracking) |

### 5.4 Pandera Schema Definition

```python

"""Pandera schema for PubChem-enriched testitem data."""

import pandera as pa
from pandera import Column, DataFrameSchema, Check

class PubChemEnrichedTestitemSchema(pa.DataFrameModel):
    """Schema for testitem data enriched with PubChem."""

    # ChEMBL primary key

    molecule_chembl_id: str = pa.Field(
        nullable=False,
        str_matches=r'^CHEMBL\d+$',
        description="ChEMBL molecule identifier"
    )

    # PubChem core fields

    pubchem_cid: pd.Int64Dtype = pa.Field(
        nullable=True,  # Optional enrichment

        ge=1,
        description="PubChem Compound ID"
    )

    pubchem_molecular_formula: str = pa.Field(
        nullable=True,
        str_matches=r'^[A-Z][a-z]?(\d+[A-Z][a-z]?)*\d*$',
        description="Molecular formula from PubChem"
    )

    pubchem_molecular_weight: float = pa.Field(
        nullable=True,
        ge=50.0,
        le=2000.0,
        description="Molecular weight from PubChem"
    )

    pubchem_canonical_smiles: str = pa.Field(
        nullable=True,
        str_min_length=5,
        description="Canonical SMILES from PubChem"
    )

    pubchem_isomeric_smiles: str = pa.Field(
        nullable=True,
        str_min_length=5,
        description="Isomeric SMILES from PubChem"
    )

    pubchem_inchi: str = pa.Field(
        nullable=True,
        str_min_length=10,
        description="InChI notation from PubChem"
    )

    pubchem_inchi_key: str = pa.Field(
        nullable=True,
        str_length=27,
        str_matches=r'^[A-Z]{14}-[A-Z]{10}-[A-Z]$',
        description="InChI Key from PubChem"
    )

    pubchem_iupac_name: str = pa.Field(
        nullable=True,
        description="IUPAC name from PubChem"
    )

    pubchem_registry_id: str = pa.Field(
        nullable=True,
        description="Registry ID from PubChem"
    )

    pubchem_rn: str = pa.Field(
        nullable=True,
        description="RN from PubChem"
    )

    # Metadata fields

    pubchem_enriched_at: str = pa.Field(
        nullable=True,  # Null if enrichment failed

        description="Timestamp of PubChem enrichment"
    )

    pubchem_cid_source: str = pa.Field(
        nullable=True,
        isin=['cache', 'direct', 'xref', 'inchikey', 'smiles_canonical',
              'smiles_isomeric', 'name', 'failed'],
        description="Source of CID resolution"
    )

    pubchem_fallback_used: bool = pa.Field(
        nullable=True,
        description="Whether fallback strategy was used"
    )

    class Config:
        """Pandera configuration."""
        strict = False  # Allow additional columns

        coerce = True   # Coerce types where possible

```text

---

## 6. Реализация (Рекомендуемая архитектура)

### 6.1 Модульная структура

Рекомендуемая организация кода для максимальной читаемости и тестируемости:

```text

library/
├── clients/
│   ├── __init__.py
│   ├── base.py                      # Base HTTP client

│   ├── pubchem.py                   # OptimalPubChemClient (section 3.2)

│   └── chembl.py                    # ChEMBL client

│
├── enrichment/
│   ├── __init__.py
│   ├── pubchem_resolver.py          # CID resolution logic (section 4.1)

│   ├── pubchem_cache.py             # Persistent cache (section 4.2)

│   └── pubchem_enricher.py          # Main orchestration (section 6.2)

│
├── schemas/
│   ├── __init__.py
│   ├── testitem_schema.py           # Pandera schemas (section 5.4)

│   └── pubchem_schema.py            # PubChem-specific schemas

│
└── pipelines/
    └── testitem/
        ├── __init__.py
        ├── extract.py               # ChEMBL extraction

        ├── enrich.py                # PubChem enrichment integration

        ├── normalize.py             # Data normalization

        ├── validate.py              # Validation

        └── pipeline.py              # Main pipeline orchestration

```text

### 6.2 Ключевые функции (Implementation)

#### 6.2.1 High-Level Orchestration

```python

"""Main PubChem enrichment orchestrator."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd

from library.clients.pubchem import OptimalPubChemClient
from library.enrichment.pubchem_cache import PubChemCIDCache
from library.enrichment.pubchem_resolver import resolve_pubchem_cid, CIDSource

logger = logging.getLogger(__name__)

class PubChemEnricher:
    """
    Main orchestrator for PubChem enrichment.

    Combines:

    - Parallel CID resolution (Proj2)
    - Batch properties fetch (Proj1)
    - Smart caching (both)
    - Graceful degradation (both)
    """

    def __init__(
        self,
        client: OptimalPubChemClient,
        cache: PubChemCIDCache,
        workers: int = 4,
        batch_size: int = 100
    ):
        self.client = client
        self.cache = cache
        self.workers = workers
        self.batch_size = batch_size

        # Statistics

        self.stats = {
            'total_molecules': 0,
            'cid_resolved': 0,
            'properties_enriched': 0,
            'cache_hits': 0,
            'api_calls': 0,
            'errors': 0,
            'source_distribution': {}
        }

    def enrich(self, molecules_df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich molecules DataFrame with PubChem data.

        Algorithm:
        1. Filter: skip already enriched molecules
        2. CID resolution (parallel) with caching
        3. Batch properties fetch
        4. Merge results into DataFrame
        5. Save cache
        6. Collect statistics

        Args:
            molecules_df: DataFrame with ChEMBL data

        Returns:
            DataFrame enriched with PubChem data
        """
        self.stats['total_molecules'] = len(molecules_df)

        logger.info(
            "pubchem_enrichment_start",
            total_molecules=len(molecules_df),
            workers=self.workers,
            batch_size=self.batch_size
        )

        # Step 1: Filter molecules needing enrichment

        molecules_to_enrich = self._filter_molecules(molecules_df)

        if molecules_to_enrich.empty:
            logger.info("pubchem_enrichment_skip_all_enriched")
            return molecules_df

        logger.info(
            "pubchem_enrichment_needed",
            count=len(molecules_to_enrich)
        )

        # Step 2: Parallel CID resolution

        cid_mapping = self._resolve_cids_parallel(molecules_to_enrich)

        # Step 3: Batch properties fetch

        properties_data = self._fetch_properties_batch(cid_mapping)

        # Step 4: Merge into DataFrame

        enriched_df = self._merge_properties(molecules_df, properties_data)

        # Step 5: Save cache

        try:
            self.cache.save()
        except Exception as e:
            logger.error("pubchem_cache_save_failed", error=str(e))

        # Step 6: Log statistics

        self._log_statistics()

        return enriched_df

    def _filter_molecules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter molecules that need PubChem enrichment."""

        # Skip if pubchem_cid already present

        if 'pubchem_cid' in df.columns:
            mask = df['pubchem_cid'].isna()
            return df[mask].copy()

        return df.copy()

    def _resolve_cids_parallel(
        self,
        molecules_df: pd.DataFrame
    ) -> dict[str, tuple[str, CIDSource]]:
        """
        Resolve CIDs in parallel using ThreadPoolExecutor.

        Returns:
            Dict mapping molecule_chembl_id to (CID, source)
        """
        cid_mapping = {}

        # Convert DataFrame to list of dicts for parallel processing

        molecules_list = molecules_df.to_dict('records')

        with ThreadPoolExecutor(max_workers=self.workers) as executor:

            # Submit all resolution tasks

            future_to_molecule = {
                executor.submit(
                    resolve_pubchem_cid,
                    molecule,
                    self.cache.data,
                    self.client
                ): molecule['molecule_chembl_id']
                for molecule in molecules_list
            }

            # Collect results as they complete

            for future in as_completed(future_to_molecule):
                molecule_id = future_to_molecule[future]
                try:
                    cid, source = future.result()
                    if cid:
                        cid_mapping[molecule_id] = (cid, source)
                        self.stats['cid_resolved'] += 1

                        # Track source distribution

                        source_str = source.value
                        self.stats['source_distribution'][source_str] = \
                            self.stats['source_distribution'].get(source_str, 0) + 1

                        if source == CIDSource.CACHE:
                            self.stats['cache_hits'] += 1
                        else:
                            self.stats['api_calls'] += 1

                except Exception as e:
                    logger.error(
                        "cid_resolution_error",
                        molecule_id=molecule_id,
                        error=str(e)
                    )
                    self.stats['errors'] += 1

        logger.info(
            "cid_resolution_complete",
            resolved=len(cid_mapping),
            total=len(molecules_list),
            resolution_rate=len(cid_mapping) / len(molecules_list) if molecules_list else 0
        )

        return cid_mapping

    def _fetch_properties_batch(
        self,
        cid_mapping: dict[str, tuple[str, CIDSource]]
    ) -> dict[str, dict[str, Any]]:
        """
        Fetch properties for all CIDs using batch API.

        Args:
            cid_mapping: Dict mapping molecule_chembl_id to (CID, source)

        Returns:
            Dict mapping molecule_chembl_id to properties
        """
        if not cid_mapping:
            return {}

        # Extract unique CIDs

        cids = list(set(cid for cid, _ in cid_mapping.values()))

        logger.info(
            "batch_properties_fetch_start",
            unique_cids=len(cids),
            batch_size=self.batch_size
        )

        # Batch fetch from PubChem

        try:
            cid_to_properties = self.client.fetch_properties_batch(
                cids,
                batch_size=self.batch_size
            )
        except Exception as e:
            logger.error("batch_properties_fetch_failed", error=str(e))
            cid_to_properties = {}

        # Map back to molecule_chembl_id

        molecule_properties = {}
        for molecule_id, (cid, source) in cid_mapping.items():
            if cid in cid_to_properties:
                properties = cid_to_properties[cid].copy()
                properties['pubchem_cid_source'] = source.value
                properties['pubchem_enriched_at'] = pd.Timestamp.utcnow().isoformat() + 'Z'
                properties['pubchem_fallback_used'] = source != CIDSource.INCHIKEY

                molecule_properties[molecule_id] = properties
                self.stats['properties_enriched'] += 1

        logger.info(
            "batch_properties_fetch_complete",
            enriched=len(molecule_properties),
            total=len(cid_mapping)
        )

        return molecule_properties

    def _merge_properties(
        self,
        original_df: pd.DataFrame,
        properties_data: dict[str, dict[str, Any]]
    ) -> pd.DataFrame:
        """Merge PubChem properties into original DataFrame."""
        if not properties_data:
            return original_df

        # Create properties DataFrame

        properties_df = pd.DataFrame.from_dict(properties_data, orient='index')
        properties_df.index.name = 'molecule_chembl_id'
        properties_df.reset_index(inplace=True)

        # Merge with original

        enriched_df = original_df.merge(
            properties_df,
            on='molecule_chembl_id',
            how='left',
            suffixes=('', '_pubchem_new')
        )

        # Resolve conflicts: prefer ChEMBL data, add PubChem where missing

        for col in properties_df.columns:
            if col == 'molecule_chembl_id':
                continue

            if f"{col}_pubchem_new" in enriched_df.columns:

                # Column existed, use ChEMBL value where available

                enriched_df[col] = enriched_df[col].fillna(enriched_df[f"{col}_pubchem_new"])
                enriched_df.drop(columns=[f"{col}_pubchem_new"], inplace=True)

        return enriched_df

    def _log_statistics(self) -> None:
        """Log enrichment statistics."""
        total = self.stats['total_molecules']
        resolved = self.stats['cid_resolved']
        enriched = self.stats['properties_enriched']

        logger.info(
            "pubchem_enrichment_complete",
            total_molecules=total,
            cid_resolved=resolved,
            cid_resolution_rate=resolved / total if total > 0 else 0,
            properties_enriched=enriched,
            enrichment_rate=enriched / total if total > 0 else 0,
            cache_hits=self.stats['cache_hits'],
            cache_hit_rate=self.stats['cache_hits'] / resolved if resolved > 0 else 0,
            api_calls=self.stats['api_calls'],
            errors=self.stats['errors'],
            source_distribution=self.stats['source_distribution']
        )

    def get_statistics(self) -> dict[str, Any]:
        """Get enrichment statistics."""
        stats = self.stats.copy()

        total = stats['total_molecules']
        resolved = stats['cid_resolved']

        # Add computed metrics (continued)

        stats['cid_resolution_rate'] = resolved / total if total > 0 else 0.0
        stats['enrichment_rate'] = stats['properties_enriched'] / total if total > 0 else 0.0
        stats['cache_hit_rate'] = stats['cache_hits'] / resolved if resolved > 0 else 0.0
        stats['error_rate'] = stats['errors'] / total if total > 0 else 0.0

        return stats

```text

#### 6.2.2 Pipeline Integration

```python

"""Integration of PubChem enrichment into testitem pipeline."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from library.clients.pubchem import OptimalPubChemClient
from library.enrichment.pubchem_cache import PubChemCIDCache
from library.enrichment.pubchem_enricher import PubChemEnricher

logger = logging.getLogger(__name__)

def enrich_testitem_with_pubchem(
    testitem_df: pd.DataFrame,
    config: dict,
    cache_path: Path | str
) -> pd.DataFrame:
    """
    Enrich testitem DataFrame with PubChem data.

    This is the main entry point for PubChem enrichment in the testitem pipeline.

    Args:
        testitem_df: DataFrame with ChEMBL testitem data
        config: PubChem configuration dict
        cache_path: Path to CID cache file

    Returns:
        DataFrame enriched with PubChem data

    Example:
        >>> config = {
        ...     'enabled': True,
        ...     'batch_size': 100,
        ...     'workers': 4,
        ...     'cache_ttl_hours': 720
        ... }
        >>> enriched_df = enrich_testitem_with_pubchem(df, config, 'cache.json')
    """

    # Check if enrichment is enabled

    if not config.get('enabled', True):
        logger.info("pubchem_enrichment_disabled")
        return testitem_df

    try:

        # Initialize components

        client = OptimalPubChemClient(
            base_url=config.get('base_url', 'https://pubchem.ncbi.nlm.nih.gov/rest/pug'),
            timeout=config.get('timeout', 30.0),
            max_retries=config.get('max_retries', 3),
            cache_dir=config.get('http_cache_dir')
        )

        cache = PubChemCIDCache(
            cache_path=cache_path,
            ttl_hours=config.get('cache_ttl_hours', 720)
        )

        enricher = PubChemEnricher(
            client=client,
            cache=cache,
            workers=config.get('workers', 4),
            batch_size=config.get('batch_size', 100)
        )

        # Perform enrichment

        enriched_df = enricher.enrich(testitem_df)

        # Log statistics

        stats = enricher.get_statistics()
        logger.info("pubchem_enrichment_statistics", **stats)

        return enriched_df

    except Exception as e:
        logger.error("pubchem_enrichment_failed", error=str(e))

        # Graceful degradation: return original data

        logger.warning("pubchem_enrichment_graceful_degradation")
        return testitem_df

```text

---

## 7. Кэширование (Трёхуровневая стратегия)

### 7.1 Level 1: In-Memory TTL Cache (из проекта 2)

**Назначение:** Кэширование HTTP responses в памяти для быстрого доступа

**Характеристики:**

- **Библиотека:** `cachetools.TTLCache`

- **TTL:** 1 час (3600 секунд)

- **Max entries:** 1000

- **Thread-safe:** Да (threading.Lock)

- **Persistence:** Нет

**Реализация:**

```python

from cachetools import TTLCache
import threading

class MemoryCacheLayer:
    """In-memory cache for HTTP responses."""

    def __init__(self, maxsize: int = 1000, ttl: int = 3600):
        self._cache = TTLCache(maxsize=maxsize, ttl=ttl)
        self._lock = threading.Lock()

    def get(self, key: str) -> dict | None:
        """Get from cache."""
        with self._lock:
            return self._cache.get(key)

    def set(self, key: str, value: dict) -> None:
        """Store in cache."""
        with self._lock:
            self._cache[key] = value

    def stats(self) -> dict:
        """Get cache statistics."""
        with self._lock:
            return {
                'current_size': len(self._cache),
                'max_size': self._cache.maxsize,
                'ttl_seconds': self._cache.ttl
            }

```text

**Преимущества:**

- ✅ Очень быстрый доступ (in-memory)

- ✅ Автоматическое истечение (TTL)

- ✅ Thread-safe

**Недостатки:**

- ❌ Не персистентен (теряется при рестарте)

- ❌ Ограниченный размер

### 7.2 Level 2: Persistent CID Mapping (из проекта 2)

**Назначение:** Долговременное хранение CID mappings

**Характеристики:**

- **Format:** JSON с metadata

- **TTL:** 30 дней (720 часов)

- **Schema version:** 1

- **Atomic writes:** Да

- **Compression:** Нет (читаемость важнее)

**Cache structure:**

```json

{
  "metadata": {
    "schema_version": 1,
    "created_at": "2024-01-01T00:00:00+00:00",
    "updated_at": "2024-01-15T12:30:00+00:00",
    "ttl_hours": 720,
    "entry_count": 15234,
    "cache_type": "pubchem_cid_mapping"
  },
  "values": {
    "inchikey:BSYNRYMUTXBXSQ-UHFFFAOYSA-N": {
      "cid": "2244",
      "source": "inchikey",
      "timestamp": "2024-01-10T08:15:00+00:00"
    },
    "chembl:CHEMBL25": {
      "cid": "2244",
      "source": "xref",
      "timestamp": "2024-01-10T08:15:00+00:00"
    },
    "smiles:CC(=O)OC1=CC=CC=C1C(=O)O": {
      "cid": "2244",
      "source": "smiles_canonical",
      "timestamp": "2024-01-10T08:15:00+00:00"
    }
  }
}

```text

**Cache key strategies:**

| Key Type | Format | Example | Uniqueness |
|----------|--------|---------|------------|
| InChIKey | `inchikey:{key}` | `inchikey:BSYNRYMUTXBXSQ-UHFFFAOYSA-N` | HIGH (best) |
| ChEMBL ID | `chembl:{id}` | `chembl:CHEMBL25` | HIGH |
| SMILES | `smiles:{smiles}` | `smiles:CC(=O)OC1=...` | MEDIUM |

**Преимущества:**

- ✅ Персистентность (переживает рестарты)

- ✅ Долгий TTL (30 дней)

- ✅ Читаемый формат для аудита

- ✅ Метаданные для мониторинга

**Недостатки:**

- ❌ Медленнее чем in-memory

- ❌ Требует disk I/O

### 7.3 Level 3: File-Based HTTP Cache (из проекта 1, опционально)

**Назначение:** Отладка и аудит HTTP запросов

**Характеристики:**

- **Format:** JSON per request

- **Naming:** SHA256 hash ключи

- **TTL:** Нет (manual cleanup)

- **Organization:** По endpoint

**Directory structure:**

```text

data/cache/pubchem_http/
├── a1b2c3d4e5f6...  # compound/cid/2244/property/...

├── f6e5d4c3b2a1...  # compound/inchikey/.../cids/

└── ...

```text

**Преимущества:**

- ✅ Полный аудит trail

- ✅ Легко инспектировать

- ✅ Можно использовать для regression testing

**Недостатки:**

- ❌ Занимает много места

- ❌ Requires manual cleanup

- ❌ Медленный

### 7.4 Cache Invalidation Strategy

```python

"""Cache invalidation logic."""

from datetime import datetime, timezone

def is_cache_entry_valid(
    entry: dict,
    ttl_hours: float,
    now: datetime | None = None
) -> bool:
    """
    Check if cache entry is still valid.

    Args:
        entry: Cache entry with 'timestamp' field
        ttl_hours: Time-to-live in hours
        now: Current time (for testing)

    Returns:
        True if entry is valid
    """
    timestamp_str = entry.get('timestamp')
    if not timestamp_str:
        return False

    try:
        timestamp = datetime.fromisoformat(timestamp_str)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        if now is None:
            now = datetime.now(timezone.utc)

        age = now - timestamp
        age_hours = age.total_seconds() / 3600

        return age_hours < ttl_hours

    except (ValueError, TypeError):
        return False

def clean_expired_entries(
    cache_data: dict[str, dict],
    ttl_hours: float
) -> tuple[dict[str, dict], int]:
    """
    Remove expired entries from cache.

    Returns:
        (cleaned_cache, expired_count)
    """
    now = datetime.now(timezone.utc)
    cleaned = {}
    expired_count = 0

    for key, entry in cache_data.items():
        if is_cache_entry_valid(entry, ttl_hours, now):
            cleaned[key] = entry
        else:
            expired_count += 1

    return cleaned, expired_count

```text

### 7.5 Cache Hit Rate Optimization

**Strategies для увеличения cache hit rate:**

1. **Multiple cache keys per molecule:**

   ```python

   # Cache под всеми возможными идентификаторами

   keys = [
       f"inchikey:{inchikey}",
       f"chembl:{chembl_id}",
       f"smiles:{canonical_smiles}"
   ]
   for key in keys:
       cache.set(key, cid)

```text

2. **Preemptive caching:**

   ```python

   # При успешном resolution, cache для всех альтернатив

   if cid_found:
       cache_all_identifiers(molecule_data, cid)

```text

3. **Cache warming:**

   ```python

   # Загрузка популярных молекул при старте

   def warm_cache(common_molecules: list[str]):
       for chembl_id in common_molecules:
           resolve_and_cache(chembl_id)

```text

---

## 8. Обработка ошибок (Resilience Patterns)

### 8.1 Иерархия обработки ошибок

```python

"""Comprehensive error handling for PubChem client."""

import logging
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

class PubChemErrorType(str, Enum):
    """Types of PubChem errors."""
    RATE_LIMIT = "rate_limit"           # 429 Too Many Requests

    SERVICE_ERROR = "service_error"      # 503, 504

    TIMEOUT = "timeout"                  # Request timeout

    NETWORK = "network"                  # Connection errors

    PARSE_ERROR = "parse_error"          # JSON parsing failed

    INVALID_INPUT = "invalid_input"      # Bad CID, SMILES, etc.

    NOT_FOUND = "not_found"              # 404

    UNKNOWN = "unknown"

class PubChemErrorHandler:
    """Centralized error handling for PubChem operations."""

    def __init__(self, outage_tracker: ServiceOutageTracker):
        self.outage_tracker = outage_tracker
        self.error_counts = {error_type: 0 for error_type in PubChemErrorType}
        self._lock = threading.Lock()

    def handle_error(
        self,
        error: Exception,
        context: dict[str, Any]
    ) -> tuple[PubChemErrorType, bool]:
        """
        Handle error and determine recovery strategy.

        Args:
            error: The exception that occurred
            context: Context info (url, cid, etc.)

        Returns:
            (error_type, should_retry)
        """
        error_type = self._classify_error(error)

        with self._lock:
            self.error_counts[error_type] += 1

        # Handle each error type

        if error_type == PubChemErrorType.RATE_LIMIT:
            return self._handle_rate_limit(error, context)

        elif error_type == PubChemErrorType.SERVICE_ERROR:
            return self._handle_service_error(error, context)

        elif error_type == PubChemErrorType.TIMEOUT:
            return self._handle_timeout(error, context)

        elif error_type == PubChemErrorType.NETWORK:
            return self._handle_network_error(error, context)

        elif error_type == PubChemErrorType.PARSE_ERROR:
            return self._handle_parse_error(error, context)

        elif error_type == PubChemErrorType.INVALID_INPUT:
            return self._handle_invalid_input(error, context)

        elif error_type == PubChemErrorType.NOT_FOUND:
            return self._handle_not_found(error, context)

        else:
            return self._handle_unknown_error(error, context)

    def _classify_error(self, error: Exception) -> PubChemErrorType:
        """Classify error by type."""
        import requests

        if isinstance(error, requests.HTTPError):
            status_code = error.response.status_code if error.response else None

            if status_code == 429:
                return PubChemErrorType.RATE_LIMIT
            elif status_code in (503, 504):
                return PubChemErrorType.SERVICE_ERROR
            elif status_code == 404:
                return PubChemErrorType.NOT_FOUND
            else:
                return PubChemErrorType.UNKNOWN

        elif isinstance(error, requests.Timeout):
            return PubChemErrorType.TIMEOUT

        elif isinstance(error, requests.ConnectionError):
            return PubChemErrorType.NETWORK

        elif isinstance(error, (json.JSONDecodeError, ValueError)):
            return PubChemErrorType.PARSE_ERROR

        elif isinstance(error, (TypeError, AttributeError)):
            return PubChemErrorType.INVALID_INPUT

        else:
            return PubChemErrorType.UNKNOWN

    def _handle_rate_limit(
        self,
        error: Exception,
        context: dict
    ) -> tuple[PubChemErrorType, bool]:
        """Handle 429 Rate Limit errors."""

        # Extract Retry-After header

        retry_after = self._parse_retry_after(error)

        # Mark service unavailable

        self.outage_tracker.mark_unavailable(retry_after, "rate_limit")

        logger.warning(
            "pubchem_rate_limit_hit",
            retry_after_seconds=retry_after,
            context=context
        )

        # Should retry after cooldown

        return (PubChemErrorType.RATE_LIMIT, True)

    def _handle_service_error(
        self,
        error: Exception,
        context: dict
    ) -> tuple[PubChemErrorType, bool]:
        """Handle 503/504 Service Unavailable errors."""

        # Mark service unavailable for 60 seconds

        self.outage_tracker.mark_unavailable(60.0, "service_error")

        logger.warning(
            "pubchem_service_unavailable",
            error=str(error),
            context=context
        )

        # Should retry after cooldown (continued)

        return (PubChemErrorType.SERVICE_ERROR, True)

    def _handle_timeout(
        self,
        error: Exception,
        context: dict
    ) -> tuple[PubChemErrorType, bool]:
        """Handle timeout errors."""
        logger.warning(
            "pubchem_timeout",
            error=str(error),
            context=context
        )

        # Retry with longer timeout

        return (PubChemErrorType.TIMEOUT, True)

    def _handle_network_error(
        self,
        error: Exception,
        context: dict
    ) -> tuple[PubChemErrorType, bool]:
        """Handle network errors."""
        logger.error(
            "pubchem_network_error",
            error=str(error),
            context=context
        )

        # Retry after brief delay

        return (PubChemErrorType.NETWORK, True)

    def _handle_parse_error(
        self,
        error: Exception,
        context: dict
    ) -> tuple[PubChemErrorType, bool]:
        """Handle JSON parsing errors."""
        logger.error(
            "pubchem_parse_error",
            error=str(error),
            context=context
        )

        # Don't retry - likely bad response

        return (PubChemErrorType.PARSE_ERROR, False)

    def _handle_invalid_input(
        self,
        error: Exception,
        context: dict
    ) -> tuple[PubChemErrorType, bool]:
        """Handle invalid input errors."""
        logger.debug(
            "pubchem_invalid_input",
            error=str(error),
            context=context
        )

        # Don't retry - fix input first

        return (PubChemErrorType.INVALID_INPUT, False)

    def _handle_not_found(
        self,
        error: Exception,
        context: dict
    ) -> tuple[PubChemErrorType, bool]:
        """Handle 404 Not Found errors."""
        logger.debug(
            "pubchem_not_found",
            context=context
        )

        # Don't retry - compound doesn't exist

        return (PubChemErrorType.NOT_FOUND, False)

    def _handle_unknown_error(
        self,
        error: Exception,
        context: dict
    ) -> tuple[PubChemErrorType, bool]:
        """Handle unknown errors."""
        logger.error(
            "pubchem_unknown_error",
            error=str(error),
            error_type=type(error).__name__,
            context=context
        )

        # Conservative: don't retry unknown errors

        return (PubChemErrorType.UNKNOWN, False)

    def _parse_retry_after(self, error: Exception) -> float:
        """Parse Retry-After header from HTTP error."""
        try:
            response = error.response
            retry_after = response.headers.get('Retry-After')

            if retry_after:
                try:
                    return float(retry_after)
                except ValueError:

                    # Might be HTTP date, use default

                    return 60.0
        except AttributeError:
            pass

        return 60.0  # Default cooldown

    def get_error_statistics(self) -> dict[str, Any]:
        """Get error statistics."""
        with self._lock:
            return {
                'total_errors': sum(self.error_counts.values()),
                'by_type': dict(self.error_counts)
            }

```text

### 8.2 Graceful Degradation Matrix

Таблица стратегий обработки для каждого типа ошибки:

| Error Type | HTTP Code | Strategy | Retry? | Continue Pipeline? | Log Level |
|------------|-----------|----------|--------|-------------------|-----------|
| **Rate Limit** | 429 | Exponential backoff + cooldown | ✅ Yes (after cooldown) | ✅ Yes | INFO |

| **Service Error** | 503, 504 | Wait 60s, then retry | ✅ Yes (max 3 attempts) | ✅ Yes | WARNING |

| **Timeout** | - | Increase timeout, retry | ✅ Yes (max 3 attempts) | ✅ Yes | WARNING |

| **Network Error** | - | Exponential backoff | ✅ Yes (max 5 attempts) | ✅ Yes | ERROR |

| **Parse Error** | 200 | Skip molecule | ❌ No | ✅ Yes | ERROR |

| **Invalid Input** | 400 | Skip molecule | ❌ No | ✅ Yes | DEBUG |

| **Not Found** | 404 | Skip molecule | ❌ No | ✅ Yes | DEBUG |

| **Cache Corruption** | - | Rebuild cache, continue | ✅ Yes | ✅ Yes | ERROR |

| **Unknown** | Other | Skip molecule | ❌ No | ✅ Yes | ERROR |

### 8.3 Retry Logic with Exponential Backoff

```python

"""Retry logic with exponential backoff and jitter."""

import random
import time
from typing import Callable, TypeVar, Any

T = TypeVar('T')

def retry_with_backoff(
    func: Callable[..., T],
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_errors: tuple = (Exception,)
) -> T:
    """
    Retry function with exponential backoff.

    Args:
        func: Function to retry
        max_attempts: Maximum number of attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential backoff
        jitter: Add random jitter to delay
        retryable_errors: Tuple of retryable exception types

    Returns:
        Result from func

    Raises:
        Last exception if all retries fail
    """
    last_exception = None

    for attempt in range(max_attempts):
        try:
            return func()

        except retryable_errors as e:
            last_exception = e

            if attempt == max_attempts - 1:

                # Last attempt, re-raise

                raise

            # Calculate delay with exponential backoff

            delay = min(base_delay * (exponential_base ** attempt), max_delay)

            # Add jitter (0-25% of delay)

            if jitter:
                delay *= (1 + random.random() * 0.25)

            logger.debug(
                "retry_attempt",
                attempt=attempt + 1,
                max_attempts=max_attempts,
                delay_seconds=delay,
                error=str(e)
            )

            time.sleep(delay)

    # Should never reach here, but for type safety

    if last_exception:
        raise last_exception
    raise RuntimeError("Unexpected retry loop exit")

# Usage example

def fetch_with_retry(cid: str, client: OptimalPubChemClient) -> dict:
    """Fetch properties with automatic retry."""
    return retry_with_backoff(
        func=lambda: client.fetch_properties(cid),
        max_attempts=3,
        base_delay=1.0,
        retryable_errors=(ServiceUnavailableError, RateLimitError, requests.Timeout)
    )

```text

---

## 9. Performance Optimization

### 9.1 Batch Processing (проект 1 approach)

**Концепция:** Группировка CIDs для единого API запроса

**Implementation:**

```python

"""Efficient batch processing for PubChem API."""

from typing import Any, Iterable

def chunked(iterable: Iterable[Any], chunk_size: int) -> Iterable[list[Any]]:
    """Split iterable into chunks of size chunk_size."""
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def fetch_properties_optimized(
    cids: list[str],
    client: OptimalPubChemClient,
    batch_size: int = 100
) -> dict[str, dict[str, Any]]:
    """
    Optimized batch fetching with smart chunking.

    Performance gains:

    - 100 molecules: 1 request instead of 100 (100x faster)
    - Automatic fallback to individual on batch failure
    - Progress tracking
    """
    results = {}
    total_cids = len(cids)
    processed = 0

    for batch in chunked(cids, batch_size):
        batch_str = ",".join(batch)

        try:

            # Try batch request first

            path = (
                f"compound/cid/{batch_str}/property/"
                f"MolecularFormula,MolecularWeight,CanonicalSMILES,"
                f"IsomericSMILES,InChI,InChIKey/JSON"
            )

            response = client._request(path)

            # Parse batch results (continued)

            if "PropertyTable" in response and "Properties" in response["PropertyTable"]:
                for prop_data in response["PropertyTable"]["Properties"]:
                    cid = str(prop_data.get("CID"))
                    if cid:
                        results[cid] = client._parse_properties(prop_data)

            processed += len(batch)
            logger.info(
                "batch_progress",
                processed=processed,
                total=total_cids,
                progress_pct=processed / total_cids * 100
            )

        except Exception as e:
            logger.warning(
                "batch_request_failed_fallback_individual",
                batch_size=len(batch),
                error=str(e)
            )

            # Fallback: fetch individually

            for cid in batch:
                try:
                    individual_result = client.fetch_properties(cid)
                    if individual_result:
                        results[cid] = individual_result
                    processed += 1
                except Exception as e2:
                    logger.debug(
                        "individual_fetch_failed",
                        cid=cid,
                        error=str(e2)
                    )
                    processed += 1

    return results

```text

**Performance comparison:**

| Approach | 1000 molecules | Requests | Time (estimated) |
|----------|---------------|----------|------------------|
| **Individual** | 1000 | 1000 | 1000 * 0.5s = 500s (~8.3 min) |

| **Batch (100)** | 1000 | 10 | 10 * 0.5s = 5s |

| **Speedup** | - | **100x fewer** | **100x faster** |

### 9.2 Parallel CID Resolution (проект 2 approach)

**Концепция:** Параллельное разрешение CIDs через ThreadPoolExecutor

**Implementation:**

```python

"""Parallel CID resolution for maximum throughput."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple

def resolve_cids_parallel_optimized(
    molecules: list[dict[str, Any]],
    cache: dict[str, str],
    client: OptimalPubChemClient,
    workers: int = 4
) -> dict[str, tuple[str, CIDSource]]:
    """
    Parallel CID resolution with optimal worker count.

    Performance gains:

    - 4 workers: ~4x faster than sequential
    - Automatic load balancing
    - Early completion tracking

    Args:
        molecules: List of molecule dicts
        cache: CID cache
        client: PubChem client
        workers: Number of parallel workers (default 4)

    Returns:
        Dict mapping molecule_chembl_id to (CID, source)
    """
    cid_mapping = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:

        # Submit all tasks

        future_to_molecule = {
            executor.submit(
                resolve_pubchem_cid,
                mol,
                cache,
                client
            ): mol['molecule_chembl_id']
            for mol in molecules
        }

        # Process results as they complete (не ждём всех)

        completed = 0
        total = len(future_to_molecule)

        for future in as_completed(future_to_molecule):
            molecule_id = future_to_molecule[future]
            completed += 1

            try:
                cid, source = future.result(timeout=30.0)

                if cid:
                    cid_mapping[molecule_id] = (cid, source)

                # Log progress every 10%

                if completed % (total // 10) == 0:
                    logger.info(
                        "parallel_resolution_progress",
                        completed=completed,
                        total=total,
                        resolved=len(cid_mapping),
                        progress_pct=completed / total * 100
                    )

            except TimeoutError:
                logger.warning(
                    "cid_resolution_timeout",
                    molecule_id=molecule_id
                )
            except Exception as e:
                logger.error(
                    "cid_resolution_error",
                    molecule_id=molecule_id,
                    error=str(e)
                )

    resolution_rate = len(cid_mapping) / total if total > 0 else 0
    logger.info(
        "parallel_resolution_complete",
        total=total,
        resolved=len(cid_mapping),
        resolution_rate=resolution_rate
    )

    return cid_mapping

```text

**Performance comparison:**

| Workers | 1000 molecules | Time (estimated) | Speedup |
|---------|---------------|------------------|---------|
| **1 (sequential)** | 1000 | 1000 * 1.0s = 1000s (~16.7 min) | 1x |

| **2** | 1000 | ~500s (~8.3 min) | 2x |

| **4** | 1000 | ~250s (~4.2 min) | 4x |

| **8** | 1000 | ~250s (rate limit) | 4x (bottleneck) |

**Note:** 8+ workers не дают выигрыша из-за rate limit 5 req/sec.

### 9.3 Smart Caching Strategies

#### 9.3.1 Cache Warming

```python

"""Pre-populate cache with common molecules."""

def warm_cache_with_common_molecules(
    cache: PubChemCIDCache,
    client: OptimalPubChemClient,
    common_molecules_file: Path
) -> int:
    """
    Warm cache with frequently used molecules.

    Args:
        cache: CID cache
        client: PubChem client
        common_molecules_file: File with list of ChEMBL IDs

    Returns:
        Number of molecules cached
    """

    # Load common molecules (e.g., top 1000 by frequency)

    with open(common_molecules_file) as f:
        chembl_ids = [line.strip() for line in f if line.strip()]

    cached_count = 0

    logger.info(
        "cache_warming_start",
        total_molecules=len(chembl_ids)
    )

    for chembl_id in chembl_ids:
        try:

            # Check if already cached

            cache_key = f"chembl:{chembl_id}"
            if cache.get(cache_key):
                continue

            # Resolve and cache

            # (Implementation depends on data availability)

            # This is a simplified example

            cached_count += 1

        except Exception as e:
            logger.debug(
                "cache_warming_error",
                chembl_id=chembl_id,
                error=str(e)
            )

    logger.info(
        "cache_warming_complete",
        cached_count=cached_count
    )

    return cached_count

```text

#### 9.3.2 Lazy Cache Persistence

```python

"""Lazy cache persistence to reduce I/O."""

class LazyPersistentCache(PubChemCIDCache):
    """Cache with lazy persistence to reduce disk I/O."""

    def __init__(
        self,
        cache_path: Path | str,
        ttl_hours: float = 720,
        persist_interval: int = 100  # Persist every N updates

    ):
        super().__init__(cache_path, ttl_hours)
        self.persist_interval = persist_interval
        self.updates_since_persist = 0

    def set(self, key: str, cid: str, source: str = "unknown") -> None:
        """Set with lazy persistence."""
        super().set(key, cid, source)

        self.updates_since_persist += 1

        # Persist только каждые N updates

        if self.updates_since_persist >= self.persist_interval:
            self.save()
            self.updates_since_persist = 0

    def __del__(self):
        """Ensure cache is saved on destruction."""
        if self._dirty:
            self.save(force=True)

```text

### 9.4 Метрики производительности

```python

"""Performance metrics tracking."""

from dataclasses import dataclass, field
from time import perf_counter
from typing import List

@dataclass
class PerformanceMetrics:
    """Performance metrics for PubChem enrichment."""

    # Timing

    total_duration_seconds: float = 0.0
    cid_resolution_duration: float = 0.0
    properties_fetch_duration: float = 0.0

    # Throughput

    molecules_per_second: float = 0.0
    requests_per_second: float = 0.0

    # Cache

    cache_hit_rate: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0

    # API

    total_api_calls: int = 0
    batch_requests: int = 0
    individual_requests: int = 0

    # Success rates

    cid_resolution_rate: float = 0.0
    properties_enrichment_rate: float = 0.0

    # Errors

    total_errors: int = 0
    rate_limit_hits: int = 0
    timeouts: int = 0

    # Timestamps

    request_timestamps: List[float] = field(default_factory=list)

    def calculate_derived_metrics(self, total_molecules: int) -> None:
        """Calculate derived metrics."""
        if self.total_duration_seconds > 0:
            self.molecules_per_second = total_molecules / self.total_duration_seconds
            self.requests_per_second = self.total_api_calls / self.total_duration_seconds

        total_cache_checks = self.cache_hits + self.cache_misses
        if total_cache_checks > 0:
            self.cache_hit_rate = self.cache_hits / total_cache_checks

    def to_dict(self) -> dict:
        """Convert to dictionary for logging."""
        return {
            'timing': {
                'total_duration_seconds': self.total_duration_seconds,
                'cid_resolution_duration': self.cid_resolution_duration,
                'properties_fetch_duration': self.properties_fetch_duration
            },
            'throughput': {
                'molecules_per_second': self.molecules_per_second,
                'requests_per_second': self.requests_per_second
            },
            'cache': {
                'cache_hit_rate': self.cache_hit_rate,
                'cache_hits': self.cache_hits,
                'cache_misses': self.cache_misses
            },
            'api': {
                'total_api_calls': self.total_api_calls,
                'batch_requests': self.batch_requests,
                'individual_requests': self.individual_requests
            },
            'success_rates': {
                'cid_resolution_rate': self.cid_resolution_rate,
                'properties_enrichment_rate': self.properties_enrichment_rate
            },
            'errors': {
                'total_errors': self.total_errors,
                'rate_limit_hits': self.rate_limit_hits,
                'timeouts': self.timeouts
            }
        }

class PerformanceTracker:
    """Track performance metrics during enrichment."""

    def __init__(self):
        self.metrics = PerformanceMetrics()
        self._start_time: float | None = None

    def start(self) -> None:
        """Start timing."""
        self._start_time = perf_counter()

    def stop(self) -> None:
        """Stop timing."""
        if self._start_time:
            self.metrics.total_duration_seconds = perf_counter() - self._start_time

    def record_api_call(self, is_batch: bool = False) -> None:
        """Record API call."""
        self.metrics.total_api_calls += 1
        if is_batch:
            self.metrics.batch_requests += 1
        else:
            self.metrics.individual_requests += 1

        # Record timestamp for rate calculation

        self.metrics.request_timestamps.append(perf_counter())

    def record_cache_hit(self) -> None:
        """Record cache hit."""
        self.metrics.cache_hits += 1

    def record_cache_miss(self) -> None:
        """Record cache miss."""
        self.metrics.cache_misses += 1

    def record_error(self, error_type: PubChemErrorType) -> None:
        """Record error."""
        self.metrics.total_errors += 1

        if error_type == PubChemErrorType.RATE_LIMIT:
            self.metrics.rate_limit_hits += 1
        elif error_type == PubChemErrorType.TIMEOUT:
            self.metrics.timeouts += 1

    def get_metrics(self, total_molecules: int) -> PerformanceMetrics:
        """Get final metrics."""
        self.metrics.calculate_derived_metrics(total_molecules)
        return self.metrics

```text

**Target performance metrics:**

```yaml

performance_targets:

  # Throughput (continued)

  molecules_per_second: 10.0        # 10 molecules/sec

  requests_per_second: 4.5          # Under API limit (5/sec)

  # Timing (continued)

  avg_cid_resolution_time: 0.5      # 0.5s per CID

  avg_properties_fetch_time: 0.5    # 0.5s per batch

  total_duration_1000_mols: 180     # 3 minutes for 1000 molecules

  # Success rates (continued)

  cid_resolution_rate: 0.85         # 85% успех

  properties_enrichment_rate: 0.80  # 80% успех

  # Cache (continued)

  cache_hit_rate: 0.60              # 60% из cache

  # Errors (continued)

  rate_limit_hits_per_1000: 2       # Max 2 на 1000 молекул

  timeout_rate: 0.01                # Max 1% timeouts

```text

---

## 10. Конфигурация

### 10.1 Базовый стандарт

- Используется единый шаблон `configs/base.yaml` и стандарты из `docs/requirements/10-configuration.md`.

- Профиль testitem подключается через `configs/pipelines/testitem.yaml` (`extends: "../base.yaml"`).

### 10.2 Расширение PubChem

| Путь | Значение по умолчанию | Ограничения | Назначение |
|------|-----------------------|-------------|------------|
| `sources.pubchem.enabled` | `true` | Можно отключить только через CLI/env | Фича-флаг enrichment. |
| `sources.pubchem.http.base_url` | `https://pubchem.ncbi.nlm.nih.gov/rest/pug` | Строка URL | Основной REST endpoint. |
| `sources.pubchem.http.retries.total` | `3` | `1–5` | Ограничение для backoff (PubChem penalizes >5). |
| `sources.pubchem.batch.size` | `100` | `≤ 100` | Верхний предел API batch. |
| `sources.pubchem.cache.cid_mapping.ttl_hours` | `720` | `≥ 0` | Стабильность соответствий CID↔molecule. |
| `sources.pubchem.performance.rate_limit.max_requests` | `5` | `≤ 5` | Соответствует публичному лимиту без API ключа. |
| `postprocess.qc.pubchem_min_enrichment_rate` | `0.70` | `0–1` | Генерирует предупреждение при деградации. |

### 10.3 Переопределения

- **CLI:**
  - `--set sources.pubchem.enabled=false` — отключить обогащение.
  - `--set sources.pubchem.performance.workers=2` — снизить параллельность в тестовом окружении.

- **Переменные окружения:**
  - `BIOETL_SOURCES__PUBCHEM__HTTP__HEADERS__USER_AGENT="BioactivityETL/2.0 (mailto:data@example.org)"` — обязательный заголовок.
  - `BIOETL_SOURCES__PUBCHEM__API_KEY=<secret>` — привязка к приватному ключу (используется в client).
  - `BIOETL_SOURCES__PUBCHEM__CACHE__CID_MAPPING__PATH=/mnt/cache/pubchem_cid_cache.json` — переопределение пути.

### 10.4 Валидация

- Проверка конфигурации осуществляется моделью `PipelineConfig` (см. §4 `10-configuration`).

- Дополнительные валидаторы: `sources.pubchem.http.headers.User-Agent` должен содержать контактный email; проверяется функцией `validate_pubchem_headers()` внутри загрузчика.

- Нарушения лимитов (`batch.size > 100`, `rate_limit.max_requests > 5`) считаются фатальными ошибками конфигурации.

## Заключение

Этот документ описывает **оптимальный подход** к извлечению данных testitem из PubChem API, объединяя лучшие практики из двух существующих проектов:

### Ключевые рекомендации

#### 1. Используйте Batch API (из проекта 1)

- 100x меньше запросов

- Значительное ускорение обработки

- Автоматический fallback на individual requests

#### 2. Реализуйте Multi-Level Caching (из проекта 2)

- In-memory cache для скорости

- Persistent CID cache для долговременного хранения

- File cache для отладки (опционально)

#### 3. Применяйте Parallel Processing (из проекта 2)

- 4 workers для CID resolution

- ThreadPoolExecutor для максимальной производительности

- Соблюдение rate limits

#### 4. Обеспечьте Graceful Degradation (оба проекта)

- PubChem всегда опционален

- Продолжение pipeline при любых сбоях

- Детальное логирование для диагностики

#### 5. Мониторьте активно

- Enrichment rate ≥80%

- Cache hit rate ≥60%

- API calls <5 req/sec

- Error rate <2%

### Сравнение проектов

| Feature | Проект 1 | Проект 2 | Рекомендация |
|---------|----------|----------|--------------|
| **Batch API** | ✅ | ❌ | ✅ Использовать |

| **Parallel processing** | ❌ | ✅ | ✅ Использовать |

| **Persistent cache** | ❌ | ✅ | ✅ Использовать |

| **Simple config** | ✅ | ❌ | ✅ Упростить |

| **Service outage tracking** | ❌ | ✅ | ✅ Использовать |

| **Telemetry** | ⚠️ | ✅ | ✅ Использовать |

### Целевые метрики

```yaml

target_performance:
  cid_resolution_rate: 0.85       # 85% molecules resolved

  enrichment_rate: 0.80           # 80% получили properties

  cache_hit_rate: 0.60            # 60% из cache

  molecules_per_second: 10.0      # Throughput

  requests_per_second: 4.5        # Under API limit

  # Timing для 1000 molecules

  total_duration_seconds: 180     # 3 minutes

  cid_resolution_duration: 120    # 2 minutes

  properties_fetch_duration: 60   # 1 minute

```text

### Следующие шаги

1. **Для проекта 1:** Добавить persistent cache и parallel processing

2. **Для проекта 2:** Добавить batch API calls и упростить конфигурацию

3. **Для нового проекта:** Использовать этот документ как reference implementation

### Ссылки на исходный код

**Проект 1 (bioactivity_data_acquisition5):**

- `src/library/clients/pubchem.py` - HTTP client с batch support

- `src/library/testitem/extract.py` - Extraction logic

- `configs/pipelines/testitem.yaml` - Configuration

**Проект 2 (ChEMBL_data_acquisition6):**

- `library/clients/pubchem.py` - Low-level client

- `library/pipelines/testitem/pubchem.py` - Pipeline integration

- `config/config.yaml` - Configuration

---

**Документ версия:** 1.0
**Дата создания:** 2024-10-28
**Авторы:** Synthesis из bioactivity_data_acquisition5 и ChEMBL_data_acquisition6
**Статус:** Production Ready

