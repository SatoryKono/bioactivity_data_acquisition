"""HTTP client for interacting with the PubChem PUG-REST API."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from bioetl.config import PipelineConfig
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.client_factory import APIClientFactory, ensure_target_source_config
from bioetl.core.deprecation import warn_legacy_client
from bioetl.core.logger import UnifiedLogger

from ..parser.pubchem_parser import PubChemParser
from ..request.builder import PubChemRequestBuilder

__all__ = ["PubChemClient"]


logger = UnifiedLogger.get(__name__)

warn_legacy_client(__name__, replacement="bioetl.adapters.pubchem")


class PubChemClient:
    """High-level operations for resolving PubChem CIDs and properties."""

    def __init__(
        self,
        api_client: UnifiedAPIClient,
        *,
        batch_size: int = 100,
        properties: Sequence[str] | None = None,
    ) -> None:
        self.api_client = api_client
        self.batch_size = max(1, min(int(batch_size), 100))
        if properties is None:
            properties = PubChemRequestBuilder.get_default_properties()
        self.properties = tuple(properties)

    def resolve_cid(self, inchikey: str) -> int | None:
        """Resolve a single InChIKey into a PubChem CID."""

        if not inchikey:
            return None
        endpoint = PubChemRequestBuilder.build_cid_lookup_url(inchikey)
        payload = self._request_json(endpoint)
        return PubChemParser.parse_cid_response(payload)

    def resolve_cids_batch(self, inchikeys: Iterable[str]) -> Mapping[str, dict[str, Any]]:
        """Resolve a collection of identifiers into CIDs."""

        results: dict[str, dict[str, Any]] = {}
        for position, raw_inchikey in enumerate(inchikeys, start=1):
            inchikey = (raw_inchikey or "").strip().upper()
            if not inchikey:
                continue
            metadata: dict[str, Any] = {
                "cid": None,
                "cid_source": "failed",
                "attempt": position,
                "fallback_used": False,
            }
            try:
                cid = self.resolve_cid(inchikey)
            except Exception as exc:  # noqa: BLE001 - propagate logging
                logger.warning(
                    "pubchem_cid_resolution_failed",
                    identifier=inchikey[:20],
                    error=str(exc),
                )
                cid = None
            if cid is not None:
                metadata["cid"] = cid
                metadata["cid_source"] = "inchikey"
                logger.debug("pubchem_cid_resolved", identifier=inchikey[:20], cid=cid)
            results[inchikey] = metadata
        logger.info("pubchem_cid_resolution_completed", total=len(results))
        return results

    def fetch_properties_batch(self, cids: Sequence[int | str]) -> list[dict[str, Any]]:
        """Fetch compound properties for a sequence of CIDs."""

        if not cids:
            return []
        endpoint = PubChemRequestBuilder.build_properties_url(cids, self.properties)
        payload = self._request_json(endpoint)
        return PubChemParser.parse_properties_response(payload)

    def enrich_batch(self, inchikeys: Sequence[str]) -> list[dict[str, Any]]:
        """Resolve CIDs and fetch their properties for the provided identifiers."""

        if not inchikeys:
            return []

        resolution = self.resolve_cids_batch(inchikeys)
        cid_to_properties: dict[int, dict[str, Any]] = {}
        resolved_cids = [info.get("cid") for info in resolution.values() if info.get("cid")]
        if resolved_cids:
            chunk: list[int] = []
            for cid in resolved_cids:
                if cid is None:
                    continue
                chunk.append(int(cid))
                if len(chunk) >= self.batch_size:
                    cid_to_properties.update(self._fetch_properties_chunk(chunk))
                    chunk = []
            if chunk:
                cid_to_properties.update(self._fetch_properties_chunk(chunk))

        results: list[dict[str, Any]] = []
        for identifier in inchikeys:
            lookup = (identifier or "").strip().upper()
            info = resolution.get(lookup, {})
            cid = info.get("cid")
            record: dict[str, Any] = {}
            if cid is not None:
                record.update(cid_to_properties.get(int(cid), {}))
                record.setdefault("CID", int(cid))
            record["_source_identifier"] = lookup or None
            record["_cid_source"] = info.get("cid_source", "inchikey" if cid else "failed")
            record["_enrichment_attempt"] = info.get("attempt", 1)
            record["_fallback_used"] = info.get("fallback_used", False)
            results.append(record)
        return results

    def _fetch_properties_chunk(self, chunk: Sequence[int]) -> Mapping[int, dict[str, Any]]:
        """Fetch a chunk of property records and return them keyed by CID."""

        response = self.fetch_properties_batch(chunk)
        records: dict[int, dict[str, Any]] = {}
        for entry in response:
            cid = entry.get("CID")
            try:
                if cid is not None:
                    records[int(cid)] = dict(entry)
            except (TypeError, ValueError):
                continue

        synonyms_map = self._fetch_synonyms(chunk)
        for cid, synonyms in synonyms_map.items():
            if not synonyms:
                continue
            records.setdefault(cid, {})["Synonym"] = list(synonyms)

        registry_ids_map, rn_map = self._fetch_registry_and_rn(chunk)
        for cid, registry_ids in registry_ids_map.items():
            if not registry_ids:
                continue
            first = next((value for value in registry_ids if value is not None), None)
            if first is None:
                continue
            records.setdefault(cid, {})["RegistryID"] = str(first)

        for cid, rns in rn_map.items():
            if not rns:
                continue
            first = next((value for value in rns if value is not None), None)
            if first is None:
                continue
            records.setdefault(cid, {})["RN"] = str(first)
        return records

    def _fetch_synonyms(self, chunk: Sequence[int]) -> Mapping[int, list[Any]]:
        """Fetch synonym lists for each provided CID individually."""

        results: dict[int, list[Any]] = {}
        for cid in chunk:
            try:
                cid_int = int(cid)
            except (TypeError, ValueError):
                continue
            endpoint = PubChemRequestBuilder.build_synonyms_url(cid_int)
            payload = self._request_json(endpoint)
            parsed = PubChemParser.parse_synonyms_response(payload)
            results.update(parsed)
        return results

    def _fetch_registry_and_rn(
        self, chunk: Sequence[int]
    ) -> tuple[Mapping[int, list[Any]], Mapping[int, list[Any]]]:
        """Fetch registry identifiers and RN numbers for each CID."""

        registry_results: dict[int, list[Any]] = {}
        rn_results: dict[int, list[Any]] = {}
        for cid in chunk:
            try:
                cid_int = int(cid)
            except (TypeError, ValueError):
                continue
            endpoint = PubChemRequestBuilder.build_registry_xrefs_url(cid_int)
            payload = self._request_json(endpoint)
            registry_results.update(PubChemParser.parse_registry_ids_response(payload))
            rn_results.update(PubChemParser.parse_rn_response(payload))
        return registry_results, rn_results

    def _request_json(self, endpoint: str) -> dict[str, Any] | None:
        """Wrapper around :meth:`UnifiedAPIClient.request_json` with logging."""

        try:
            result = self.api_client.request_json(endpoint)
            return dict(result) if isinstance(result, dict) else result  # type: ignore[return-value]
        except Exception as exc:  # noqa: BLE001 - centralised logging
            logger.error("pubchem_request_failed", endpoint=endpoint, error=str(exc))
            raise

    @classmethod
    def from_config(cls, config: PipelineConfig) -> tuple[PubChemClient, UnifiedAPIClient] | tuple[None, None]:
        """Instantiate the client from pipeline configuration."""

        source = config.sources.get("pubchem") if config.sources else None
        source_config = ensure_target_source_config(
            source,
            defaults={
                "enabled": True,
                "base_url": "https://pubchem.ncbi.nlm.nih.gov/rest/pug",
                "batch_size": 100,
                "rate_limit": {"max_calls": 5, "period": 1.0},
            },
        )
        if not source_config.enabled:
            logger.info("pubchem_client_disabled", enabled=False)
            return None, None

        factory = APIClientFactory.from_pipeline_config(config)
        api_config = factory.create("pubchem", source_config)
        api_client = UnifiedAPIClient(api_config)
        batch_size = source_config.batch_size or 100
        client = cls(api_client, batch_size=batch_size)
        return client, api_client
