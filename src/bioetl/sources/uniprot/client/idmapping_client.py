"""Client for interacting with the UniProt ID mapping service."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable
from typing import Any

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.sources.uniprot.request import build_idmapping_payload

__all__ = ["UniProtIdMappingClient"]


@dataclass(slots=True)
class UniProtIdMappingClient:
    """Helper around :class:`UnifiedAPIClient` for ID mapping workflows."""

    api: UnifiedAPIClient
    run_endpoint: str = "/run"
    status_endpoint: str = "/status/{job_id}"
    results_endpoint: str = "/results/{job_id}"
    default_source: str = "UniProtKB_AC-ID"
    default_target: str = "UniProtKB"

    def map(
        self,
        identifiers: Iterable[str | int | float | None],
        *,
        source: str | None = None,
        target: str | None = None,
    ) -> str | None:
        """Submit a mapping job and return the assigned job identifier."""

        payload = build_idmapping_payload(
            identifiers,
            source=source or self.default_source,
            target=target or self.default_target,
        )
        if not payload.get("ids"):
            return None

        response = self.api.request_json(self.run_endpoint, method="POST", data=payload)
        job_id = response.get("jobId") or response.get("job_id")
        if isinstance(job_id, str):
            job_id = job_id.strip()
        return job_id or None

    def poll(self, job_id: str) -> dict[str, Any]:
        """Retrieve job status and results for the provided ``job_id``."""

        status_payload = self.api.request_json(
            self.status_endpoint.format(job_id=job_id)
        )
        if not isinstance(status_payload, dict):
            status_payload = {}
        job_status = (
            status_payload.get("jobStatus")
            or status_payload.get("status")
            or status_payload.get("job_status")
            or ""
        )
        normalized_status = str(job_status).upper()

        result: dict[str, Any] = {
            "job_id": job_id,
            "status": normalized_status or "",
            "status_payload": status_payload,
        }

        if normalized_status in {"FINISHED", "FINISHED_WITH_WARNING", "FINISHED_WITH_WARNINGS", "SUCCESS", "COMPLETE", "FAILED"}:
            results_payload = self.api.request_json(
                self.results_endpoint.format(job_id=job_id)
            )
            if not isinstance(results_payload, dict):
                results_payload = {}
            result["results_payload"] = results_payload
            result["results"] = (
                results_payload.get("results")
                or results_payload.get("mappedTo")
                or []
            )
            result["failed_ids"] = (
                results_payload.get("failedIds")
                or results_payload.get("failed")
                or []
            )
        return result
