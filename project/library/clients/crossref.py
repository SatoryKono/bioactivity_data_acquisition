"""Crossref client."""
from __future__ import annotations

from typing import Any, Dict, List

from .base import BaseClient
from ..io.normalize import coerce_text, normalise_doi


class CrossrefClient(BaseClient):
    SOURCE = "crossref"

    def __init__(self, *, run_id: str, session=None, global_limiter=None) -> None:
        super().__init__(
            source=self.SOURCE,
            base_url="https://api.crossref.org",
            session=session,
            per_client_rps=5.0,
            global_limiter=global_limiter,
            run_id=run_id,
        )

    def fetch_by_doi(self, doi: str | None) -> Dict[str, Any]:
        norm = normalise_doi(doi)
        if not norm:
            return {}
        encoded = self.encode_identifier(norm)
        self.logger.info("fetching crossref by doi", extra={"doi": norm})
        payload = self.get_json(f"works/{encoded}")
        message = payload.get("message", {})
        return parse_work(message)

    def fetch_by_pmid(self, pmid: str | None) -> Dict[str, Any]:
        pmid_text = coerce_text(pmid)
        if not pmid_text:
            return {}
        self.logger.info("fetching crossref by pmid", extra={"pmid": pmid_text})
        payload = self.get_json("works", params={"filter": f"pmid:{pmid_text}"})
        message = payload.get("message", {})
        items: List[Dict[str, Any]] = message.get("items", [])  # type: ignore[assignment]
        if not items:
            return {}
        return parse_work(items[0])


def parse_work(message: Dict[str, Any]) -> Dict[str, Any]:
    title = message.get("title") or []
    container_title = message.get("container-title") or []
    return {
        "crossref.doi": normalise_doi(message.get("DOI")),
        "crossref.title": coerce_text(" ".join(title) if isinstance(title, list) else title),
        "crossref.journal": coerce_text(
            container_title[0] if isinstance(container_title, list) and container_title else container_title
        ),
        "crossref.pmid": coerce_text(message.get("pubmed-id")),
    }

