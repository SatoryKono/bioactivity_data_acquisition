"""Semantic Scholar client."""
from __future__ import annotations

from typing import Dict, Iterable, List, Mapping

from .base import BaseClient
from ..io.normalize import coerce_text, normalise_doi


_FIELDS = "paperId,title,externalIds,doi"


class SemanticScholarClient(BaseClient):
    SOURCE = "semscholar"

    def __init__(self, *, run_id: str, session=None, global_limiter=None) -> None:
        super().__init__(
            source=self.SOURCE,
            base_url="https://api.semanticscholar.org/graph/v1",
            session=session,
            per_client_rps=2.0,
            global_limiter=global_limiter,
            run_id=run_id,
        )

    def fetch_by_pmid(self, pmid: str | None) -> Dict[str, str | None]:
        pmid_text = coerce_text(pmid)
        if not pmid_text:
            return {}
        path = f"paper/PMID:{pmid_text}"
        params = {"fields": _FIELDS}
        self.logger.info("fetching semantic scholar by pmid", extra={"pmid": pmid_text})
        payload = self.get_json(path, params=params)
        return parse_paper(payload)

    def fetch_batch_by_pmids(self, pmids: Iterable[str]) -> Mapping[str, Dict[str, str | None]]:
        unique = [coerce_text(p) for p in pmids]
        filtered: List[str] = [p for p in unique if p]
        if not filtered:
            return {}
        payload = {"ids": [f"PMID:{pmid}" for pmid in filtered], "fields": _FIELDS}
        self.logger.info("batch fetching semantic scholar", extra={"extra_fields": {"count": len(filtered)}})
        response = self.post_json("paper/batch", payload)
        if not isinstance(response, list):
            return {}
        results: Dict[str, Dict[str, str | None]] = {}
        for item in response:
            parsed = parse_paper(item)
            pmid_value = parsed.get("semscholar.pmid")
            if pmid_value:
                results[pmid_value] = parsed
        return results


def parse_paper(payload) -> Dict[str, str | None]:
    if not isinstance(payload, dict):
        return {}
    external_ids = payload.get("externalIds") if isinstance(payload.get("externalIds"), dict) else {}
    pmid = coerce_text(external_ids.get("PMID"))
    doi = payload.get("doi") or external_ids.get("DOI")
    return {
        "semscholar.paper_id": coerce_text(payload.get("paperId")),
        "semscholar.title": coerce_text(payload.get("title")),
        "semscholar.doi": normalise_doi(doi),
        "semscholar.pmid": pmid,
    }

