from __future__ import annotations

import responses

from library.clients.chembl import ChEMBLClient


@responses.activate
def test_fetch_activities_pages_through_results() -> None:
    client = ChEMBLClient("https://example.com")
    responses.add(
        responses.GET,
        "https://example.com/activities",
        json={
            "activities": [
                {
                    "assay_id": 1,
                    "molecule_chembl_id": "CHEMBL1",
                    "standard_value": 1.0,
                    "standard_units": "nM",
                    "activity_comment": None,
                }
            ],
            "next_page": True,
        },
        status=200,
    )
    responses.add(
        responses.GET,
        "https://example.com/activities",
        json={
            "activities": [
                {
                    "assay_id": 2,
                    "molecule_chembl_id": "CHEMBL2",
                    "standard_value": 2.0,
                    "standard_units": "uM",
                    "activity_comment": "active",
                }
            ],
            "next_page": False,
        },
        status=200,
    )
    activities = list(client.fetch_activities("/activities", page_size=100))
    client.close()
    assert len(activities) == 2
    assert activities[1]["standard_units"] == "uM"
