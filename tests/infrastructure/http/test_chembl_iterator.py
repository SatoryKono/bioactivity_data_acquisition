"""Unit tests for ChemblEntityFetcherBase."""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest  # type: ignore[reportMissingImports]

from bioetl.clients.chembl_config import EntityConfig
from bioetl.clients.client_chembl_entity_base import ChemblEntityFetcherBase


@pytest.fixture
def chembl_config() -> EntityConfig:
    """Return a minimal EntityConfig for iterator tests."""

    return EntityConfig(
        "/entity.json",
        "entity_id",
        "entity_id__in",
        "entities",
        "entity",
        ("entity_id",),
        False,
        100,
        None,
        (),
        {},
        10,
        True,
    )


@pytest.fixture
def mock_logger() -> Iterator[MagicMock]:
    """Patch UnifiedLogger.get to avoid touching global logging."""

    with patch("bioetl.clients.client_chembl_entity_base.UnifiedLogger.get") as mock_get_logger:
        bound_logger = MagicMock()
        mock_get_logger.return_value = MagicMock(bind=MagicMock(return_value=bound_logger))
        yield bound_logger


@pytest.mark.unit  # type: ignore[reportUnknownMemberType]
class TestChemblEntityFetcherBase:
    """Unit tests for shared entity fetcher behaviour."""

    def test_handshake_caches_release(
        self,
        chembl_config: EntityConfig,
        mock_logger: MagicMock,
    ) -> None:
        """Ensure handshake stores chembl_release and returns payload."""
        chembl_client = MagicMock()
        chembl_client.handshake.return_value = {"chembl_db_version": "34"}

        fetcher = ChemblEntityFetcherBase(
            chembl_client,
            chembl_config,
            batch_size=25,
            max_url_length=128,
        )

        payload = fetcher.handshake()

        assert payload == {"chembl_db_version": "34"}
        assert fetcher.chembl_release == "34"
        chembl_client.handshake.assert_called_once_with(None)
        mock_logger.info.assert_called_with(
            "entity.handshake",
            handshake_endpoint=None,
            handshake_enabled=True,
            chembl_release="34",
        )

    def test_iterate_all_respects_limit_and_select_fields(
        self,
        chembl_config: EntityConfig,
        mock_logger: MagicMock,
    ) -> None:
        """Ensure iterate_all forwards params and respects limit."""
        chembl_client = MagicMock()
        chembl_client.paginate.return_value = iter(
            [
                {"entity_id": "E1"},
                {"entity_id": "E2"},
                {"entity_id": "E3"},
            ]
        )

        fetcher = ChemblEntityFetcherBase(
            chembl_client,
            chembl_config,
            batch_size=10,
            max_url_length=128,
        )

        records = list(fetcher.iterate_all(limit=2, select_fields=("field1", "field2")))

        assert records == [{"entity_id": "E1"}, {"entity_id": "E2"}]
        chembl_client.paginate.assert_called_once()
        call_args = chembl_client.paginate.call_args
        assert call_args.args[0] == chembl_config.endpoint
        assert call_args.kwargs["page_size"] == 2
        assert call_args.kwargs["items_key"] == chembl_config.items_key
        params = call_args.kwargs["params"]
        assert params["limit"] == 2
        assert params["only"] == "field1,field2"

    def test_chunk_identifiers_enforces_url_length(
        self,
        chembl_config: EntityConfig,
        mock_logger: MagicMock,
    ) -> None:
        """Ensure identifier chunking respects URL length constraints."""
        chembl_client = MagicMock()
        fetcher = ChemblEntityFetcherBase(
            chembl_client,
            chembl_config,
            batch_size=5,
            max_url_length=20,
        )

        chunks = list(
            fetcher._chunk_identifiers(  # type: ignore[attr-defined]
                (
                    "AAAAAAAAAA",
                    "BBBBBBBBBB",
                    "CCCCCCCCCC",
                ),
                select_fields=None,
            )
        )

        assert chunks == [
            ("AAAAAAAAAA",),
            ("BBBBBBBBBB",),
            ("CCCCCCCCCC",),
        ]

    def test_resolve_page_size_limits_to_batch(
        self,
        chembl_config: EntityConfig,
        mock_logger: MagicMock,
    ) -> None:
        """Ensure page size resolution respects batch size constraints."""
        chembl_client = MagicMock()
        fetcher = ChemblEntityFetcherBase(
            chembl_client,
            chembl_config,
            batch_size=25,
            max_url_length=128,
        )

        assert fetcher._resolve_page_size(None, None) == 25  # type: ignore[attr-defined]
        assert fetcher._resolve_page_size(0, None) == 25  # type: ignore[attr-defined]
        assert fetcher._resolve_page_size(-5, None) == 25  # type: ignore[attr-defined]
        assert fetcher._resolve_page_size(10, None) == 10  # type: ignore[attr-defined]
        assert fetcher._resolve_page_size(100, None) == 25  # type: ignore[attr-defined]
        assert fetcher._resolve_page_size(50, 5) == 5  # type: ignore[attr-defined]

