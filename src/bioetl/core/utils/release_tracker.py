"""Helpers for capturing and normalising ChEMBL release information.

This module consolidates the release tracking contract in one place so that
pipeline mixins and clients do not have to re-implement the same rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from logging import getLogger
from typing import Any, Mapping

from structlog.stdlib import BoundLogger

from bioetl.chembl.common.mixins import ChemblOptionalStringValueMixin

__all__ = [
    "ChemblHandshakeResult",
    "ChemblReleaseMixin",
    "perform_chembl_handshake",
]

_RELEASE_KEYS: tuple[str, ...] = (
    "chembl_release",
    "chembl_db_version",
    "release",
    "version",
)


@dataclass(slots=True)
class ChemblHandshakeResult:
    """Container for handshake payload, release identifier and timestamps."""

    payload: Mapping[str, Any]
    release: str | None
    requested_at_utc: datetime


class ChemblReleaseMixin(ChemblOptionalStringValueMixin):
    """Mixin с реализацией отслеживания ChEMBL release и handshake-хелперами.

    Контракт:
    - При извлечении релиза метод ищет ключи в порядке: "chembl_release",
      "chembl_db_version", "release", "version" (см. :data:`_RELEASE_KEYS`).
    - Нормализация: любые non-None значения приводятся к ``str()``, затем
      ``.strip()``; пустые строки приводятся к ``None``.
    - Выполняется попытка вызвать handshake(``endpoint=...``, ``enabled=...``),
      если у клиента есть callable ``handshake``; поддерживается старый
      сигнатурный вариант ``handshake(endpoint=...)``.
    - Если метод ``handshake`` отсутствует, пытается взять релиз из запасного
      атрибута клиента (``release_attr_fallback``, по умолчанию
      ``"chembl_release"``).
    - Возвращается :class:`ChemblHandshakeResult`, релиз кешируется через
      ``_set_chembl_release()``, и всегда вызывается запись метаданных
      (``record_extract_metadata``) с ``chembl_release`` и ``requested_at_utc``.
    """

    _chembl_release: str | None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._chembl_release = None
        super().__init__(*args, **kwargs)

    @property
    def chembl_release(self) -> str | None:
        """Return the cached ChEMBL release identifier."""

        return self._get_optional_string_value("_chembl_release", field_name="chembl_release")

    def _set_chembl_release(self, value: str | None) -> None:
        """Store a ChEMBL release identifier after normalisation."""

        self._set_optional_string_value("_chembl_release", value, field_name="chembl_release")


def _normalize_release(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _extract_release_from_payload(payload: Mapping[str, Any]) -> str | None:
    for key in _RELEASE_KEYS:
        if key in payload:
            release = _normalize_release(payload.get(key))
            if release:
                return release
    return None


def perform_chembl_handshake(
    chembl_client: Any,
    *,
    endpoint: str | None = None,
    enabled: bool = True,
    release_attr_fallback: str = "chembl_release",
    log: BoundLogger | None = None,
) -> ChemblHandshakeResult:
    """Выполнить handshake с ChEMBL и вернуть нормализованный release.

    - Метод ищет значения ключей ``"chembl_release"``, ``"chembl_db_version"``,
      ``"release"`` и ``"version"`` в указанном порядке. Первое непустое
      значение нормализуется (``str`` + ``strip``) и используется как release.
    - При наличии ``chembl_client.handshake`` вызывается
      ``handshake(endpoint=...)``. Если клиент реализует legacy-сигнатуру,
      допускается позиционный вызов ``handshake(endpoint)``.
    - Когда ``enabled`` равно ``False``, handshake не выполняется, логируется
      событие ``chembl_handshake_skipped``, но по-прежнему возвращается
      :class:`ChemblHandshakeResult` с отметкой времени.
    - Если метод ``handshake`` отсутствует или payload не содержит release,
      предпринимается попытка взять его из атрибута клиента
      ``release_attr_fallback`` (по умолчанию ``chembl_release``).
    - Функция всегда логирует результат (payload/release) и возвращает
      :class:`ChemblHandshakeResult(payload, release, requested_at_utc)`. Это
      позволяет вызывающей стороне записать ``record_extract_metadata`` и
      синхронизировать внутренний кеш через ``_set_chembl_release``.
    """

    logger = log if log is not None else getLogger("bioetl.release_tracker")
    requested_at = datetime.now(timezone.utc)

    if not enabled:
        logger.info("chembl_handshake_skipped", endpoint=endpoint, enabled=False)
        release = _normalize_release(getattr(chembl_client, release_attr_fallback, None))
        return ChemblHandshakeResult(payload={}, release=release, requested_at_utc=requested_at)

    payload: Mapping[str, Any] = {}
    handshake_method = getattr(chembl_client, "handshake", None)

    if callable(handshake_method):
        try:
            if endpoint is None:
                payload = handshake_method()
            else:
                payload = handshake_method(endpoint=endpoint)
        except TypeError:
            if endpoint is None:
                payload = handshake_method()
            else:
                payload = handshake_method(endpoint)
    else:
        logger.info(
            "chembl_handshake_missing",
            endpoint=endpoint,
            release_attr_fallback=release_attr_fallback,
        )

    release = _extract_release_from_payload(payload) if payload else None
    if release is None:
        release = _normalize_release(getattr(chembl_client, release_attr_fallback, None))

    logger.info(
        "chembl_handshake_completed",
        endpoint=endpoint,
        has_payload=bool(payload),
        release=release,
    )
    return ChemblHandshakeResult(payload=payload, release=release, requested_at_utc=requested_at)
