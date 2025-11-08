"""Centralized factory for configured HTTP API clients."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from importlib import metadata
from typing import Mapping, MutableMapping

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bioetl.core.logger import UnifiedLogger

__all__ = [
    "ApiRetryPolicy",
    "ApiTimeouts",
    "ApiConnectionLimits",
    "ApiProfile",
    "get_api_client",
    "profile_from_http_config",
]


_LOG = UnifiedLogger.get(__name__).bind(component="api_factory")


def _default_user_agent() -> str:
    """Return a deterministic User-Agent string for BioETL."""

    try:
        version = metadata.version("bioetl")
    except metadata.PackageNotFoundError:  # pragma: no cover - fallback for editable installs
        version = "0.0.0"
    return f"bioetl/{version} (https://github.com/openai/bioetl)"


@dataclass(frozen=True)
class ApiRetryPolicy:
    """Retry strategy description for transport-level retries."""

    total: int = 5
    backoff_factor: float = 0.5
    backoff_max: float = 60.0
    statuses: tuple[int, ...] = (408, 429, 500, 502, 503, 504)
    methods: frozenset[str] = frozenset({"GET", "HEAD", "OPTIONS"})
    respect_retry_after: bool = True
    retry_on_connection_errors: bool = True
    retry_on_read_errors: bool = True
    retry_on_other_errors: bool = False


@dataclass(frozen=True)
class ApiTimeouts:
    """Fine grained timeout configuration for an API client."""

    connect: float = 5.0
    read: float = 5.0
    write: float = 5.0
    pool: float = 5.0
    total: float = 60.0

    def as_requests_timeout(self) -> tuple[float, float]:
        """Return a tuple suitable for :mod:`requests` timeout parameter."""

        connect = min(self.connect, self.total)
        remaining = max(self.total - connect, 0.0)
        read = min(self.read, remaining) if remaining else self.read
        if read <= 0:
            read = self.read
        return (connect, read)


@dataclass(frozen=True)
class ApiConnectionLimits:
    """Connection pool settings for the HTTP adapter."""

    max_connections: int = 10
    max_keepalive: int = 10
    pool_block: bool = True


@dataclass(frozen=True)
class ApiProfile:
    """Aggregate API client profile returned by configuration."""

    name: str = "default"
    timeouts: ApiTimeouts = field(default_factory=ApiTimeouts)
    limits: ApiConnectionLimits = field(default_factory=ApiConnectionLimits)
    retries: ApiRetryPolicy = field(default_factory=ApiRetryPolicy)
    headers: Mapping[str, str] = field(default_factory=dict)
    proxies: Mapping[str, str] | None = None
    verify: bool | str = True
    cert: str | tuple[str, str] | None = None
    trust_env: bool = False
    max_redirects: int = 30


class _ConfiguredSession(requests.Session):
    """Session subclass injecting default timeouts for every request."""

    def __init__(self, *, default_timeout: tuple[float, float]) -> None:
        super().__init__()
        self._default_timeout = default_timeout

    def request(self, method: str, url: str, **kwargs):  # type: ignore[override]
        if "timeout" not in kwargs or kwargs["timeout"] is None:
            kwargs["timeout"] = self._default_timeout
        return super().request(method, url, **kwargs)


class _LoggingRetry(Retry):
    """Retry subclass emitting structured logs for every retry attempt."""

    def __init__(self, *, logger: logging.Logger, profile_name: str, **kwargs) -> None:
        self._logger = logger
        self._profile_name = profile_name
        super().__init__(**kwargs)

    def increment(self, method, url, response=None, error=None, *_args, **_kwargs):  # type: ignore[override]
        new_retry = super().increment(method, url, response=response, error=error, *_args, **_kwargs)
        attempt = len(new_retry.history) + 1
        wait_seconds = new_retry.get_backoff_time()
        status_code = getattr(response, "status", None)
        self._logger.debug(
            "http.retry",
            attempt=attempt,
            wait_seconds=wait_seconds,
            method=method,
            url=url,
            status_code=status_code,
            error=str(error) if error else None,
            profile=self._profile_name,
            status_remaining=getattr(new_retry, "status", None),
            status_forcelist=list(self.status_forcelist),
        )
        return new_retry

    def new(self, **kwargs):  # type: ignore[override]
        kwargs.setdefault("logger", self._logger)
        kwargs.setdefault("profile_name", self._profile_name)
        return super().new(**kwargs)


class _LoggingHTTPAdapter(HTTPAdapter):
    """Adapter that wires retry strategy and connection pool settings."""

    def __init__(self, *, profile: ApiProfile) -> None:
        retry_policy = profile.retries
        retry_attempts = max(retry_policy.total, 0)
        status_attempts = retry_attempts + 1 if retry_attempts else 0
        retry = _LoggingRetry(
            logger=_LOG,
            profile_name=profile.name,
            total=None,
            connect=retry_attempts if retry_policy.retry_on_connection_errors else 0,
            read=retry_attempts if retry_policy.retry_on_read_errors else 0,
            other=retry_attempts if retry_policy.retry_on_other_errors else 0,
            status=status_attempts,
            allowed_methods=retry_policy.methods,
            status_forcelist=retry_policy.statuses,
            backoff_factor=retry_policy.backoff_factor,
            backoff_max=retry_policy.backoff_max,
            respect_retry_after_header=retry_policy.respect_retry_after,
            raise_on_status=False,
            raise_on_redirect=False,
        )
        super().__init__(
            pool_connections=profile.limits.max_connections,
            pool_maxsize=profile.limits.max_keepalive,
            max_retries=retry,
            pool_block=profile.limits.pool_block,
        )


def _prepare_session(session: _ConfiguredSession, profile: ApiProfile) -> None:
    headers: MutableMapping[str, str] = {"User-Agent": _default_user_agent()}
    headers.update(profile.headers)
    session.headers.clear()
    session.headers.update(headers)
    if profile.proxies:
        session.proxies.update(profile.proxies)
    session.verify = profile.verify
    if profile.cert:
        session.cert = profile.cert
    session.trust_env = profile.trust_env
    session.max_redirects = profile.max_redirects
    adapter = _LoggingHTTPAdapter(profile=profile)
    session.mount("http://", adapter)
    session.mount("https://", adapter)


def get_api_client(profile: ApiProfile) -> requests.Session:
    """Build and return a configured :class:`requests.Session` for the profile."""

    session = _ConfiguredSession(default_timeout=profile.timeouts.as_requests_timeout())
    _prepare_session(session, profile)
    _LOG.debug("http.session.created", profile=profile.name)
    return session


def profile_from_http_config(config: "HTTPClientConfig", *, name: str = "default") -> ApiProfile:
    """Translate :class:`HTTPClientConfig` into :class:`ApiProfile`."""

    from bioetl.config.models.policies import HTTPClientConfig  # Lazy import to avoid cycle

    if not isinstance(config, HTTPClientConfig):  # pragma: no cover - defensive guard
        msg = "config must be an instance of HTTPClientConfig"
        raise TypeError(msg)
    timeouts = ApiTimeouts(
        connect=float(config.connect_timeout_sec),
        read=float(config.read_timeout_sec),
        write=float(config.write_timeout_sec),
        pool=float(config.pool_timeout_sec),
        total=float(config.timeout_sec),
    )
    limits = ApiConnectionLimits(
        max_connections=int(config.pool_connections),
        max_keepalive=int(config.pool_maxsize),
        pool_block=bool(config.pool_block),
    )
    retries = ApiRetryPolicy(
        total=int(config.retries.total),
        backoff_factor=float(config.retries.backoff_multiplier),
        backoff_max=float(config.retries.backoff_max),
        statuses=tuple(config.retries.statuses),
        methods=frozenset(method.upper() for method in config.retries.allowed_methods),
        respect_retry_after=bool(config.retries.respect_retry_after),
        retry_on_connection_errors=bool(config.retries.retry_on_connection_errors),
        retry_on_read_errors=bool(config.retries.retry_on_read_errors),
        retry_on_other_errors=bool(config.retries.retry_on_other_errors),
    )
    headers = dict(config.headers)
    return ApiProfile(
        name=name,
        timeouts=timeouts,
        limits=limits,
        retries=retries,
        headers=headers,
        proxies=dict(config.proxies) if config.proxies else None,
        verify=config.verify,
        cert=config.cert,
        trust_env=config.trust_env,
        max_redirects=int(config.max_redirects),
    )

