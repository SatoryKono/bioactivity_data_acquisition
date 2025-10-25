"""Runtime configuration and session management."""

from __future__ import annotations

import ssl
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .models import ApiCfg, RetryCfg


def session_with_retry(
    api_cfg: ApiCfg,
    retry_cfg: RetryCfg | None = None,
    **kwargs: Any,
) -> requests.Session:
    """Create a configured requests session with retry logic.

    Parameters
    ----------
    api_cfg:
        API configuration settings.
    retry_cfg:
        Optional retry configuration. If not provided, uses defaults.
    **kwargs:
        Additional arguments passed to requests.Session.

    Returns
    -------
    requests.Session
        Configured session with retry adapter and SSL settings.
    """
    if retry_cfg is None:
        retry_cfg = RetryCfg()

    # Create session
    session = requests.Session(**kwargs)

    # Configure retry strategy
    retry_strategy = Retry(
        total=retry_cfg.retries,
        backoff_factor=retry_cfg.backoff_multiplier,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
        raise_on_status=False,
    )

    # Create HTTP adapter with retry strategy
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=20,
        pool_block=False,
    )

    # Mount adapter for both HTTP and HTTPS
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Configure SSL verification
    if isinstance(api_cfg.verify, bool):
        session.verify = api_cfg.verify
    elif isinstance(api_cfg.verify, str):
        verify_path = Path(api_cfg.verify)
        if verify_path.exists():
            session.verify = str(verify_path)
        else:
            # If path doesn't exist, treat as boolean
            session.verify = api_cfg.verify.lower() in {"true", "1", "yes", "on"}
    else:
        session.verify = True

    # Set default headers
    session.headers.update(
        {
            "User-Agent": api_cfg.user_agent,
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
        }
    )

    # Configure SSL context for better security
    if session.verify:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = True
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        # Note: SSL context configuration is handled by requests internally
        # when verify=True is set

    return session


def create_default_session() -> requests.Session:
    """Create a default session with standard configuration."""
    api_cfg = ApiCfg()
    return session_with_retry(api_cfg)
