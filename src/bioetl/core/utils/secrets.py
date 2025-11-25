from __future__ import annotations

import os

from dotenv import load_dotenv

from .interfaces import SecretProviderABC


class DotenvSecretProvider(SecretProviderABC):
    """Возвращает секреты из ``.env`` или окружения."""

    def __init__(self, env_file: str | None = ".env", prefix: str | None = None) -> None:
        self._prefix = (prefix or "").upper()
        if env_file:
            load_dotenv(env_file, override=False)

    def get(self, name: str) -> str:
        key = f"{self._prefix}{name}" if self._prefix else name
        value = os.getenv(key)
        if value is None:
            raise KeyError(f"Secret '{key}' not found in environment")
        return value

