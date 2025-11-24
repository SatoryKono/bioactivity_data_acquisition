"""OpenAI API client with Codex support."""

from __future__ import annotations

import os
from typing import Any

from infrastructure.config.models.http import HTTPClientConfig
from infrastructure.http import UnifiedAPIClient
from infrastructure.logging import LogEvents, UnifiedLogger

__all__ = ["OpenAIClient"]


class OpenAIClient(UnifiedAPIClient):
    """Client for OpenAI API (Codex, GPT models, etc.).

    The client automatically handles:
    - Rate limiting and retries
    - Circuit breaker protection
    - Structured logging
    - Bearer token authentication

    Examples
    --------
    >>> import os
    >>> from infrastructure.config.models.http import HTTPClientConfig, RateLimitConfig, RetryConfig
    >>> api_key = os.environ["OPENAI_API_KEY"]
    >>> config = HTTPClientConfig(
    ...     rate_limit=RateLimitConfig(max_calls=3, period=60.0),
    ...     retries=RetryConfig(total=3),
    ... )
    >>> client = OpenAIClient(config, api_key=api_key)
    >>> response = client.create_completion(
    ...     model="code-davinci-002",
    ...     prompt="def factorial(n):",
    ...     max_tokens=100,
    ... )
    >>> print(response["choices"][0]["text"])
    """

    BASE_URL = "https://api.openai.com/v1"

    def __init__(
        self,
        config: HTTPClientConfig,
        *,
        api_key: str | None = None,
        name: str = "openai",
    ) -> None:
        """Initialize OpenAI client.

        Parameters
        ----------
        config:
            HTTP client configuration (timeouts, retries, rate limits, etc.).
        api_key:
            OpenAI API key. If not provided, will attempt to read from OPENAI_API_KEY env var.
        name:
            Client name for logging and metrics.

        Raises
        ------
        ValueError:
            If API key is not provided and OPENAI_API_KEY env var is not set.
        """
        self._api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self._api_key:
            msg = (
                "OpenAI API key is required. "
                "Provide it via the `api_key` parameter or set the OPENAI_API_KEY environment variable."
            )
            raise ValueError(msg)

        # Initialize base client with OpenAI base URL
        super().__init__(config, base_url=self.BASE_URL, name=name)

        # Add Authorization header with Bearer token
        self._session.headers["Authorization"] = f"Bearer {self._api_key}"

        self._logger = UnifiedLogger.get(__name__).bind(
            component="openai_client",
            client_name=name,
        )
        self._logger.info(
            LogEvents.CHEMBL_CLIENT_INITIALIZED,
            base_url=self.BASE_URL,
            name=name,
        )

    def create_completion(
        self,
        *,
        model: str,
        prompt: str | list[str],
        max_tokens: int | None = None,
        temperature: float = 0.7,
        top_p: float = 1.0,
        n: int = 1,
        stream: bool = False,
        stop: str | list[str] | None = None,
        presence_penalty: float = 0.0,
        frequency_penalty: float = 0.0,
        logit_bias: dict[str, float] | None = None,
        user: str | None = None,
    ) -> Any:
        """Create a text completion using OpenAI API.

        Parameters
        ----------
        model:
            Model to use (e.g., "code-davinci-002", "gpt-3.5-turbo-instruct").
        prompt:
            Prompt text or list of prompts.
        max_tokens:
            Maximum number of tokens to generate. Defaults to model's max context length - prompt tokens.
        temperature:
            Sampling temperature (0.0 to 2.0). Higher = more random.
        top_p:
            Nucleus sampling probability threshold.
        n:
            Number of completions to generate per prompt.
        stream:
            Whether to stream partial results. Not currently supported (returns full response).
        stop:
            Sequence(s) where the API will stop generating further tokens.
        presence_penalty:
            Penalty for new tokens based on whether they appear in the text so far.
        frequency_penalty:
            Penalty for new tokens based on their existing frequency in the text.
        logit_bias:
            Modify the likelihood of specified tokens appearing.
        user:
            Unique identifier for the end-user, for abuse monitoring.

        Returns
        -------
        dict:
            Completion response from OpenAI API.

        Raises
        ------
        requests.HTTPError:
            If the API request fails.
        """
        payload: dict[str, Any] = {
            "model": model,
            "prompt": prompt,
            "temperature": temperature,
            "top_p": top_p,
            "n": n,
            "stream": stream,
            "presence_penalty": presence_penalty,
            "frequency_penalty": frequency_penalty,
        }

        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if stop is not None:
            payload["stop"] = stop
        if logit_bias is not None:
            payload["logit_bias"] = logit_bias
        if user is not None:
            payload["user"] = user

        self._logger.info(
            LogEvents.HTTP_REQUEST_COMPLETED,
            endpoint="/completions",
            model=model,
            prompt_length=(
                len(prompt)
                if isinstance(prompt, str)
                else sum(len(p) for p in prompt)
            ),
        )

        return self.request_json("POST", "/completions", json=payload)

    def create_chat_completion(
        self,
        *,
        model: str,
        messages: list[dict[str, str]],
        max_tokens: int | None = None,
        temperature: float = 0.7,
        top_p: float = 1.0,
        n: int = 1,
        stream: bool = False,
        stop: str | list[str] | None = None,
        presence_penalty: float = 0.0,
        frequency_penalty: float = 0.0,
        logit_bias: dict[str, float] | None = None,
        user: str | None = None,
    ) -> Any:
        """Create a chat completion using OpenAI API.

        Parameters
        ----------
        model:
            Model to use (e.g., "gpt-3.5-turbo", "gpt-4").
        messages:
            List of message dicts with "role" and "content" keys.
            Example: [{"role": "user", "content": "Hello!"}]
        max_tokens:
            Maximum number of tokens to generate.
        temperature:
            Sampling temperature (0.0 to 2.0).
        top_p:
            Nucleus sampling probability threshold.
        n:
            Number of completions to generate.
        stream:
            Whether to stream partial results. Not currently supported.
        stop:
            Sequence(s) where the API will stop generating.
        presence_penalty:
            Penalty for new tokens based on presence.
        frequency_penalty:
            Penalty for new tokens based on frequency.
        logit_bias:
            Modify token likelihoods.
        user:
            Unique identifier for end-user.

        Returns
        -------
        dict:
            Chat completion response from OpenAI API.

        Raises
        ------
        requests.HTTPError:
            If the API request fails.
        """
        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "top_p": top_p,
            "n": n,
            "stream": stream,
            "presence_penalty": presence_penalty,
            "frequency_penalty": frequency_penalty,
        }

        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if stop is not None:
            payload["stop"] = stop
        if logit_bias is not None:
            payload["logit_bias"] = logit_bias
        if user is not None:
            payload["user"] = user

        self._logger.info(
            LogEvents.HTTP_REQUEST_COMPLETED,
            endpoint="/chat/completions",
            model=model,
            message_count=len(messages),
        )

        return self.request_json("POST", "/chat/completions", json=payload)

    def list_models(self) -> list[dict[str, Any]]:
        """List available models from OpenAI API.

        Returns
        -------
        list[dict]:
            List of model objects with id, created timestamp, etc.
        """
        response = self.request_json("GET", "/models")
        return response.get("data", [])

    def retrieve_model(self, model_id: str) -> dict[str, Any]:
        """Retrieve information about a specific model.

        Parameters
        ----------
        model_id:
            Model identifier (e.g., "gpt-3.5-turbo").

        Returns
        -------
        dict:
            Model object with id, created timestamp, etc.
        """
        return self.request_json("GET", f"/models/{model_id}")
