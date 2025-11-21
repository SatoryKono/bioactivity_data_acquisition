#!/usr/bin/env python3
"""Example usage of OpenAI Codex integration.

This script demonstrates how to use the OpenAI API client
to generate code completions using Codex or GPT models.

Prerequisites:
1. Set OPENAI_API_KEY in your .env file
2. Install dependencies: pip install -r requirements-dev.txt

Run:
    python examples/openai_codex_example.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from bioetl.clients.client_openai import OpenAIClient
from bioetl.config.models.http import (
    CircuitBreakerConfig,
    HTTPClientConfig,
    RateLimitConfig,
    RetryConfig,
)


def create_openai_config() -> HTTPClientConfig:
    """Create HTTP configuration for OpenAI API.

    This uses conservative rate limits suitable for OpenAI's free tier.
    Adjust based on your actual tier.
    """
    return HTTPClientConfig(
        timeout_sec=120.0,
        connect_timeout_sec=15.0,
        read_timeout_sec=120.0,
        retries=RetryConfig(
            total=3,
            backoff_multiplier=2.0,
            backoff_max=30.0,
            statuses=(429, 500, 502, 503, 504),
        ),
        rate_limit=RateLimitConfig(
            max_calls=3,  # 3 requests per minute (free tier)
            period=60.0,
        ),
        rate_limit_jitter=True,
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=3,
            timeout=60.0,
            half_open_max_calls=1,
            ignore_status_codes=(400, 401, 403, 404),
        ),
        headers={
            "User-Agent": "BioETL/1.0 (OpenAI-Client-Example)",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )


def example_code_completion() -> None:
    """Example: Generate code using Codex or GPT-3.5-turbo-instruct."""
    print("=" * 70)
    print("Example 1: Code Completion with GPT-3.5-turbo-instruct")
    print("=" * 70)

    config = create_openai_config()
    client = OpenAIClient(config)

    prompt = """def calculate_ic50(concentrations, responses):
    \"\"\"Calculate IC50 from dose-response data.
    
    Parameters
    ----------
    concentrations : array-like
        Drug concentrations
    responses : array-like
        Measured responses (0-100%)
        
    Returns
    -------
    float
        IC50 value
    \"\"\"
"""

    try:
        response = client.create_completion(
            model="gpt-3.5-turbo-instruct",
            prompt=prompt,
            max_tokens=200,
            temperature=0.2,  # Low temperature for deterministic code
            stop=["\n\n", "def ", "class "],  # Stop at next function/class
        )

        completion = response["choices"][0]["text"]
        print(f"\nPrompt:\n{prompt}")
        print(f"\nGenerated code:\n{completion}")
        print(f"\nFinish reason: {response['choices'][0]['finish_reason']}")
        print(f"Tokens used: {response['usage']['total_tokens']}")

    except Exception as e:
        print(f"Error: {e}")


def example_chat_completion() -> None:
    """Example: Ask a bioinformatics question using ChatGPT."""
    print("\n" + "=" * 70)
    print("Example 2: Chat Completion with GPT-3.5-turbo")
    print("=" * 70)

    config = create_openai_config()
    client = OpenAIClient(config)

    try:
        response = client.create_chat_completion(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful bioinformatics assistant with expertise in drug discovery.",
                },
                {
                    "role": "user",
                    "content": "Explain the difference between IC50, EC50, and Ki in simple terms.",
                },
            ],
            max_tokens=300,
            temperature=0.7,
        )

        answer = response["choices"][0]["message"]["content"]
        print("\nQuestion: Explain the difference between IC50, EC50, and Ki")
        print(f"\nAnswer:\n{answer}")
        print(f"\nFinish reason: {response['choices'][0]['finish_reason']}")
        print(f"Tokens used: {response['usage']['total_tokens']}")

    except Exception as e:
        print(f"Error: {e}")


def example_list_models() -> None:
    """Example: List available OpenAI models."""
    print("\n" + "=" * 70)
    print("Example 3: List Available Models")
    print("=" * 70)

    config = create_openai_config()
    client = OpenAIClient(config)

    try:
        models = client.list_models()

        # Filter and sort models
        gpt_models = sorted(
            [m for m in models if "gpt" in m["id"].lower()],
            key=lambda x: x["id"],
        )

        print("\nAvailable GPT models:")
        for model in gpt_models[:10]:  # Show first 10
            print(f"  - {model['id']}")

        if len(gpt_models) > 10:
            print(f"  ... and {len(gpt_models) - 10} more")

        print(f"\nTotal models available: {len(models)}")

    except Exception as e:
        print(f"Error: {e}")


def main() -> None:
    """Run all examples."""
    # Check if API key is set
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY environment variable is not set!")
        print("\nPlease:")
        print("1. Get your API key from https://platform.openai.com/api-keys")
        print("2. Add it to your .env file:")
        print("   OPENAI_API_KEY=sk-proj-xxxxxxxxxxxxxxxxxxxxx")
        print("3. Run this script again")
        sys.exit(1)

    print("OpenAI Codex Integration Examples")
    print("==================================\n")
    print(f"API Key configured: {api_key[:10]}...{api_key[-4:]}")

    # Run examples
    example_code_completion()
    example_chat_completion()
    example_list_models()

    print("\n" + "=" * 70)
    print("All examples completed!")
    print("=" * 70)


if __name__ == "__main__":
    main()
