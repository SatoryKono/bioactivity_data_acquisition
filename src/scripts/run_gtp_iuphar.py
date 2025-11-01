#!/usr/bin/env python3
"""CLI entrypoint for executing the Guide to Pharmacology pipeline."""

from scripts import create_pipeline_app  # noqa: E402

app = create_pipeline_app(
    "gtp_iuphar",
    "Run Guide to Pharmacology pipeline to extract and transform target data",
)


if __name__ == "__main__":
    app()
