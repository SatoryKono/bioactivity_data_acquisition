FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC \
    LC_ALL=C \
    LANG=C

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       git \
       curl \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN groupadd -r bioetl && useradd -r -g bioetl bioetl

COPY pyproject.toml README.md ./
COPY src ./src
COPY configs ./configs
COPY scripts ./scripts

RUN pip install --upgrade pip \
    && pip install -e ".[dev]"

RUN chown -R bioetl:bioetl /app

USER bioetl

ENV PYTHONPATH=/app/src

CMD ["bash"]
