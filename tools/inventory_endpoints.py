"""Сканирование исходников для инвентаризации HTTP-вызовов.

Скрипт пробегает по `src/bioetl`, извлекает сведения о вызовах HTTP-
клиентов и формировании датафреймов и отдаёт JSON, пригодный для
дальнейшей отчётности.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path

ROOT = Path("src/bioetl")

HTTP_SOURCES = (
    "requests",
    "httpx",
    "aiohttp",
    "UnifiedAPIClient",
    "self._client",
    "self._chembl_client",
    "chembl_client",
    "client",
)
HTTP_METHODS = (
    "get",
    "post",
    "request",
    "request_json",
    "paginate",
    "handshake",
)
HTTP_CALL_PATTERN = re.compile(
    (
        r"(?:(?:"
        + "|".join(re.escape(src) for src in HTTP_SOURCES)
        + r")\.(?:"
        + "|".join(HTTP_METHODS)
        + r"))\((?P<args>.+)"
    ),
    re.DOTALL,
)
ONLY_PATTERN = re.compile(r"(?:only|fields|select)\s*=\s*(?P<val>[^,&\\)]+)")
PAGINATION_PATTERN = re.compile(r"(limit|offset|page|cursor|per_page|size)")
RETRY_PATTERN = re.compile(r"retry|retri|backoff|tenacity", re.IGNORECASE)
DF_BIRTH_PATTERN = re.compile(r"pd\.DataFrame|spark\.createDataFrame|json_normalize|to_pandas")
BASE_URL_PATTERN = re.compile(r"url\s*=\s*['\"](?P<url>https?://[^'\"]+)")


@dataclass(slots=True)
class EndpointInventoryRecord:
    """Описание HTTP-вызова, найденного в исходнике."""

    file: str
    http_call: str
    request_base_url: str | None
    has_only: bool
    pagination: bool
    retries: bool
    df_entrypoint: bool


@dataclass(slots=True)
class _ClientAccumulator:
    endpoints: set[str] = field(default_factory=set)
    base_urls: set[str] = field(default_factory=set)
    has_only: bool = False
    pagination: bool = False
    retries: bool = False
    df_entrypoint: bool = False


def _load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def _iter_http_matches(path: Path, text: str) -> Iterator[EndpointInventoryRecord]:
    retries_detected = bool(RETRY_PATTERN.search(text))
    df_birth_detected = bool(DF_BIRTH_PATTERN.search(text))
    base_match = BASE_URL_PATTERN.search(text)
    base_url = base_match.group("url") if base_match else None

    for match in HTTP_CALL_PATTERN.finditer(text):
        args = match.group("args")
        has_only = bool(ONLY_PATTERN.search(args))
        has_pagination = bool(PAGINATION_PATTERN.search(args))
        snippet = match.group(0).splitlines()[0][:200]

        yield EndpointInventoryRecord(
            file=str(path).replace("\\", "/"),
            http_call=snippet,
            request_base_url=base_url,
            has_only=has_only,
            pagination=has_pagination,
            retries=retries_detected,
            df_entrypoint=df_birth_detected,
        )


def _scan_paths(paths: Iterable[Path]) -> Iterator[EndpointInventoryRecord]:
    for path in paths:
        text = _load_text(path)
        yield from _iter_http_matches(path, text)


def collect_inventory(root: Path = ROOT) -> list[EndpointInventoryRecord]:
    python_files = sorted(root.rglob("*.py"))
    return list(_scan_paths(python_files))


def _write_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
        tmp.write(content)
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)


def _render_markdown(records: Sequence[EndpointInventoryRecord]) -> str:
    clients: dict[str, _ClientAccumulator] = {}
    for record in records:
        if not record.file.startswith("src/bioetl/clients"):
            continue
        bucket = clients.setdefault(record.file, _ClientAccumulator())
        bucket.endpoints.add(record.http_call.strip())
        bucket.has_only = bucket.has_only or record.has_only
        bucket.pagination = bucket.pagination or record.pagination
        bucket.retries = bucket.retries or record.retries
        bucket.df_entrypoint = bucket.df_entrypoint or record.df_entrypoint
        if record.request_base_url:
            bucket.base_urls.add(record.request_base_url)

    lines = [
        "# Инвентаризация HTTP-вызовов (клиенты)",
        "",
        "| Client module | Base URLs | has_only | pagination | retries | DF entrypoint |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for file, info in sorted(clients.items()):
        base_urls = ", ".join(sorted(info.base_urls)) if info.base_urls else "—"
        lines.append(
            f"| `{file}` | {base_urls} | {info.has_only} | {info.pagination} | {info.retries} | {info.df_entrypoint} |"
        )
    lines.extend(["", "## Endpoint snippets", ""])
    for file, info in sorted(clients.items()):
        lines.append(f"### `{file}`")
        for endpoint in sorted(info.endpoints):
            lines.append(f"- `{endpoint}`")
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Инвентаризация HTTP-вызовов.")
    parser.add_argument(
        "--json-output",
        type=Path,
        help="Путь до JSON-файла. По умолчанию печатается в stdout.",
    )
    parser.add_argument(
        "--markdown-output",
        type=Path,
        help="Дополнительно сохранить Markdown-таблицу по клиентам.",
    )
    args = parser.parse_args()

    records = collect_inventory()
    payload = [asdict(record) for record in records]

    if args.json_output:
        json_text = json.dumps(payload, ensure_ascii=False, indent=2)
        _write_atomic(args.json_output, json_text)
    else:
        json.dump(payload, ensure_ascii=False, indent=2, fp=sys.stdout)

    if args.markdown_output:
        markdown = _render_markdown(records)
        _write_atomic(args.markdown_output, markdown)


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    main()
