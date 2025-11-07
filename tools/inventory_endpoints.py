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
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence


ROOT = Path("src/bioetl")

HTTP_SOURCES = (
    "requests",
    "httpx",
    "aiohttp",
    "UnifiedAPIClient",
    "self._client",
    "client",
)
HTTP_CALL_PATTERN = re.compile(
    r"(?:(?:" + "|".join(re.escape(src) for src in HTTP_SOURCES) + \
    r")\.(?:get|post|request))\((?P<args>.+)",
    re.DOTALL,
)
ONLY_PATTERN = re.compile(r"(?:only|fields|select)\s*=\s*(?P<val>[^,&\\)]+)")
PAGINATION_PATTERN = re.compile(r"(limit|offset|page|cursor|per_page|size)")
RETRY_PATTERN = re.compile(r"retry|retri|backoff|tenacity", re.IGNORECASE)
DF_BIRTH_PATTERN = re.compile(
    r"pd\.DataFrame|spark\.createDataFrame|json_normalize|to_pandas"
)
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
    clients: dict[str, dict[str, Any]] = {}
    for record in records:
        if not record.file.startswith("src/bioetl/clients"):
            continue
        bucket = clients.setdefault(
            record.file,
            {
                "endpoints": set(),
                "has_only": False,
                "pagination": False,
                "retries": False,
                "df_entrypoint": False,
                "base_urls": set(),
            },
        )
        endpoints = bucket["endpoints"]
        assert isinstance(endpoints, set)
        endpoints.add(record.http_call.strip())
        for key in ("has_only", "pagination", "retries", "df_entrypoint"):
            bucket[key] = bool(bucket[key]) or getattr(record, key)
        if record.request_base_url:
            base_urls = bucket["base_urls"]
            assert isinstance(base_urls, set)
            base_urls.add(record.request_base_url)

    lines = [
        "# Инвентаризация HTTP-вызовов (клиенты)",
        "",
        "| Client module | Base URLs | has_only | pagination | retries | DF entrypoint |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for file, info in sorted(clients.items()):
        base_set = info["base_urls"]
        assert isinstance(base_set, set)
        base_urls = ", ".join(sorted(base_set)) if base_set else "—"
        lines.append(
            "| `{file}` | {base_urls} | {has_only} | {pagination} | {retries} | {df_entrypoint} |".format(
                file=file,
                base_urls=base_urls,
                has_only=bool(info["has_only"]),
                pagination=bool(info["pagination"]),
                retries=bool(info["retries"]),
                df_entrypoint=bool(info["df_entrypoint"]),
            )
        )
    lines.extend(["", "## Endpoint snippets", ""])
    for file, info in sorted(clients.items()):
        lines.append(f"### `{file}`")
        endpoints = info["endpoints"]
        assert isinstance(endpoints, set)
        for endpoint in sorted(endpoints):
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

