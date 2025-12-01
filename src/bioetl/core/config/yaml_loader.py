from __future__ import annotations

from collections.abc import MutableMapping, Sequence
from pathlib import Path
from typing import Any, cast

import yaml
from yaml.nodes import ScalarNode

from .file_resolver import _resolve_reference
from .merge_utils import _apply_yaml_merge, _deep_merge


def _load_yaml(path: Path) -> Any:
    """Load a YAML file with support for ``!include`` and merge keys.

    The loader eagerly resolves ``!include`` statements relative to ``path``
    before applying the standard YAML merge operator (``<<``).
    """

    class Loader(yaml.SafeLoader):
        pass

    def construct_include(loader: Loader, node: ScalarNode) -> Any:
        filename = loader.construct_scalar(node)
        include_path = _resolve_reference(Path(filename), base=path.parent)
        return _load_yaml(include_path)

    Loader.add_constructor("!include", construct_include)

    with path.open("r", encoding="utf-8") as handle:
        try:
            raw = handle.read()
        except OSError as exc:
            raise FileNotFoundError(path) from exc

    try:
        loaded = yaml.load(raw, Loader=Loader)
    except yaml.YAMLError:
        raise

    if loaded is None:
        return {}

    return _apply_yaml_merge(loaded)


def _ensure_mapping(data: Any, path: Path) -> dict[str, Any]:
    if isinstance(data, MutableMapping):
        return dict(data)
    msg = f"Configuration file must contain a mapping at top-level: {path}"
    raise TypeError(msg)


def _load_with_extends(path: Path, *, stack: tuple[Path, ...]) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if resolved in stack:
        chain = " -> ".join(str(p) for p in (*stack, resolved))
        raise ValueError(f"Circular extends detected: {chain}")

    data = _ensure_mapping(_load_yaml(resolved), resolved)
    extends = data.pop("extends", None)
    parents: list[Path] = []
    if isinstance(extends, (str, Path)):
        parents = [Path(extends)]
    elif isinstance(extends, Sequence):
        parents = [Path(p) for p in cast(Sequence[Any], extends)]

    merged: dict[str, Any] = {}
    for parent in parents:
        parent_path = _resolve_reference(parent, base=resolved.parent)
        parent_payload = _load_with_extends(parent_path, stack=(*stack, resolved))
        merged = _deep_merge(merged, parent_payload)

    return _deep_merge(merged, data)


__all__ = ["_apply_yaml_merge", "_ensure_mapping", "_load_with_extends", "_load_yaml"]
