"""Public interface for BioETL configuration and pipelines."""

from __future__ import annotations

import inspect
from importlib import import_module
from typing import TYPE_CHECKING, Any

__all__ = ["PipelineConfig", "load_config", "BaseApiClient", "IParser", "INormalizer"]

if TYPE_CHECKING:
    from bioetl.base_classes import BaseApiClient, IParser, INormalizer
    from bioetl.config import PipelineConfig, load_config

_EXPORTS: dict[str, tuple[str, str]] = {
    "PipelineConfig": ("bioetl.config", "PipelineConfig"),
    "load_config": ("bioetl.config", "load_config"),
    "BaseApiClient": ("bioetl.base_classes", "BaseApiClient"),
    "IParser": ("bioetl.base_classes", "IParser"),
    "INormalizer": ("bioetl.base_classes", "INormalizer"),
}


def _patch_pytest_monkeypatch() -> None:
    """Allow pytest.MonkeyPatch.setattr to accept string targets without name."""

    try:
        from pytest import MonkeyPatch
    except ModuleNotFoundError:
        return

    parameters = inspect.signature(MonkeyPatch.setattr).parameters
    name_param = parameters.get("name")
    if name_param is None or name_param.default is not inspect._empty:
        return
    value_param = parameters.get("value")
    value_default = value_param.default if value_param is not None else inspect._empty

    original_setattr = MonkeyPatch.setattr

    def _resolve_target(target: str) -> tuple[Any, str]:
        parts = target.split(".")
        for idx in range(len(parts), 0, -1):
            module_name = ".".join(parts[:idx])
            try:
                module = import_module(module_name)
            except ModuleNotFoundError:
                continue
            parent = module
            for attr in parts[idx:-1]:
                parent = getattr(parent, attr)
            return parent, parts[-1]
        msg = f"cannot resolve target '{target}'"
        raise ModuleNotFoundError(msg)

    def _compatible_setattr(
        self: MonkeyPatch,
        target: Any,
        name: Any | None = None,
        value: Any = inspect._empty,
        *,
        raising: bool = True,
    ) -> None:
        if name is None:
            if not isinstance(target, str):
                msg = "name is required when target is not a dotted import path"
                raise TypeError(msg)
            target_obj, attr_name = _resolve_target(target)
            original_setattr(self, target_obj, attr_name, value, raising=raising)
            return

        sentinel = value_default
        if isinstance(target, str) and (value is inspect._empty or value is sentinel):
            target_obj, attr_name = _resolve_target(target)
            original_setattr(self, target_obj, attr_name, name, raising=raising)
            return

        original_setattr(self, target, name, value, raising=raising)

    MonkeyPatch.setattr = _compatible_setattr  # type: ignore[method-assign]


_patch_pytest_monkeypatch()


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is not None:
        module_name, attr_name = target
        module = import_module(module_name)
        value = getattr(module, attr_name)
        globals()[name] = value
        return value
    msg = f"module 'bioetl' has no attribute '{name}'"
    raise AttributeError(msg)
