"""Compatibility package that exposes modules from ``src/bioetl``."""

from __future__ import annotations

from importlib import util as importlib_util
from pathlib import Path
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

_SRC_PACKAGE = Path(__file__).resolve().parent.parent / "src" / "bioetl"
if _SRC_PACKAGE.exists():
    package_path = str(_SRC_PACKAGE)
    if package_path not in __path__:
        __path__.append(package_path)

    src_spec = importlib_util.spec_from_file_location("_bioetl_src_init", _SRC_PACKAGE / "__init__.py")
    if src_spec and src_spec.loader:
        src_module = importlib_util.module_from_spec(src_spec)
        src_spec.loader.exec_module(src_module)
        __version__ = getattr(src_module, "__version__", "0.0.0")
        for attr_name in getattr(src_module, "__all__", []):
            globals()[attr_name] = getattr(src_module, attr_name)
else:  # pragma: no cover - defensive branch for incomplete environments
    raise ImportError("src/bioetl package is missing; install project dependencies first")

__all__ = list(globals().get("__all__", []))
