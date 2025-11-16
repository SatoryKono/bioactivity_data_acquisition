"""Utilities for normalising callable and documented function signatures."""

from __future__ import annotations

import ast
import inspect
from typing import Any

__all__ = ["signature_from_callable", "signature_from_docs"]


def _format_annotation(annotation: Any, *, empty_value: str | None) -> str | None:
    """Convert an annotation object to a comparable string representation."""

    if (
        annotation is inspect.Parameter.empty
        or annotation is inspect.Signature.empty
        or annotation is None
    ):
        return empty_value

    if isinstance(annotation, str):
        return annotation

    try:
        return inspect.formatannotation(annotation, base_module=None)
    except Exception:  # pragma: no cover - defensive fallback
        return str(annotation)


def _format_default(default: Any) -> str | None:
    """Represent a default value consistently for comparisons."""

    if default is inspect.Parameter.empty:
        return None
    return str(default)


def signature_from_callable(
    callable_obj: Any,
    *,
    empty_annotation: str | None = None,
    include_abstract_flag: bool = False,
) -> dict[str, Any]:
    """Serialise a live callable into a comparable signature dictionary."""

    signature = inspect.signature(callable_obj)
    parameters: list[dict[str, Any]] = []
    for parameter in signature.parameters.values():
        parameters.append(
            {
                "name": parameter.name,
                "kind": str(parameter.kind),
                "annotation": _format_annotation(
                    parameter.annotation, empty_value=empty_annotation
                ),
                "default": _format_default(parameter.default),
            }
        )

    payload: dict[str, Any] = {
        "name": getattr(
            callable_obj,
            "__name__",
            getattr(callable_obj, "__qualname__", repr(callable_obj)),
        ),
        "parameters": parameters,
        "return_annotation": _format_annotation(
            signature.return_annotation, empty_value=empty_annotation
        ),
    }

    if include_abstract_flag:
        payload["is_abstract"] = bool(getattr(callable_obj, "__isabstractmethod__", False))

    return payload


def _annotation_from_ast(annotation: ast.AST | None, *, empty_value: str | None) -> str | None:
    if annotation is None:
        return empty_value
    return ast.unparse(annotation).strip()


def _default_from_ast(default: ast.AST | None) -> str | None:
    if default is None:
        return None
    return ast.unparse(default).strip()


def signature_from_docs(
    definition: str | ast.FunctionDef,
    *,
    empty_annotation: str | None = None,
) -> dict[str, Any]:
    """Parse a documented function definition into a comparable signature."""

    if isinstance(definition, ast.FunctionDef):
        function_node = definition
    else:
        text = definition.strip()
        if not text.startswith("def "):
            text = f"def {text}"
        if not text.endswith(":"):
            text = text + ":"
        module = ast.parse(f"{text}\n    ...")
        node = module.body[0]
        if not isinstance(node, ast.FunctionDef):  # pragma: no cover - defensive
            raise ValueError("Provided definition does not describe a function")
        function_node = node

    parameters: list[dict[str, Any]] = []
    positional_only = list(function_node.args.posonlyargs)
    positional_or_keyword = list(function_node.args.args)
    combined = positional_only + positional_or_keyword
    total_defaults = len(function_node.args.defaults)
    defaults_start = len(combined) - total_defaults

    for index, argument in enumerate(combined):
        default_node = None
        if total_defaults and index >= defaults_start:
            default_node = function_node.args.defaults[index - defaults_start]
        kind = "POSITIONAL_ONLY" if index < len(positional_only) else "POSITIONAL_OR_KEYWORD"
        parameters.append(
            {
                "name": argument.arg,
                "kind": kind,
                "annotation": _annotation_from_ast(
                    argument.annotation, empty_value=empty_annotation
                ),
                "default": _default_from_ast(default_node),
            }
        )

    if function_node.args.vararg is not None:
        parameters.append(
            {
                "name": function_node.args.vararg.arg,
                "kind": "VAR_POSITIONAL",
                "annotation": _annotation_from_ast(
                    function_node.args.vararg.annotation, empty_value=empty_annotation
                ),
                "default": None,
            }
        )

    for kw_arg, default in zip(function_node.args.kwonlyargs, function_node.args.kw_defaults):
        parameters.append(
            {
                "name": kw_arg.arg,
                "kind": "KEYWORD_ONLY",
                "annotation": _annotation_from_ast(
                    kw_arg.annotation, empty_value=empty_annotation
                ),
                "default": _default_from_ast(default),
            }
        )

    if function_node.args.kwarg is not None:
        parameters.append(
            {
                "name": function_node.args.kwarg.arg,
                "kind": "VAR_KEYWORD",
                "annotation": _annotation_from_ast(
                    function_node.args.kwarg.annotation, empty_value=empty_annotation
                ),
                "default": None,
            }
        )

    return {
        "name": function_node.name,
        "parameters": parameters,
        "return_annotation": _annotation_from_ast(
            function_node.returns, empty_value=empty_annotation
        ),
    }
