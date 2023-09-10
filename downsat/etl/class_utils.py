from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable
from collections.abc import Iterable
from functools import partial
import inspect
from types import MappingProxyType

import attrs
from attrs import field


if TYPE_CHECKING:
    from attrs._make import _CountingAttr


def _callable_signature_to_fields(obj: Callable) -> dict[str, "_CountingAttr"]:
    """Return list of attrs fields corresponding to function arguments.

    :param func: Function to be analyzed. Cannot contain variadic args or kwargs
        unless the signature is func(*args, **kwargs) in which case they are ommited.
    :returns: List of function arguments described by attrs fields.
    :raises ValueError: Function accepts positional-only arguments.
    :raises NotImplementedError: Function accepts **kwargs.
    """
    # inspect __init__ signature
    params: dict[int, inspect.Parameter] | MappingProxyType[str, inspect.Parameter]
    if hasattr(obj, "__init__"):
        sig = inspect.signature(obj.__init__)  # type: ignore
        params = {i: param for i, param in enumerate(sig.parameters.values()) if i > 0}  # remove self
    else:
        sig = inspect.signature(obj)
        params = sig.parameters

    has_args = any(param.kind == param.VAR_POSITIONAL for param in params.values())
    has_kwargs = any(param.kind == param.VAR_KEYWORD for param in params.values())
    if has_args and has_kwargs and len(params) == 2:
        # func(*args, **kwargs)
        params = {}  # type: ignore  # changing type of params
    elif has_args:
        raise ValueError("Function contains positional-only arguments")
    elif has_kwargs:
        raise NotImplementedError("Function contains **kwargs")

    # find attrs fields
    try:
        attrs_fields = {f.name: f for f in attrs.fields(obj) if not f.name.startswith("_")}  # type: ignore  # obj may not be type
    except (TypeError, attrs.exceptions.NotAnAttrsClassError):
        attrs_fields = {}

    # find defaults
    defaults = {p.name: p.default if p.default != inspect._empty else attrs.NOTHING for p in params.values()}
    defaults.update(
        {k: v.default for k, v in attrs_fields.items()}
    )  # attrs fields take precedence, default may be factory

    # build fields
    fields = {param_name: field(default=default, kw_only=True) for param_name, default in defaults.items()}

    return fields


def _check_fields_compatibility(
    fields1: dict[str, "_CountingAttr"], fields2: dict[str, "_CountingAttr"]
) -> None:
    """Check if input/init parameters of two functions/classes are compatible.

    At this moment does not check anything

    :param fields1: Input/init parameters of the first function/class in the form of attrs fields.
    :param fields2: Input/init parameters of the second function/class in the form of attrs fields.
    :raises TypeError: Parameters are not compatible.

    # TODO: what if type annotation differs?
    """
    ...


def _extract_fields(obj: Any, forbidden_names: Iterable[str] | None = None) -> dict[str, "_CountingAttr"]:
    """Find attrs fields corresponding to input parameters of a function/class.

    :param obj: Function or class to be analyzed.
    :param forbidden_names: Names that cannot be used as input parameters.
    :returns: List of function arguments described by attrs fields.
    :raises ValueError: Function arguments collide with forbidden_names.
    """
    forbidden_names = forbidden_names or []

    if isinstance(obj, partial):
        if obj.args:
            raise ValueError("Partial function with args cannot be used")
        fields = _extract_fields(obj.func, forbidden_names=forbidden_names)
        for kw in obj.keywords:
            del fields[kw]
    elif isinstance(obj, type):
        # dataset class => copy input parameters
        fields = _callable_signature_to_fields(obj)

        colliding_names = set(fields) & set(forbidden_names)
        if colliding_names:
            raise ValueError(
                "Dataset arguments {colliding_names} collide with internal names. Please use different ones."
            )
    else:
        fields = {}

    return fields


def _build_constant_property(obj: Any) -> property:
    """This function allows creating properties inside a loop such as

    for obj in objs:
        properties.append(_build_constant_property(obj))

    Using this funciton solves the problem that if we used

    for obj in objs:
        properties.append(lambda self: obj)

    all the properties would return the last element of objs. Calling one _build_constant_property
    on the other hand creates a closure that remembers the correct value of obj.
    """

    return property(lambda self: obj)
