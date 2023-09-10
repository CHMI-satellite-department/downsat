from __future__ import annotations

from typing import Any, Callable
from functools import singledispatch, wraps

from typing_extensions import Concatenate

from downsat.etl import WeakIdKeyDictionary, types


METADATA_DUNDER = "attrs"
_global_metadata_dict: WeakIdKeyDictionary = WeakIdKeyDictionary()


@singledispatch
def get_local_metadata(obj: Any) -> dict[str, Any]:
    """Get local metadata dict or raise AttributeError.

    The dictionary can be directly modified to update the metadata.

    :param obj: Object whose metadata are to be retrieved.
    :raises AttributeError: Local metadata do not exist.
    """
    return getattr(obj, METADATA_DUNDER)


@singledispatch
def set_local_metadata(obj: Any, **kwargs: Any) -> None:
    """Default implementation of set_local_metadata that tries to modify the object.

    Metadata are stored in `obj.{METADATA_DUNDER}` property.

    :param obj: Object whose metadata are to be set.
    :param key: Metadata key.
    :param value: Metadata value.
    :raises TypeError: Local metadata dict cannot be created.
    """
    try:
        metadata_dict = getattr(obj, METADATA_DUNDER)
    except AttributeError:
        metadata_dict = {}
        try:
            setattr(obj, METADATA_DUNDER, metadata_dict)
        except AttributeError as e:
            raise TypeError(f"Cannot set attribute {METADATA_DUNDER} to object of type {type(obj)}") from e

    metadata_dict.update(kwargs)


@singledispatch
def clear_local_metadata(obj: Any) -> None:
    """Clear locally stored metadata.

    :param obj: Object whose metadata are to be set.
    """

    try:
        metadata = get_local_metadata(obj)
    except AttributeError:
        pass
    else:
        metadata.clear()


def getmeta(obj: Any) -> dict[str, Any]:
    """Get metadata values of given object.

    Merges metadata values stored locally in the object and those in the global
    metadata dict. Local objects have priority in case of conflict.

    `getmeta` returns copy of the metadat dictionary -> its updates are not reflected
    in metadata. Use `setmeta` to add or update metadata.

    :param obj: Object whose metadata is being queried.
    :returns: Metadata dictionary of the object.

    # TODO: merge implementation with getcontext
    """
    metadata = _global_metadata_dict.get(obj, {}).copy()
    try:
        local_metadata = get_local_metadata(obj)
    except AttributeError:
        pass
    else:
        metadata.update(local_metadata)

    return metadata


def setmeta(obj: Any, **kwargs: Any) -> None:
    """Set metadata on an object.

    The properties are stored either in global context dictionary or localy using singledispatch function
    that can be overloaded for specific data types but stores the metadata by default in a property with
    name given in `downsat.etl.metadata.METADATA_DUNDER`.

    `setmeta` adds updates existing metadata dictionary, i.e. does not delete keys not given in kwargs.

    :param obj: Object whose metadata should be set.
    :param **kwargs: Metadata
    :raises TypeError: Not possible to add context variable to this type of object.
    # TODO: merge implementation with setcontext?
    """
    try:
        # try to store localy
        set_local_metadata(obj, **kwargs)
    except TypeError:
        # not possible, store in the global context dict
        _global_metadata_dict.setdefault(obj, {})
        _global_metadata_dict[obj].update(kwargs)


def clearmeta(obj: Any) -> None:
    """Clear all metadat of an object."""

    # destroy global metadata
    try:
        del _global_metadata_dict[obj]
    except KeyError:
        pass

    # destroy local metadata
    clear_local_metadata(obj)


def keepmeta(
    f: Callable[Concatenate[types.InputType, types.Params], types.OutputType]
) -> Callable[Concatenate[types.InputType, types.Params], types.OutputType]:
    """Decorator that copies metadata from input to output of the decorated function.

    :param f: Function to be decorated. Must accept single argument and return single value.
    :returns: Decorated function that copies metadata from input to output.
    """

    @wraps(f)
    def wrapper(
        input: types.InputType, *args: types.Params.args, **kwargs: types.Params.kwargs
    ) -> types.OutputType:
        metadata = getmeta(input)
        output = f(input, *args, **kwargs)
        setmeta(output, **metadata)

        return output

    return wrapper
