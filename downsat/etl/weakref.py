from __future__ import annotations

import typing
from typing import Any, Generic, Type
from collections.abc import ValuesView
import pathlib
from weakref import finalize

from downsat.etl import types


class WeakIdKeyDictionary(Generic[types.KeyType, types.ValueType]):
    """Dictionary used to associate additional properties to arbitrary python objects.

    Uses weak references so that storing object properties in this dictionary
    does not prevent the object from being garbage collected. If that happens,
    the record in this dictionary is automatically deleted.

    The dictionary is indexed by objects themselves.

    Note: It is possible to attach values even to imutable constants such as
    1 or 'some_string'.

    Note: The class does not yet implement common dict methods such as keys, values,
    items, update etc.
    """

    def __init__(self) -> None:
        """Initialize dictionary."""
        self.data: dict[int, Any] = dict()

    def _weak_delitem(self, key_id: int) -> None:
        """Delete item, do not raise KeyError if the key does not exist.

        :param key: Object whose record should be deleted.
        """
        try:
            self.data.__delitem__(key_id)
        except KeyError:
            pass

    def __setitem__(self, key: types.KeyType, value: types.ValueType) -> None:
        """Setitem that uses object id as a key.

        :param obj: Object whose id to use as a key.
        :param value: Value to store.
        """
        obj_id = id(key)
        finalize_fun = None
        try:
            if obj_id not in self.data:
                finalize_fun = finalize(key, self._weak_delitem, obj_id)
            self.data.__setitem__(obj_id, value)
        except Exception:
            # something broke, deactivate finalize_fun and remove the item
            if finalize_fun:
                finalize_fun()
            raise

    def __getitem__(self, key: types.KeyType) -> types.ValueType:
        """Getitem that queries by object id.

        :param key: Object to be queried.
            Any integer is interpreted as object id.
        :returns: Value stored for that object.
        :raises KeyError: Object id not found.
        """
        obj_id = id(key)
        try:
            return self.data.__getitem__(obj_id)
        except KeyError as e:
            raise KeyError(f"{key}") from e

    def __delitem__(self, key: types.KeyType) -> None:
        obj_id = id(key)
        self.data.__delitem__(obj_id)

    def __contains__(self, key: Type) -> bool:
        obj_id = id(key)
        return self.data.__contains__(obj_id)

    def __len__(self) -> int:
        return self.data.__len__()

    def values(self) -> ValuesView:
        """Return stored values."""
        return self.data.values()

    def setdefault(self, key: types.KeyType, default: types.ValueType) -> types.ValueType:
        """Insert key with a value of default if key is not in the dictionary.

        :param key: Key.
        :param default: Default value.
        :return: Value for key if key is in the dictionary, else default.
        """
        obj_id = id(key)
        if obj_id in self.data:
            return self[key]
        else:
            self[key] = default
            return default

    def get(self, key: types.KeyType, default: types.ValueType | None = None) -> types.ValueType:
        """Get value from dict or default value.

        :param key: Key.
        :param default: Default value.
        :returns: self[key] if key in self else default
        """
        obj_id = id(key)
        return self.data.get(obj_id, default)


class Path(pathlib.Path):
    """Path with weakref."""  # TODO: copy the Path docstring

    _flavour = type(pathlib.Path())._flavour  # type: ignore


class List(typing.List[types.ItemType], Generic[types.ItemType]):
    """List with weakref."""  # TODO: copy list docstring


class Dict(typing.Dict[types.KeyType, types.ValueType], Generic[types.KeyType, types.ValueType]):
    """Dict with weakref."""  # TODO: copy dict docstring


class MetaStr(str):
    """Str with weakref."""  # TODO: copy dict docstring
