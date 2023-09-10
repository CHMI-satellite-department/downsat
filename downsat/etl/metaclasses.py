"""Metaclass conflict resolution.

Source: https://code.activestate.com/recipes/204197-solving-the-metaclass-conflict/
"""
from __future__ import annotations

from typing import Any, Callable, Generator, Type
from collections.abc import Iterable
import inspect


memoized_metaclasses_map: dict[tuple[Type, ...], Type] = {}  # result cache


def _skip_redundant(iterable: Iterable, skipset: set | None = None) -> Generator[Any, None, None]:
    """Skip redundant items, i.e. repeated items and items in the original skipset.

    :param iterable: Iterable containing some items.
    :param skipset: Set with items to be skipped. If not None, the input set is modified
        as a side effect.
    :yields: Unique items from iterable except those from skipset.
    """
    if skipset is None:
        skipset = set()

    for item in iterable:
        if item not in skipset:
            skipset.add(item)
            yield item


def _remove_redundant(metaclasses: Iterable[Type]) -> tuple[Type, ...]:
    """Remove redundant metaclasses from the list.

    :param metaclasses: Iterable of metaclasses.
    :returns: Tuple of unique ancestors composed by traversing mro of all the metaclasses with `type` excluded.
    """
    skipset: set[Type] = set([type])
    for meta in metaclasses:  # determines the metaclasses to be skipped
        skipset.update(inspect.getmro(meta)[1:])

    return tuple(_skip_redundant(metaclasses, skipset))


def get_noconflict_metaclass(
    bases: tuple[Type, ...], left_metas: tuple[Type, ...], right_metas: tuple[Type, ...]
) -> Type:
    """Resolve MRO of multiple metaclasses.

    :param bases: Base classes of the class to be constructed.
    :param left_metas: Extra metaclasses to be added with higher priority than base classes.
    :param right_metas: Extra metaclasses to be added with lower priority than base classes.
    :returns: New metaclass with given metaclasses as ancestors.
    """
    # make tuple of needed metaclasses in specified priority order
    metas = left_metas + tuple(map(type, bases)) + right_metas
    needed_metas = _remove_redundant(metas)

    # return existing confict-solving meta, if any
    if needed_metas in memoized_metaclasses_map:
        return memoized_metaclasses_map[needed_metas]
    # nope: compute, memoize and return needed conflict-solving meta
    elif not needed_metas:  # wee, a trivial case, happy us
        meta: Type = type
    elif len(needed_metas) == 1:  # another trivial case
        meta = needed_metas[0]
    # check for recursion, can happen i.e. for Zope ExtensionClasses
    elif needed_metas == bases:
        raise TypeError("Incompatible root metatypes", needed_metas)
    else:  # gotta work ...
        metaname = "_" + "".join([m.__name__ for m in needed_metas])
        meta = classmaker()(metaname, needed_metas, {})
    memoized_metaclasses_map[needed_metas] = meta
    return meta


def classmaker(
    left_metas: tuple[Type, ...] = (), right_metas: tuple[Type, ...] = ()
) -> Callable[[str, tuple[Type, ...], dict[str, Any]], Type]:
    """Decorator that resolve MRO of metaclasses of class ancestors and may be used to add some extra metaclasses.

    :param left_metas: Extra metaclasses to be added with higher priority than class ancestors.
    :param right_metas: Extra metaclasses to be added with lower priority than class ancestors.
    """

    def make_class(name: str, bases: tuple[Type, ...], adict: dict[str, Any]) -> Type:
        metaclass = get_noconflict_metaclass(bases, left_metas, right_metas)
        return metaclass(name, bases, adict)

    return make_class


def inject_class_base(
    cls: Type,
    base: Type,
    update_dict: dict[str, Any] | None = None,
    left_metas: tuple[Type, ...] = (),
    right_metas: tuple[Type, ...] = (),
) -> Type:
    """Inject base class at the end of MRO.

    Note: The base class can have a different metaclass then cls. In that case
    the function tries to resolve possible problems with metaclass MRO.

    :param cls: Class to be modified.
    :param base: Base class to be injected.
    :param update_dict: Update cls dict by this dictionary.
    :param left_metas: Extra metaclasses to be added with higher priority than class ancestors.
    :param right_metas: Extra metaclasses to be added with lower priority than class ancestors.
    :returns: New class with the same name that has base as an ancestor.
    """

    # respect __slots__ and __weakref__ of original class
    cls_dict = {key: val for key, val in cls.__dict__.items() if key not in {"__dict__"}}
    cls_dict.update(update_dict or {})

    # inject `base` as a base class
    bases = cls.__bases__ + (base,)
    bases = tuple(filter(lambda x: x != object, bases))
    metaclass = get_noconflict_metaclass(bases, left_metas=left_metas, right_metas=right_metas)

    modified_cls = metaclass(cls.__name__, bases, cls_dict)

    # Handle slots if they exist in the original class

    return modified_cls


def update_class_dict(
    cls: Type,
    update_dict: dict[str, Any],
) -> Type:
    """Update class dict.

    :param cls: Class to be modified.
    :param update_dict: Update cls dict by this dictionary.
    """

    cls_dict = dict(cls.__dict__)
    cls_dict.update(update_dict)

    # build new class
    modified_cls = type(cls.__name__, cls.__bases__, cls_dict)

    return modified_cls
