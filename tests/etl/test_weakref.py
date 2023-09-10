from typing import Any, Type

import pytest
from pytest_cases import parametrize_with_cases


def class_common() -> Type:
    class TestClass:
        ...

    return TestClass


def class_attrs_hashable() -> Type:
    from attrs import define

    @define(hash=True)
    class TestClassAttrs:
        ...

    return TestClassAttrs


def class_attrs_unhashable() -> Type:
    from attrs import define

    @define
    class TestClassAttrs:
        ...

    return TestClassAttrs


def class_attrs_frozen() -> Type:
    from attrs import frozen

    @frozen(slots=False)
    class TestClassAttrsFrozen:
        ...

    return TestClassAttrsFrozen


@parametrize_with_cases("test_class", cases=".", prefix="class")
def test_weak_id_key_dictionary(test_class: Type) -> None:
    import gc

    from downsat.etl.context import WeakIdKeyDictionary

    instance1 = test_class()
    instance2 = test_class()
    instance3 = test_class()

    weak_dict: WeakIdKeyDictionary = WeakIdKeyDictionary()

    weak_dict[instance1] = 1
    weak_dict[instance2] = 2

    # the dictionary distinguishes between instances of the same type
    assert len(weak_dict) == 2
    assert weak_dict[instance1] == 1
    assert weak_dict[instance2] == 2

    # .values works
    assert set(weak_dict.values()) == {1, 2}

    # can delete key
    del weak_dict[instance2]
    assert len(weak_dict) == 1
    assert instance1 in weak_dict
    assert instance2 not in weak_dict

    # when an instance is deleted, it is removed from the dict
    del instance1
    gc.collect()
    assert len(weak_dict) == 0
    assert instance2 not in weak_dict

    # setdefault works
    weak_dict.setdefault(instance2, 5)
    assert weak_dict[instance2] == 5

    weak_dict[instance2] = 6
    weak_dict.setdefault(instance2, 5)
    assert weak_dict[instance2] == 6

    # get works
    assert weak_dict.get(instance3) is None
    assert weak_dict.get(instance3, -1) == -1
    assert weak_dict.get(instance2) == weak_dict[instance2]


def test_weakref_path() -> None:
    from downsat.etl.weakref import Path, WeakIdKeyDictionary

    # TODO: why WeakIdKeyDictionary[Any, Any] has to be specified?
    weak_dict: WeakIdKeyDictionary[Any, Any] = WeakIdKeyDictionary()
    p = Path("/mnt")

    # weak Path works
    assert p.relative_to("/") == Path("mnt")

    # weak Path has weakref
    weak_dict[p] = 3
    assert len(weak_dict) == 1

    del p
    assert len(weak_dict) == 0


def test_weakref_custom_containers() -> None:
    # TODO: parametrize
    from downsat.etl.weakref import List, MetaStr, WeakIdKeyDictionary

    # TODO: why WeakIdKeyDictionary[Any, Any] has to be specified?
    weak_dict: WeakIdKeyDictionary[Any, Any] = WeakIdKeyDictionary()

    # List
    list_ = List([1, 2, 3])

    weak_dict[list_] = 1
    assert len(weak_dict) == 1

    del list_
    assert len(weak_dict) == 0

    # Str
    s = MetaStr("Test string")
    weak_dict[s] = 5

    assert weak_dict[s] == 5
    assert len(weak_dict) == 1

    del s
    assert len(weak_dict) == 0


@pytest.mark.parametrize("invalid_obj", [[1], (1,)], ids=["list", "tuple"])
def test_invalid_weakref_objects(invalid_obj: Any) -> None:
    """Object that cannot have weak references must raise upon assignment."""
    from downsat.etl.weakref import WeakIdKeyDictionary

    # TODO: why WeakIdKeyDictionary[Any, Any] has to be specified?
    weak_dict: WeakIdKeyDictionary[Any, Any] = WeakIdKeyDictionary()
    with pytest.raises(TypeError):
        weak_dict[invalid_obj] = 3

    with pytest.raises(TypeError):
        weak_dict.setdefault(invalid_obj, 3)
