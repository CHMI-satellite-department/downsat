from typing import Any, Type

from pytest_cases import parametrize_with_cases


try:
    import xarray as xr
except ImportError:
    xarray_exists = False
else:
    xarray_exists = True


def class_common() -> Type[Any]:
    class TestClass:
        ...

    return TestClass


def class_attrs_hashable() -> Type[Any]:
    from attrs import define

    @define(hash=True)
    class TestClassAttrs:
        ...

    return TestClassAttrs


def class_attrs_unhashable() -> Type[Any]:
    from attrs import define

    @define
    class TestClassAttrs:
        ...

    return TestClassAttrs


def class_attrs_frozen() -> Type[Any]:
    from attrs import frozen

    @frozen(slots=False)
    class TestClassAttrsFrozen:
        ...

    return TestClassAttrsFrozen


if xarray_exists:

    def class_metadata_attrs_xr_dataarray() -> Type[xr.DataArray]:
        return xr.DataArray

    def class_metadata_attrs_xr_dataset() -> Type[xr.Dataset]:
        return xr.Dataset


@parametrize_with_cases("data_class", cases=".", prefix="class")
def test_setmetadata(data_class: Type) -> None:
    import datetime

    from downsat.etl.metadata import getmeta, setmeta

    data = data_class()
    meta_dict = {"a": 5, "b": datetime.datetime.now()}

    # context can be set and recovered
    setmeta(data, **meta_dict)
    assert getmeta(data) == meta_dict


@parametrize_with_cases("data_class", cases=".", prefix="class")
def test_clearmeta(data_class: Type) -> None:
    import datetime

    from downsat.etl.metadata import clearmeta, getmeta, setmeta

    data = data_class()
    meta_dict = {"a": 5, "b": datetime.datetime.now()}

    setmeta(data, **meta_dict)
    assert getmeta(data) == meta_dict

    # metadata can be cleared
    clearmeta(data)
    assert getmeta(data) == {}


@parametrize_with_cases("attrs_data_class", cases=".", prefix="class_metadata_attrs")
def test_obj_with_attrs(attrs_data_class: Type) -> None:
    import datetime

    from downsat.etl.metadata import getmeta, setmeta

    data = attrs_data_class()
    meta_dict = {"a": 5, "b": datetime.datetime.now()}

    # context is set and recovered from attrs
    setmeta(data, **meta_dict)
    assert data.attrs == meta_dict
    assert getmeta(data) == meta_dict

    # manual change is reflected
    data.attrs["a"] = 6
    assert getmeta(data)["a"] == 6

    # pre-existing metadata survive
    data = attrs_data_class()
    data.attrs["c"] = -1
    setmeta(data, **meta_dict)
    assert getmeta(data)["c"] == -1


def test_keepmeta() -> None:
    from functools import singledispatch

    from downsat.etl.metadata import getmeta, keepmeta, setmeta

    # normal func
    @keepmeta
    def func(a: set) -> set:
        return set([v for v in a])  # create new set without metadata

    testset = set([1, 2, 3])
    metadata = {"a": 5, "b": "test"}
    setmeta(testset, **metadata)
    transformed_set = func(testset)

    assert getmeta(transformed_set) == metadata

    # singledispatch func
    @singledispatch
    def test_func(a: Any) -> Any:
        raise NotImplementedError

    @test_func.register
    @keepmeta
    def _(a: set) -> set:
        return set([v for v in a])  # create new set without metadata

    transformed_set = test_func(testset)

    assert getmeta(transformed_set) == metadata
