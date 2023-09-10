from typing import Any, Type

import pytest
from pytest_cases import parametrize_with_cases
from pytest_mock import MockerFixture


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


@parametrize_with_cases("class_with_context", cases=".", prefix="class")
def test_setcontext(class_with_context: Type) -> None:
    from downsat.etl.context import RunContext, getcontext, setcontext

    c = class_with_context()

    # context can be set and recovered
    setcontext(num_workers=3)(c)
    assert RunContext.from_obj(c).num_workers == 3
    assert getcontext(c)["num_workers"] == 3

    # cannot use invalid context variable in strict mode
    with pytest.raises(ValueError):
        setcontext(invalid_var=5)

    # can use uknown context variables in non-strict mode
    setcontext(invalid_value=5, _strict=False)


def test_context_map(mocker: MockerFixture) -> None:
    from downsat.etl import context

    mocker.patch("downsat.etl.context.Parallel")

    def mapfun(element: Any) -> None:  # noqa: U100
        pass

    data = [None] * 3

    # serial processing does not call joblib.Parallel
    serial_context = context.RunContext(max_workers=0)
    serial_context.map(mapfun, data)
    context.Parallel.assert_not_called()

    # parallel processing calls joblib.Parallel with correct number of jobs
    parallel_context = context.RunContext()
    parallel_context.map(mapfun, data)
    context.Parallel.assert_not_called()  # single serial job if not specified otherwise
    context.Parallel.reset_mock()

    parallel_context = context.RunContext(num_workers=5, max_workers=3)
    parallel_context.map(mapfun, data)
    context.Parallel.assert_called_once_with(n_jobs=3)
