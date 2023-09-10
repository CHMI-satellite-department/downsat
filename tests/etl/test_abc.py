from __future__ import annotations

from typing import TYPE_CHECKING, Any, Hashable, Type
from collections.abc import Sequence
from functools import partial
from pathlib import Path

import pytest
from pytest_cases import parametrize_with_cases


if TYPE_CHECKING:
    from downsat.etl import protocols


def case_transform_class_with_fields() -> tuple[Type["protocols.PipelineTransform"], dict[str, Any]]:
    class TestTransform:
        def __init__(self, inp_arg: int) -> None:
            self.inp_arg = inp_arg

        def __call__(self, inp: int) -> int:
            return inp + self.inp_arg

    return TestTransform, {"inp_arg": 3}


def case_transform_partial_class_with_fields() -> tuple[
    partial[Type["protocols.PipelineTransform"]], dict[str, Any]
]:
    from functools import partial

    class TestTransform:
        def __init__(self, inp_arg: int) -> None:
            self.inp_arg = inp_arg

        def __call__(self, inp: int) -> int:
            return inp + self.inp_arg

    return partial(TestTransform, inp_arg=3), {}  # type: ignore


def case_transform_class_wo_fields() -> tuple[Type["protocols.PipelineTransform"], dict[str, Any]]:
    class TestTransform:
        def __call__(self, inp: int) -> int:
            return inp + 3

    return TestTransform, {}


def case_transform_function() -> tuple["protocols.PipelineTransform[float, float]", dict[str, Any]]:
    def test_func(inp: float) -> float:
        return inp + 3.1

    return test_func, {}


def case_datasource_class_wo_fields() -> tuple[Type["protocols.DataSource"], dict[str, Any]]:
    class TestSource:
        def __getitem__(self, key: int) -> int:
            return key**2

    return TestSource, {}


def case_datasource_class_with_fields_classical() -> tuple[Type["protocols.DataSource"], dict[str, Any]]:
    class TestSource:
        def __init__(self, init_arg: int) -> None:
            self.init_arg = init_arg

        def __getitem__(self, key: int) -> int:
            return key + self.init_arg

    return TestSource, {"init_arg": 3}


def case_datasource_class_with_fields_attrs() -> tuple[Type["protocols.DataSource"], dict[str, Any]]:
    from attrs import define

    @define(slots=False)
    class TestSource:
        init_arg: int

        def __getitem__(self, key: int) -> int:
            return key + self.init_arg

    return TestSource, {"init_arg": 3}


def case_datasource_class_with_fields_attrs_and_slots() -> tuple[
    Type["protocols.DataSource"], dict[str, Any]
]:
    pytest.xfail("multikey_ds not yet implemented for slot classes")
    from attrs import define

    @define
    class TestSource:
        init_arg: int

        def __getitem__(self, key: int) -> int:
            return key + self.init_arg

    return TestSource, {"init_arg": 3}


def case_datasource_class_with_fields_attrs_frozen() -> tuple[Type["protocols.DataSource"], dict[str, Any]]:
    from attrs import frozen

    @frozen(slots=False)
    class TestSource:
        init_arg: int

        def __getitem__(self, key: int) -> int:
            return key + self.init_arg

    return TestSource, {"init_arg": 3}


def case_datasource_class_with_fields_attrs_field_default() -> tuple[
    Type["protocols.DataSource"], dict[str, Any]
]:
    from attrs import field, frozen

    @frozen(slots=False)
    class TestSource:
        init_arg: int = field(default=0)
        init_arg2: int = field(default=1)

        def __getitem__(self, key: int) -> int:
            return key + self.init_arg + 2 * self.init_arg2

    return TestSource, {"init_arg": 3}


def case_datasource_class_with_fields_attrs_field_factory() -> tuple[
    Type["protocols.DataSource"], dict[str, Any]
]:
    from attrs import field, frozen

    @frozen(slots=False)
    class TestSource:
        init_arg: int = field(factory=lambda: 0)
        init_arg2: int = field(factory=lambda: 1)

        def __getitem__(self, key: int) -> int:
            return key + self.init_arg + 2 * self.init_arg2

    return TestSource, {"init_arg": 3}


def case_datasource_class_partial_with_fields_attrs_frozen() -> tuple[
    partial[Type["protocols.DataSource"]], dict[str, Any]
]:
    """DataSource with fields created by functools.partial and attrs.frozen."""
    from functools import partial

    from attrs import frozen

    @frozen(slots=False)
    class TestSource:
        init_arg: int

        def __getitem__(self, key: int) -> int:
            return key + self.init_arg

    return partial(TestSource, init_arg=3), {}  # type: ignore


def case_query_cache_none() -> None:
    return None


def case_query_cache_diskcache(tmp_path: Path) -> "protocols.Dataset":
    from diskcache import Cache

    return Cache(directory=tmp_path)


def test_incomplete_pipeline_transform() -> None:
    from downsat.etl.abc import PipelineTransform

    class IncompleteTestTransform(PipelineTransform):
        ...

    # PipelineTransform is ABC => cannot be instantiated without implementing __call__
    with pytest.raises(TypeError):
        IncompleteTestTransform()  # type: ignore  # the mypy error is what we are actually testing


@parametrize_with_cases("TestSource_", cases=".", prefix="case_datasource")
def test_pipeline_transform_rshift(TestSource_: tuple[Type["protocols.DataSource"], dict[str, Any]]) -> None:
    from downsat.etl.abc import MultiKeyDataSource, PipelineTransform

    TestSource, source_kwargs = TestSource_

    class TestTransform(PipelineTransform):
        def __call__(self, inp: int) -> int:
            return inp + 3

    test_source = TestSource(**source_kwargs)
    try:
        TestSource()
    except Exception:
        empty_test_source_raises = True
    else:
        empty_test_source_raises = False

    inp = 1
    # __rshift__ works
    ModifiedKeySource: Type[MultiKeyDataSource[int, int]] = TestTransform >> TestSource
    assert ModifiedKeySource(**source_kwargs)[inp] == test_source[TestTransform()(inp)]
    if source_kwargs and empty_test_source_raises:
        with pytest.raises(TypeError):
            ModifiedKeySource()

    # chaining __rshift__ works
    ModifiedKeySource2 = TestTransform >> TestTransform >> TestSource
    assert ModifiedKeySource2(**source_kwargs)[inp] == test_source[TestTransform()(TestTransform()(inp))]
    if source_kwargs and empty_test_source_raises:
        with pytest.raises(TypeError):
            ModifiedKeySource2()

    # __rrshift__ works
    ModifiedOutputSource = TestSource >> TestTransform
    assert ModifiedOutputSource(**source_kwargs)[inp] == TestTransform()(test_source[inp])
    if source_kwargs and empty_test_source_raises:
        with pytest.raises(TypeError):
            ModifiedOutputSource()

    # chaining __rrshift__ works
    ModifiedOutputSource2 = TestSource >> TestTransform >> TestTransform
    assert ModifiedOutputSource2(**source_kwargs)[inp] == TestTransform()(TestTransform()(test_source[inp]))
    if source_kwargs and empty_test_source_raises:
        with pytest.raises(TypeError):
            ModifiedOutputSource2()

    # combination of __rshift__ and __rrshift__ works
    ModifiedSource = TestTransform >> TestSource >> TestTransform
    assert ModifiedSource(**source_kwargs)[inp] == TestTransform()(test_source[TestTransform()(inp)])
    if source_kwargs and empty_test_source_raises:
        with pytest.raises(TypeError):
            ModifiedSource()


@parametrize_with_cases("TestTransform_", cases=".", prefix="case_transform")
def test_datasource_rshift(
    TestTransform_: tuple[Type["protocols.PipelineTransform"], dict[str, Any]]
) -> None:
    from downsat.etl.abc import DataSource

    TestTransform, transform_kwargs = TestTransform_

    class TestSource(DataSource):
        def __getitem__(self, key: int) -> int:
            return key**2

    inp = 1
    if isinstance(TestTransform, (type, partial)):
        # class
        test_transform = TestTransform(**transform_kwargs)  # type: ignore  # cannot instantiate Callable
    else:
        # function
        test_transform = TestTransform

    try:
        TestTransform()
    except Exception:
        empty_transform_raises = True
    else:
        empty_transform_raises = False

    # __rrshift__ works
    ModifiedKeySource = TestTransform >> TestSource  # type: ignore  # TODO: why? fix
    assert ModifiedKeySource(**transform_kwargs)[inp] == TestSource()[test_transform(inp)]
    if transform_kwargs and empty_transform_raises:
        with pytest.raises(TypeError):
            ModifiedKeySource()

    # chaining __rrshift__ works
    ModifiedKeySource2 = TestTransform >> (
        TestTransform >> TestSource
    )  # TODO: add >> to function objects so that this works without parenthesis?
    assert ModifiedKeySource2(**transform_kwargs)[inp] == TestSource()[test_transform(test_transform(inp))]
    if transform_kwargs and empty_transform_raises:
        with pytest.raises(TypeError):
            ModifiedKeySource2()

    # __rshift__ works
    ModifiedOutputSource = TestSource >> TestTransform
    assert ModifiedOutputSource(**transform_kwargs)[inp] == test_transform(TestSource()[inp])
    if transform_kwargs and empty_transform_raises:
        with pytest.raises(TypeError):
            ModifiedOutputSource()

    # chaining __rshift__ works
    ModifiedOutputSource2 = TestSource >> TestTransform >> TestTransform
    assert ModifiedOutputSource2(**transform_kwargs)[inp] == test_transform(test_transform(TestSource()[inp]))
    if transform_kwargs and empty_transform_raises:
        with pytest.raises(TypeError):
            ModifiedOutputSource2()

    # combination of __rshift__ and __rrshift__ works
    ModifiedSource = TestTransform >> TestSource >> TestTransform  # type: ignore  # TODO: why? fix
    assert ModifiedSource(**transform_kwargs)[inp] == test_transform(TestSource()[test_transform(inp)])  # type: ignore  # TODO: why? fix
    if transform_kwargs and empty_transform_raises:
        with pytest.raises(TypeError):
            ModifiedSource()


@parametrize_with_cases("TestSource_", cases=".", prefix="case_datasource")
@parametrize_with_cases("test_query_cache", cases=".", prefix="case_query_cache")
def test_query(
    TestSource_: tuple[Type["protocols.DataSource"], dict[str, Any]],
    test_query_cache: "protocols.Dataset[Any, Any, Any]" | None,
) -> None:
    from downsat.etl.class_transforms import query

    TestSource, source_kwargs = TestSource_

    def query_fun(self: Any, x: int | None = None, y: int | None = None) -> int:
        query_fun.n_queries += 1  # type: ignore  # mypy doesn't like this trick

        res = 0
        if x:
            res += 2 * x
        if y:
            res += y
        return res

    query_fun.n_queries = 0  # type: ignore  # mypy doesn't like this trick

    test_source = TestSource(**source_kwargs)
    if isinstance(TestSource, partial):
        TestSource.func.query = query_fun  # type: ignore  # DataSource has no attribute query
    else:
        TestSource.query = query_fun  # type: ignore  # DataSource has no attribute query

    # by=callable
    QueryDataSource_callable = query(TestSource, by=lambda x: {"x": x, "y": 3}, cache=test_query_cache)  # type: ignore  # TODO: fix; cannot infer type argument 1
    queried_source = QueryDataSource_callable(**source_kwargs)
    assert queried_source[1] == test_source[5]
    assert query_fun.n_queries == 1  # type: ignore  # mypy doesn't like this trick
    if test_query_cache is not None:
        assert queried_source[1] == test_source[5]  # should go from cache
        assert query_fun.n_queries == 1  # type: ignore  # mypy doesn't like this trick

    # by=string
    QueryDataSource_x = query(TestSource, by="x", cache=test_query_cache)
    queried_source_x = QueryDataSource_x(**source_kwargs)
    assert queried_source_x[1] == test_source[2]
    assert query_fun.n_queries == 2  # type: ignore  # mypy doesn't like this trick
    if test_query_cache is not None:
        assert queried_source_x[1] == test_source[2]  # should go from cache
        assert query_fun.n_queries == 2  # type: ignore  # mypy doesn't like this trick

    QueryDataSource_y = query(TestSource, by="y", cache=test_query_cache)
    queried_source_y = QueryDataSource_y(**source_kwargs)
    assert queried_source_y[1] == test_source[1]
    assert query_fun.n_queries == 3  # type: ignore  # mypy doesn't like this trick
    if test_query_cache is not None:
        assert queried_source_y[1] == test_source[1]  # should go from cache
        assert query_fun.n_queries == 3  # type: ignore  # mypy doesn't like this trick


def test_cache() -> None:
    from downsat.etl.abc import MultiKeyDataSource
    from downsat.etl.class_transforms import cache

    class CounterDict(MultiKeyDataSource):
        def __init__(self, init_dict: dict | None = None) -> None:
            self.get_counter = 0
            self.set_counter = 0
            self.data_dict = init_dict or {}

        def __getitem__(self, key: Hashable | Sequence[Hashable]) -> Any:
            if isinstance(key, Sequence) and not isinstance(key, str):
                return [self[k] for k in key]
            self.get_counter += 1
            return self.data_dict[key]

        def __setitem__(self, key: Hashable | Sequence[Hashable], item: Any | Sequence[Any]) -> None:
            if isinstance(key, Sequence) and not isinstance(key, str):
                assert len(key) == len(item)
                for k, it in zip(key, item):
                    self[k] = it
                return

            # single item
            self.set_counter += 1
            self.data_dict[key] = item

        def query(self, **kwargs: Any) -> tuple[Hashable, ...]:
            startswith = kwargs.pop("startswith")
            if len(kwargs) > 0:
                raise ValueError(f"Unexpected query arguments: {kwargs.keys()}")
            keys = list(self.data_dict.keys())
            if startswith is not None:
                keys = [k for k in keys if k.startswith(startswith)]
            return tuple(keys)

    class CounterSink(CounterDict):
        def __init__(self) -> None:
            super().__init__()

    Dataset = cache(CounterDict, cache=CounterSink)
    dataset = Dataset(init_dict={"a": 0, "b": 1})  # type: ignore  # TODO: fix, make mypy plugin
    source = dataset._datasource  # type: ignore  # TODO: fix, make mypy plugin
    sink = dataset._cache  # type: ignore  # TODO: fix, make mypy plugin

    # caching works
    assert dataset["a"] == 0
    assert source.get_counter == 1
    assert sink.set_counter == 1
    assert (
        sink.get_counter == 2
    )  # first try get from sink, then set from source and get from it immediately after

    assert dataset["a"] == 0
    assert source.get_counter == 1
    assert sink.set_counter == 1
    assert sink.get_counter == 3

    assert dataset["b"] == 1
    assert source.get_counter == 2
    assert sink.set_counter == 2
    assert (
        sink.get_counter == 5
    )  # first try get from sink, then set from source and get from it immediately after

    # can query
    source["ahoj"] = 2
    query = dataset.query(startswith="a")  # type: ignore  # TODO: fix, make mypy plugin
    assert len(query) == 2
    res = dataset[query]
    assert len(res) == 2

    # skip_if works
    Dataset_skip = cache(CounterDict, cache=CounterSink, skip_if=lambda key, _: key == "a")
    dataset_skip = Dataset_skip(init_dict={"a": 0, "b": 1})  # type: ignore  # TODO: fix, make mypy plugin
    cache_skip = dataset_skip._cache  # type: ignore  # TODO: fix, make mypy plugin

    assert dataset_skip["a"] == 0
    assert len(cache_skip.data_dict) == 0  # "a" was not cached
    assert dataset_skip["b"] == 1
    assert len(cache_skip.data_dict) == 1  # "b" was cached

    # applies transform

    def transform(x: int) -> int:
        return x + 1

    Dataset2 = cache(CounterDict >> transform, cache=CounterSink)
    dataset2 = Dataset2(init_dict={0: 0, 1: 1, 2: 2})  # type: ignore  # TODO: fix, make mypy plugin
    source2 = dataset2._datasource  # type: ignore  # TODO: fix, make mypy plugin
    sink2 = dataset2._cache  # type: ignore  # TODO: fix, make mypy plugin

    # cache-miss
    assert dataset2[0] == 1  # transform shifts the key by 1
    assert source2.get_counter == 1
    assert sink2.set_counter == 1
    assert (
        sink2.get_counter == 2
    )  # first try get from sink, then set from source and get from it immediately after

    # cache-hit
    assert dataset2[0] == 1  # transform shifts the key by 1
    assert source2.get_counter == 1
    assert sink2.set_counter == 1
    assert sink2.get_counter == 3

    # cache-miss
    assert dataset2[1] == 2  # transform shifts the key by 1
    assert source2.get_counter == 2
    assert sink2.set_counter == 2
    assert (
        sink2.get_counter == 5
    )  # first try get from sink, then set from source and get from it immediately after


@parametrize_with_cases("TestSource_", cases=".", prefix="case_datasource")
def test_parallel_backend(TestSource_: tuple[Type["protocols.DataSource"], dict[str, Any]]) -> None:
    from downsat.etl.abc import MultiKeyDataSource, PipelineTransform
    from downsat.etl.context import setcontext

    TestSource, source_kwargs = TestSource_

    class TestTransform(PipelineTransform):
        def __call__(self, inp: int) -> int:
            return inp + 3

    test_source = TestSource(**source_kwargs)

    inp = (1, 2)
    # paralellized __rshift__ works
    ModifiedKeySource: Type[MultiKeyDataSource[int, int]] = TestTransform >> TestSource
    setcontext(num_workers=2)(ModifiedKeySource)
    assert ModifiedKeySource(**source_kwargs)[inp] == tuple(test_source[TestTransform()(i)] for i in inp)

    # TODO: test also other compositions Dataset >> Transform, Transform >> Transform, cache, query


@parametrize_with_cases("TestSource_", cases=".", prefix="case_datasource")
def test_reduce(
    TestSource_: tuple[Type["protocols.DataSource"], dict[str, Any]],
) -> None:
    """Test that the reduce method works."""
    from downsat.etl.class_transforms import multikey_ds, reduce

    TestSource, source_kwargs = TestSource_
    TestSource = multikey_ds(TestSource)

    def reduce_transform_sum(inp: tuple[int, ...]) -> tuple[int]:
        return (sum(inp),)

    orig_source = TestSource(**source_kwargs)
    ReducedSource = reduce(TestSource, reduce_transform_sum)  # type: ignore # TODO: fix
    reduced_source = ReducedSource(**source_kwargs)

    orig_source[1, 2, 3]
    assert reduced_source[1, 2, 3] == (sum(orig_source[1, 2, 3]),)
