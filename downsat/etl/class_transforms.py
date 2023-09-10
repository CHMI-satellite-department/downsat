from __future__ import annotations

from typing import Any, Callable, Type, overload
from functools import partial

from downsat.etl import abc, protocols, types
from downsat.etl.context import RunContext
from downsat.etl.metaclasses import inject_class_base


def reduce(
    ds: Type[protocols.DataSource[types.KeyType, types.InputOutputType]]
    | partial[protocols.DataSource[types.KeyType, types.InputOutputType]],
    transform: Type[protocols.PipelineTransform[types.InputOutputType, types.OutputType]]
    | partial[protocols.PipelineTransform[types.InputOutputType, types.OutputType]],
) -> Type[protocols.DataSource[types.KeyType, types.OutputType]]:
    """Apply reduce transform on the output of a datasource.

    :param ds: Datasource.
    :param transform: Reduce transform.
    :returns: Datasource that applies reduce transform on the output of the internal datasource.
    """
    return abc._ReduceTransform._concretize(
        {"datasource": ds, "transform": transform}
    )  # TODO: multikey_ds(ds)


def cache(
    ds: Type[protocols.DataSource[types.KeyType, types.OutputType]]
    | partial[protocols.DataSource[types.KeyType, types.OutputType]],
    cache: Type[protocols.Dataset[types.KeyType, types.InputOutputType, types.OutputType]]
    | partial[protocols.Dataset[types.KeyType, types.InputOutputType, types.OutputType]],
    skip_if: Callable[[types.KeyType, types.InputOutputType], bool] | None = None,
) -> Type[protocols.DataSource[types.KeyType, types.OutputType]]:
    """Datasource that caches its outputs.

    :param ds: Original datasource.
    :param cache: Datset used to cache ds outputs.
    :returns: Datasource that acts as a cache for ds.
    """

    cached_datasource = abc._CachedDataSource._concretize({"datasource": ds, "cache": cache})
    if skip_if is not None:
        cached_datasource._skip_if = staticmethod(skip_if)

    return cached_datasource


def query(
    ds: Type[protocols.DataSource[types.KeyType, types.OutputType]]
    | partial[protocols.DataSource[types.KeyType, types.OutputType]],
    by: str | Callable[[types.InputType], dict[str, types.KeyType]] | None = None,
    cache: protocols.Dataset[
        types.InputType, types.KeyType | tuple[types.KeyType, ...], types.KeyType | tuple[types.KeyType, ...]
    ]
    | partial[
        protocols.Dataset[
            types.InputType,
            types.KeyType | tuple[types.KeyType, ...],
            types.KeyType | tuple[types.KeyType, ...],
        ]
    ]
    | None = None,
) -> Type[protocols.MultiKeyDataSource[Any | types.InputType, types.OutputType]]:
    """Compose query function and datasource.

    :param ds: Datasource used to retrieve the data.
    :param by: Callable used to preprocess key to **kwargs passed to ds.query. Or str in which case
        the key goes to that input argument of query. None (default) means that the key must be a dict
        which goes as **kwargs directly to query.
    """

    # TODO: ds = multikey_ds(ds) but it currently crashes - fix

    # TODO: sanity check - dataset must have query method. However, it must work also for cached datasets with query method.

    QueryDataSource = abc._QueryDataSource._concretize({"datasource": ds, "query_cache": cache})

    if by is None:
        # pass key (must be dict) directly to query method
        return QueryDataSource
    elif isinstance(by, str):
        # use ds.query function, query by one particular parameter

        # TODO: this could actually look like _Value2dict(key=by) >> cls.query >> cls
        # and abc._QueryDataSource would not be needed at all,
        # but for that we would have to find a way how to instantiate cls just once
        # so that if instance = cls() than cls.query becomes instance.query
        return abc._Value2dict(key=by) >> QueryDataSource  # type: ignore  # TODO: fix
    elif callable(by):
        # query with preprocessing by an external function provided in by
        return transform_ds_input(
            transform=by, ds=QueryDataSource  # type: ignore  # TODO: fix
        )  # do not use >> directly, because neither ds nor by may support it
    else:
        raise TypeError(f"Invalid type of `by` argument: {type(by)}")


@overload
def multikey_ds(
    cls: Type[protocols.DataSource[types.KeyType_contra, types.OutputType_co]]
) -> Type[abc.MultiKeyDataSource[types.KeyType_contra, types.OutputType_co]]:
    ...


@overload
def multikey_ds(
    cls: partial[Type[protocols.DataSource[types.KeyType_contra, types.OutputType_co]]]
) -> partial[Type[abc.MultiKeyDataSource[types.KeyType_contra, types.OutputType_co]]]:
    ...


def multikey_ds(
    cls: Type[protocols.DataSource[types.KeyType_contra, types.OutputType_co]]
    | partial[Type[protocols.DataSource[types.KeyType_contra, types.OutputType_co]]],
) -> Type[abc.MultiKeyDataSource[types.KeyType_contra, types.OutputType_co]] | partial[
    Type[abc.MultiKeyDataSource[types.KeyType_contra, types.OutputType_co]]
]:
    """Transform DataSource class that accepts single key to a MultiKeyDataSource.

    Parallelizes the requests using joblib.Parallel.
    TODO: allow customisation of the parallelisation (different backend, specify num_cpu in the decorator or by a property of the object during runtime)
    """
    if isinstance(cls, partial):
        # special case: partial function
        return partial(multikey_ds(cls.func), *cls.args, **cls.keywords)  # type: ignore

    if issubclass(cls, abc.MultiKeyDataSource):
        return cls
    # TODO: if is protocols.MultiKeyDataSource, check signature (tuple->tuple)

    def multikey_getitem(
        self: abc.MultiKeyDataSource[types.KeyType_contra, types.OutputType_co],
        key: types.KeyType_contra | tuple[types.KeyType_contra, ...],
    ) -> types.OutputType_co | tuple[types.OutputType_co, ...]:

        if isinstance(key, tuple):
            # multiple keys
            return RunContext.from_obj(self).map(lambda k: cls.__getitem__(self, k), key)  # type: ignore

        # single key
        return cls.__getitem__(self, key)  # type: ignore  # self is MultiKeyDataSource, but single_getitem expects DataSource

    # dynamically redefine the class; works better with mypy than using setattr
    return inject_class_base(cls, abc.MultiKeyDataSource, update_dict={"__getitem__": multikey_getitem})


def transform_ds_input(
    transform: Type[protocols.PipelineTransform[types.InputType, types.KeyType]]
    | protocols.PipelineTransform[types.InputType, types.KeyType]
    | Callable[[types.InputType], types.KeyType]
    | partial[
        protocols.PipelineTransform[types.InputType, types.KeyType]
        | Callable[[types.InputType], types.KeyType]
        | Type[protocols.PipelineTransform[types.InputType, types.KeyType]]
    ],
    ds: Type[protocols.DataSource[types.KeyType, types.OutputType_co]]
    | protocols.DataSource[types.KeyType, types.OutputType_co]
    | partial[
        protocols.DataSource[types.KeyType, types.OutputType_co]
        | Type[protocols.DataSource[types.KeyType, types.OutputType_co]]
    ],
) -> Type[abc._ModifiedInputDataSource[types.InputType, types.KeyType, types.OutputType_co]]:
    """Transform DataSource key.

    Parallelizes the requests using joblib.Parallel.
    TODO: allow customisation of the parallelisation (different backend, specify num_cpu in the decorator or by a property of the object during runtime)
    """

    # TODO: validate input_transform_class and datasource_class
    return abc._ModifiedInputDataSource._concretize(
        {"input_transform": transform, "datasource": ds},
    )


def transform_ds_output(
    ds: Type[protocols.DataSource[types.KeyType_contra, types.InputType]]
    | protocols.DataSource[types.KeyType_contra, types.InputType]
    | partial[
        protocols.DataSource[types.KeyType_contra, types.InputType]
        | Type[protocols.DataSource[types.KeyType_contra, types.InputType]]
    ],
    transform: Type[protocols.PipelineTransform[types.InputType, types.OutputType_co]]
    | protocols.PipelineTransform[types.InputType, types.OutputType_co]
    | Callable[[types.InputType], types.OutputType_co]
    | partial[
        protocols.PipelineTransform[types.InputType, types.OutputType_co]
        | Callable[[types.InputType], types.OutputType_co]
        | Type[protocols.PipelineTransform[types.InputType, types.OutputType_co]]
    ],
) -> Type[abc._ModifiedOutputDataSource[types.KeyType_contra, types.InputType, types.OutputType_co]]:
    """Transform DataSource output.

    Parallelizes the requests using joblib.Parallel.
    TODO: allow customisation of the parallelisation (different backend, specify num_cpu in the decorator or by a property of the object during runtime)
    """

    # TODO: validate input_transform_class and datasource_class
    return abc._ModifiedOutputDataSource._concretize({"datasource": ds, "output_transform": transform})


def compose_transforms(
    transform1: Type[protocols.PipelineTransform[types.InputType_contra, types.OutputInputType]]
    | protocols.PipelineTransform[types.InputType_contra, types.OutputInputType]
    | partial[
        protocols.PipelineTransform[types.InputType_contra, types.OutputInputType]
        | Type[protocols.PipelineTransform[types.InputType_contra, types.OutputInputType]]
    ],
    transform2: Type[protocols.PipelineTransform[types.OutputInputType, types.OutputType_co]]
    | protocols.PipelineTransform[types.OutputInputType, types.OutputType_co]
    | partial[
        protocols.PipelineTransform[types.OutputInputType, types.OutputType_co]
        | Type[protocols.PipelineTransform[types.OutputInputType, types.OutputType_co]]
    ],
) -> Type[abc._ComposedTransform[types.InputType_contra, types.OutputInputType, types.OutputType_co]]:
    """Compose two transforms into single ComposedTransform.

    Creates new transform that, when instantiated, does roughly
    composed_transform(inp) = transform2(transform1(inp)).

    Init arguments of ComposedTransform are union of init arguments of both transforms
    and each transform gets only the relevant ones during its instantiation. If both
    transforms have an argument with the same name and same (or no) default, it gets
    the same init value. However, if the default is different, a TypeError is raised.

    Note: If any of the transforms has signature `transform(*args, **kwargs)`, it will
    not get any init values, because this is default signature of a class without __init__
    method.

    :param transform1: Left transform class, instance or function.
    :param transform2: Right transform class, instance or function.
    :returns: Composed transform.
    :raises TypeError: Init arguments of the transforms are not compatible.
    """
    # TODO: validate input_transform_class and datasource_class
    return abc._ComposedTransform._concretize({"transform1": transform1, "transform2": transform2})
