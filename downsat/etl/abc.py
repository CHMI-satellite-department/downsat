from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, Generic, Type, _ProtocolMeta, overload
from abc import ABC, ABCMeta, abstractmethod, abstractproperty
from functools import partial
import logging

from attrs import field, frozen

from downsat.etl import class_transforms, protocols, types
from downsat.etl.class_utils import _build_constant_property, _check_fields_compatibility, _extract_fields
from downsat.etl.context import RunContext


if TYPE_CHECKING:
    from attrs._make import _CountingAttr


def _is_pipeline_transform(transform: Any) -> bool:
    """Check if the object follows the PipelineTransform protocol"""
    # since protocols.PipelineTransform is a subscripted Generics Callable[[Any], Any],
    # we cannot use isinstance(transform, protocols.PipelineTransform) or issubclass
    # and we must test it this is a correct transform manually
    if isinstance(transform, type):
        return issubclass(
            transform, protocols.PipelineTransform
        )  # TODO: and check signature for a single arguments
    elif isinstance(transform, partial):
        return issubclass(
            transform.func, protocols.PipelineTransform  # type: ignore
        )  # TODO: and check signature for a single arguments
    else:
        return isinstance(
            transform, protocols.PipelineTransform
        )  # TODO: and check signature for a single arguments


class DataSourceMeta(_ProtocolMeta, ABCMeta):
    """Metaclass implementing >> operator on DataSource.

    Derived from ABCMeta and _ProtocolMeta to allow combining with ABC classes.
    """

    def __rshift__(
        ds: Type[protocols.DataSource[types.KeyType_contra, types.InputType]],
        transform: Type[protocols.PipelineTransform[types.InputType, types.OutputType_co]]
        | protocols.PipelineTransform[types.InputType, types.OutputType_co]
        | Callable[[types.InputType], types.OutputType_co],
    ) -> Type[MultiKeyDataSource[types.KeyType_contra, types.OutputType_co]]:
        """DataSource >> PipelineTransform transforms DataSource output.

        Using instace of the return data source `new_data_source[inp_data]` is
        equivalent to `transform(original_data_source[inp_data])`.

        :param transform: Transformation of data source output.
        :param ds: Original data source.
        :returns: New data source that outputs transformed data.
        """
        if _is_pipeline_transform(transform):
            return class_transforms.transform_ds_output(ds, transform)
        else:
            raise TypeError(f"Unkonwn type {type(transform)} for >> operator.)")

    def __rrshift__(
        ds: Type[protocols.DataSource[types.KeyType, types.OutputType_co]],
        transform: Type[protocols.PipelineTransform[types.InputType, types.KeyType]]
        | protocols.PipelineTransform[types.InputType, types.KeyType]
        | Callable[[types.InputType], types.KeyType],
    ) -> Type[MultiKeyDataSource[types.InputType, types.OutputType_co]]:
        """PipelineTransform >> DataSource transforms DataSource key.

        Using instace of the return data source `new_data_source[inp_data]` is
        equivalent to `original_data_source[transform(inp_data)]`.

        :param transform: Transformation of input data to data source key.
        :param ds: Original data source.
        :returns: New data source that takes input data instead of the original key.
        """
        if _is_pipeline_transform(transform):
            return class_transforms.transform_ds_input(transform, ds)
        else:
            raise TypeError(f"Unkonwn type {type(transform)} for >> operator.)")


class DataSource(
    ABC,
    protocols.DataSource[types.KeyType_contra, types.OutputType_co],
    Generic[types.KeyType_contra, types.OutputType_co],
    metaclass=DataSourceMeta,
):
    """DataSource abstract base class. Implements DataSource protocol."""

    @abstractmethod
    def __getitem__(self, key: types.KeyType_contra) -> types.OutputType_co:
        """Return item.

        :param key: Index or id of the item.
        :returns: Item.
        :raises KeyError: The item is not present.
        """


class MultiKeyDataSource(
    ABC,
    protocols.MultiKeyDataSource[types.KeyType_contra, types.OutputType_co],
    Generic[types.KeyType_contra, types.OutputType_co],
    metaclass=DataSourceMeta,
):
    """Abstract base class of a datasource accepting multiple keys. Implements MultiKeyDataSource."""

    @abstractmethod
    def __getitem__(
        self, key: types.KeyType_contra | tuple[types.KeyType_contra, ...]
    ) -> types.OutputType_co | tuple[types.OutputType_co, ...]:
        """Return one or more items.

        :param key: Index or id of the item or tuple of those.
        :returns: Item or items.
        :raises KeyError: At least one of the items is not present.
        """


class PipelineTransformMeta(_ProtocolMeta, ABCMeta):
    """Metaclass implementing >> operator on PipelineTransform.

    Derived from ABCMeta and _ProtocolMeta to allow combining with ABC classes and Protocols.
    """

    @overload
    def __rshift__(  # type: ignore  # using transform instead of self
        transform: Type[protocols.PipelineTransform[types.InputType, types.KeyType]],
        ds_or_transform: Type[protocols.DataSource[types.KeyType, types.OutputType_co]]
        | protocols.DataSource[types.KeyType, types.OutputType_co],
    ) -> Type[MultiKeyDataSource[types.InputType, types.OutputType_co]]:
        ...

    @overload
    def __rshift__(  # type: ignore  # using transform instead of self
        transform: Type[protocols.PipelineTransform[types.InputType, types.KeyType]],
        ds_or_transform: Type[protocols.PipelineTransform[types.KeyType, types.OutputType_co]]
        | protocols.PipelineTransform[types.KeyType, types.OutputType_co],
    ) -> Type[PipelineTransform[types.InputType, types.OutputType_co]]:
        ...

    def __rshift__(  # type: ignore  # using transform instead of self
        transform: Type[protocols.PipelineTransform[types.InputType, types.KeyType]],
        ds_or_transform: Type[protocols.DataSource[types.KeyType, types.OutputType_co]]
        | protocols.DataSource[types.KeyType, types.OutputType_co]
        | Type[protocols.PipelineTransform[types.KeyType, types.OutputType_co]]
        | protocols.PipelineTransform[types.KeyType, types.OutputType_co],
    ) -> Type[MultiKeyDataSource[types.InputType, types.OutputType_co]] | Type[
        PipelineTransform[types.InputType, types.OutputType_co]
    ]:
        """PipelineTransform >> DataSource transforms DataSource key.

        Using instace of the return data source `new_data_source[inp_data]` is
        equivalent to `original_data_source[transform(inp_data)]`.

        :param transform: Transformation of input data to data source key.
        :param ds_or_transform: Data source whose key should be modified or transform
            that should be composed.
        :returns: New data source that takes input data instead of the original key.
        """
        if (
            (isinstance(ds_or_transform, type) and issubclass(ds_or_transform, protocols.DataSource))
            or isinstance(ds_or_transform, protocols.DataSource)
            or (
                isinstance(ds_or_transform, partial)
                and issubclass(ds_or_transform.func, protocols.DataSource)  # type: ignore
            )
        ):
            # modify dataset key
            return class_transforms.transform_ds_input(transform, ds_or_transform)
        elif _is_pipeline_transform(ds_or_transform):
            # compose transforms
            return class_transforms.compose_transforms(transform, ds_or_transform)
        else:
            raise TypeError(
                f"Unkonwn type {type(ds_or_transform)} for >> operator.)"
            )  # TODO: implement DataSink and Dataset (setitem has priority over getitem)

    @overload
    def __rrshift__(  # type: ignore  # transform instead of self
        transform: Type[protocols.PipelineTransform[types.InputType, types.OutputType_co]],
        ds_or_transform: Type[protocols.DataSource[types.KeyType_contra, types.InputType]]
        | protocols.DataSource[types.KeyType_contra, types.InputType],
    ) -> Type[MultiKeyDataSource[types.KeyType_contra, types.OutputType_co]]:
        ...

    @overload
    def __rrshift__(  # type: ignore  # transform instead of self
        transform: Type[protocols.PipelineTransform[types.InputType, types.OutputType_co]],
        ds_or_transform: type[protocols.PipelineTransform[types.KeyType_contra, types.InputType]]
        | protocols.PipelineTransform[types.KeyType_contra, types.InputType],
    ) -> Type[PipelineTransform[types.KeyType_contra, types.OutputType_co]]:
        ...

    def __rrshift__(  # type: ignore  # transform instead of self
        transform: Type[protocols.PipelineTransform[types.InputType, types.OutputType_co]],
        ds_or_transform: Type[protocols.DataSource[types.KeyType_contra, types.InputType]]
        | protocols.DataSource[types.KeyType_contra, types.InputType]
        | type[protocols.PipelineTransform[types.KeyType_contra, types.InputType]]
        | protocols.PipelineTransform[types.KeyType_contra, types.InputType],
    ) -> Type[MultiKeyDataSource[types.KeyType_contra, types.OutputType_co]] | Type[
        PipelineTransform[types.KeyType_contra, types.OutputType_co]
    ]:
        """DataSource >> PipelineTransform transforms DataSource output.

        Using instace of the return data source `new_data_source[inp_data]` is
        equivalent to `transform(original_data_source[inp_data])`.

        :param transform: Transformation of data source output.
        :param ds: Original data source.
        :returns: New data source that outputs transformed data.
        """
        if (
            (isinstance(ds_or_transform, type) and issubclass(ds_or_transform, protocols.DataSource))
            or isinstance(ds_or_transform, protocols.DataSource)
            or (
                isinstance(ds_or_transform, partial)
                and issubclass(ds_or_transform.func, protocols.DataSource)  # type: ignore
            )
        ):
            # modify output of datasource
            return class_transforms.transform_ds_output(ds_or_transform, transform)
        elif _is_pipeline_transform(ds_or_transform):
            # compose two transforms
            return class_transforms.compose_transforms(ds_or_transform, transform)
        else:
            raise TypeError(f"Unkonwn type {type(ds_or_transform)} for >> operator.)")


class PipelineTransform(
    ABC,
    Generic[types.InputType_contra, types.OutputType_co],
    metaclass=PipelineTransformMeta,
):
    @abstractmethod
    def __call__(self, inp: types.InputType_contra) -> types.OutputType_co:
        pass


class _InstantiateClassMixin:
    _logger = field(init=False)

    # TODO: test funcionality
    @overload
    def _maybe_instantiate(
        self, obj: Type[types.Instance], kwarg_names: list[str] | None = None
    ) -> types.Instance:
        ...

    @overload
    def _maybe_instantiate(self, obj: types.Instance, kwarg_names: list[str] | None = None) -> types.Instance:
        ...

    def _maybe_instantiate(
        self, obj: types.Instance | Type[types.Instance], kwarg_names: list[str] | None = None
    ) -> types.Instance:
        if obj is None:
            return None
        elif isinstance(obj, (type, partial)):
            kwarg_names = kwarg_names or []
            kwargs = {name: getattr(self, name) for name in kwarg_names}

            # class that needs to be instantiated
            return obj(**kwargs)
        else:
            # already transform function
            return obj

    def __attrs_post_init__(self) -> None:
        """Create instances of the properties."""
        # create logger local to the class
        object.__setattr__(self, "_logger", logging.getLogger(self.__class__.__name__))

        # find properties that should be instantiated
        all_fields = dir(self.__class__)
        properties_for_initialization = [
            field
            for field in all_fields
            if (field + "_kwargs" in all_fields) and (field + "_def" in all_fields) and field.startswith("_")
        ]

        # instantiate
        for property_name in properties_for_initialization:
            property_instance = self._maybe_instantiate(
                getattr(self, f"{property_name}_def"), getattr(self, f"{property_name}_kwargs")
            )
            object.__setattr__(self, f"{property_name}", property_instance)

    @classmethod
    def _concretize(cls, objects: dict[str, Any], class_name: str | None = None) -> Type:
        """Helper function that builds a dynamic class that can instatiate given properties on init.

        :param class_name: Name of the created class.
        :param base_class: Parent class implementing bussiness logic.
        :param properties: Dict of properties to be added to the class. The class will have properties
            _{key}_def, _{key}_kwargs and _{key}
        :returns: Class that instantiates the properties during __init__ and passes them proper init arguments.
        """

        # add properties _{name}_def that store the property class or instance
        properties: dict[str, Any] = {
            f"_{name}_def": _build_constant_property(obj) for name, obj in objects.items()
        }

        # find init arguments of all properties and make sure they are compatible
        kwargs: dict[str, dict[str, "_CountingAttr"]] = {}
        for name, obj in objects.items():
            fields = _extract_fields(obj, properties)
            for other_fields in kwargs.values():
                _check_fields_compatibility(fields, other_fields)
            kwargs[name] = fields

        # store infor on which input arguments go to which property during initialization
        for name, fields in kwargs.items():
            properties.update(fields)
            properties[f"_{name}_kwargs"] = list(fields.keys())

        if class_name is None:
            # use parent class name but remove leading _ so that the names do not clash
            # TODO: rather append some random string and check that the class name is unique?
            class_name = cls.__name__
            if not class_name.startswith("_"):
                raise ValueError(
                    f"Parent class name {class_name} does not start with '_'. "
                    "Please provide explicit class name via input argument class_name."
                )
            class_name = class_name[1:]
        cls = type(class_name, (cls,), properties)
        return frozen(slots=False)(cls)


class _ModifiedInputDataSource(
    _InstantiateClassMixin,
    MultiKeyDataSource[types.InputType, types.OutputType_co],
    Generic[types.InputType, types.KeyType, types.OutputType_co],
):
    @abstractproperty
    def _input_transform_def(self) -> Type[protocols.PipelineTransform[types.InputType, types.KeyType]]:
        """Transform keys entering the datasource."""

    @abstractproperty
    def _datasource_def(self) -> Type[protocols.DataSource[types.KeyType, types.OutputType_co]]:
        """Original datasource."""

    @abstractproperty
    def _input_transform_kwargs(self) -> list[str]:
        """Kwargs used when initializing self._input_transform_def."""

    @abstractproperty
    def _datasource_kwargs(self) -> list[str]:
        """Kwargs used when initializing self._datasource_def."""

    _input_transform: protocols.PipelineTransform[types.InputType, types.KeyType] = field(init=False)
    _datasource: protocols.DataSource[types.KeyType, types.OutputType_co] = field(init=False)

    def __getattr__(self, item: str) -> Any:
        """Make all methods and properties from the original datasource accessible."""
        return getattr(self._datasource, item)

    def __getitem__(
        self,
        key: types.InputType | tuple[types.InputType, ...],
    ) -> types.OutputType_co | tuple[types.OutputType_co, ...]:

        result: types.OutputType_co | tuple[types.OutputType_co, ...]
        if isinstance(key, tuple):
            # multiple keys
            # TODO: what if ds is of type MultiKeyDataSource? Some optimization here?
            result = RunContext.from_obj(self).map(
                self.__getitem__, key  # type: ignore  # TODO: fix
            )  # TODO: use class_transform decorator `multikey``
        else:
            # single key
            new_key = self._input_transform(key)
            self._logger.debug(
                f"Applied input transform {self._input_transform_def} on key {key}, got {new_key}."
            )
            result = self._datasource[new_key]
            self._logger.debug(f"Loaded key {new_key} from {self._datasource_def}.")

        return result


class _ModifiedOutputDataSource(
    _InstantiateClassMixin,
    MultiKeyDataSource[types.KeyType_contra, types.OutputType_co],
    Generic[types.KeyType_contra, types.InputType, types.OutputType_co],
):
    """Datasource that transforms its outputs."""

    @abstractproperty
    def _datasource_def(self) -> Type[protocols.DataSource[types.KeyType_contra, types.InputType]]:
        """Original datasource."""

    @abstractproperty
    def _output_transform_def(
        self,
    ) -> Type[protocols.PipelineTransform[types.InputType, types.OutputType_co]]:
        """Transform datasource outputs."""

    @abstractproperty
    def _datasource_kwargs(self) -> list[str]:
        """Kwargs used when initializing self._datasource_def."""

    @abstractproperty
    def _output_transform_kwargs(self) -> list[str]:
        """Kwargs used when initializing self._output_transform_def."""

    _datasource: protocols.DataSource[types.KeyType_contra, types.InputType] = field(init=False)
    _output_transform: protocols.PipelineTransform[types.InputType, types.OutputType_co] = field(init=False)

    def __getattr__(self, item: str) -> Any:
        """Make all methods and properties from the original datasource accessible."""
        return getattr(self._datasource, item)

    def __getitem__(
        self,
        key: types.KeyType_contra | tuple[types.KeyType_contra, ...],
    ) -> types.OutputType_co | tuple[types.OutputType_co, ...]:

        result: types.OutputType_co | tuple[types.OutputType_co]
        if isinstance(key, tuple):
            # multiple keys
            # TODO: what if ds is of type MultiKeyDataSource? Some optimization here?
            result = RunContext.from_obj(self).map(
                self.__getitem__, key  # type: ignore  # TODO: fix
            )  # TODO: use class_transform decorator `multikey``
        else:
            # single key
            data = self._datasource[key]
            result = self._output_transform(data)
            self._logger.debug(
                f"Applied output transform {self._output_transform_def} on key {key} loaded from {self._datasource_def}."
            )

        return result


class _ComposedTransform(
    _InstantiateClassMixin,
    PipelineTransform[types.InputType_contra, types.OutputType_co],
    Generic[types.InputType_contra, types.OutputInputType, types.OutputType_co],
):
    @abstractproperty
    def _transform1_def(
        self,
    ) -> Type[protocols.PipelineTransform[types.InputType_contra, types.OutputInputType]]:
        """First transform."""

    @abstractproperty
    def _transform2_def(
        self,
    ) -> Type[protocols.PipelineTransform[types.OutputInputType, types.OutputType_co]]:
        """Second transform."""

    @abstractproperty
    def _transform1_kwargs(self) -> list[str]:
        """Kwargs used when initializing self._transform1_def."""

    @abstractproperty
    def _transform2_kwargs(self) -> list[str]:
        """Kwargs used when initializing self._transform2_def."""

    _transform1: protocols.PipelineTransform[types.InputType_contra, types.OutputInputType] = field(
        init=False
    )
    _transform2: protocols.PipelineTransform[types.OutputInputType, types.OutputType_co] = field(init=False)

    def __call__(self, inp: types.InputType_contra) -> types.OutputType_co:
        """Apply composed transform on input data."""
        res = self._transform2(self._transform1(inp))
        self._logger.debug(
            f"Applying composed transform {self._transform2_def}({self._transform1_def}) on input."
        )
        return res


@frozen(slots=False)
class _Value2dict(PipelineTransform[types.InputType, Dict[str, types.InputType]], Generic[types.InputType]):
    """Convert value to dict {key: value}."""

    key: str

    def __call__(self, value: types.InputType) -> dict[str, types.InputType]:  # TODO: use TypedDict
        return {self.key: value}


class _QueryDataSource(
    _InstantiateClassMixin,
    MultiKeyDataSource[types.InputType, types.OutputType_co],
    Generic[types.InputType, types.KeyType, types.OutputType_co],
):
    @abstractproperty
    def _datasource_def(self) -> Type[protocols.DataSource[types.KeyType, types.OutputType_co]]:
        """Original datasource."""

    @abstractproperty
    def _datasource_kwargs(self) -> list[str]:
        """Kwargs used when initializing self._datasource_def."""

    @property
    def _query_cache_def(
        self,
    ) -> Type[
        protocols.Dataset[
            types.InputType,
            types.KeyType | tuple[types.KeyType, ...],
            types.KeyType | tuple[types.KeyType, ...],
        ]
    ] | None:
        """No caching by default"""
        return None

    @property
    def _query_cache_kwargs(self) -> list[str]:
        return []

    _datasource: protocols.QueryDataSource[types.KeyType, types.OutputType_co] = field(
        init=False
    )  # TODO: validate it has a query method

    _query_cache: protocols.Dataset[
        types.InputType, types.KeyType | tuple[types.KeyType, ...], types.KeyType | tuple[types.KeyType, ...]
    ] = field(init=False)

    def __getattr__(self, item: str) -> Any:
        """Make all methods and properties from the original datasource accessible."""
        return getattr(self._datasource, item)

    def __getitem__(
        self,
        key: types.InputType | tuple[types.InputType, ...],
    ) -> types.OutputType_co | tuple[types.OutputType_co, ...]:

        result: types.OutputType_co | tuple[types.OutputType_co, ...]
        if isinstance(key, tuple):
            # multiple keys
            # TODO: what if ds is of type MultiKeyDataSource? Some optimization here?

            result = RunContext.from_obj(self).map(
                self.__getitem__, key  # type: ignore  # TODO: fix
            )  # TODO: use class_transform decorator `multikey``

        else:
            # single key
            # TODO: decide between query(key) and query(**key) based on query signature
            if self._query_cache is not None:
                try:
                    data_id = self._query_cache[key]
                except KeyError:
                    self._logger.debug(
                        f"Data for query {key} not found in cache {self._query_cache_def}. Running the query."
                    )
                    data_id = self._datasource.query(**key)  # type: ignore  # types.InputType is not a mapping
                    self._query_cache[key] = data_id
                    self._logger.debug(f"Data for query {key} saved to cache {self._query_cache_def}.")
                else:
                    self._logger.debug(f"Data for query {key} found in cache {self._query_cache_def}.")
            else:
                data_id = self._datasource.query(**key)  # type: ignore  # types.InputType is not a mapping
                self._logger.debug(f"Data for query {key} queried directly using {self._query_cache_def}.")

            result = self._datasource[data_id]  # type: ignore  # Datasource does not have query
            self._logger.debug(f"Data for query {key} loaded from data source {self._datasource_def}.")

        return result


class _CachedDataSource(
    _InstantiateClassMixin,
    MultiKeyDataSource[types.KeyType_contra, types.OutputType_co],
    Generic[types.KeyType_contra, types.InputOutputType, types.OutputType_co],
):
    """Datasource that caches its outputs."""

    @abstractproperty
    def _datasource_def(self) -> Type[protocols.DataSource[types.KeyType_contra, types.OutputType_co]]:
        """Original datasource."""

    @abstractproperty
    def _cache_def(
        self,
    ) -> Type[protocols.Dataset[types.KeyType_contra, types.InputOutputType, types.OutputType_co]]:
        """Cache datasource outputs."""

    @abstractproperty
    def _datasource_kwargs(self) -> list[str]:
        """Kwargs used when initializing self._datasource_def."""

    @abstractproperty
    def _cache_kwargs(self) -> list[str]:
        """Kwargs used when initializing self._cache_def."""

    _datasource: protocols.DataSource[types.KeyType_contra, types.OutputType_co] = field(init=False)
    _cache: protocols.Dataset[types.KeyType_contra, types.InputOutputType, types.OutputType_co] = field(
        init=False
    )
    _skip_if: Callable[[types.KeyType_contra, types.OutputType_co], bool] | None = None

    def __getattr__(self, item: str) -> Any:
        """Make all methods and properties from the original datasource accessible."""
        return getattr(self._datasource, item)

    def __getitem__(
        self, key: types.KeyType_contra | tuple[types.KeyType_contra, ...]
    ) -> types.OutputType_co | tuple[types.OutputType_co, ...]:
        """Get cached data or load them from the original data source and save to cache.

        :param key: Single index or tuple of those.
        :returns: Possibly cached data from the original DataSource.
        """
        result: types.OutputType_co | tuple[types.OutputType_co, ...]
        if isinstance(key, tuple):
            # multiple keys
            result = tuple(self[single_key] for single_key in key)  # type: ignore  # TODO: fix  # TODO: use map to paralelize

            return result

        # single key
        try:
            result = self._cache[key]
        except KeyError:
            self._logger.debug(
                f"Data for key {key} not found in cache {self._cache_def}. Loading from original data source {self._datasource_def}."
            )
            # extract
            data = self._datasource[key]
            self._logger.debug(f"Data for key {key} loaded from original data source {self._datasource_def}.")
            # load
            if self._skip_if is None or not self._skip_if(key, data):
                # load to cache
                self._cache[key] = data  # type: ignore  # TODO: fix, classical __getitem__(self, key | tuple[key, ...]) -> result | tuple[result, ...] problem
                self._logger.debug(f"Data for key {key} saved to cache {self._cache_def}.")
                result = self._cache[key]  # do not return data directly because the cache may transform them
            else:
                # skip loading to cache
                result = data
        else:
            self._logger.debug(f"Data for key {key} loaded from cache {self._cache_def}.")

        return result


class _ReduceTransform(
    _InstantiateClassMixin,
    MultiKeyDataSource[types.KeyType_contra, types.OutputType_co],
    Generic[types.KeyType_contra, types.InputOutputType, types.OutputType_co],
):
    # TODO: ReduceTransform can be applied also on the transform, not only on dataset - implement
    @abstractproperty
    def _datasource_def(
        self,
    ) -> Type[protocols.MultiKeyDataSource[types.KeyType_contra, types.InputOutputType]]:
        """Original datasource."""

    @abstractproperty
    def _transform_def(
        self,
    ) -> Type[
        protocols.PipelineTransform[
            types.InputOutputType | tuple[types.InputOutputType, ...],
            types.OutputType_co | tuple[types.OutputType_co, ...],
        ]
    ]:
        """First transform."""

    @abstractproperty
    def _datasource_kwargs(self) -> list[str]:
        """Kwargs used when initializing self._datasource_def."""

    @abstractproperty
    def _transform_kwargs(self) -> list[str]:
        """Kwargs used when initializing self._transform_def."""

    _datasource: protocols.MultiKeyDataSource[types.KeyType_contra, types.InputOutputType] = field(
        init=False
    )  # TODO: require DataSource and convert to MultiKey here
    _transform: protocols.PipelineTransform[
        types.InputOutputType | tuple[types.InputOutputType, ...],
        types.OutputType_co | tuple[types.OutputType_co, ...],
    ] = field(init=False)

    def __getitem__(
        self, key: types.KeyType_contra | tuple[types.KeyType_contra, ...]
    ) -> types.OutputType_co | tuple[types.OutputType_co, ...]:
        """Aply reduce transform on input data source."""
        data = self._datasource[key]
        self._logger.debug(f"Data for key {key} loaded from original source {self._datasource_def}.")
        res = self._transform(data)
        self._logger.debug(f"Data for key {key} transformed by {self._transform_def}.")
        return res
