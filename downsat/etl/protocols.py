from __future__ import annotations

from typing import Any, Protocol, runtime_checkable
from types import TracebackType

from downsat.etl import types


@runtime_checkable
class DataSource(Protocol[types.KeyType_contra, types.OutputType_co]):
    """Datasource indexed by a single key."""

    def __getitem__(self, key: types.KeyType_contra) -> types.OutputType_co:
        """Return item.

        :param key: Index or id of the item.
        :returns: Item.
        :raises KeyError: The item is not present.
        """


@runtime_checkable
class MultiKeyDataSource(Protocol[types.KeyType_contra, types.OutputType_co]):
    """DataSource that be indexed by multiple keys."""

    def __getitem__(
        self, key: types.KeyType_contra | tuple[types.KeyType_contra, ...]
    ) -> types.OutputType_co | tuple[types.OutputType_co, ...]:
        """Return one or more items.

        :param key: Index or id of the item or tuple of those.
        :returns: Item or items.
        :raises KeyError: At least one of the items is not present.
        """


@runtime_checkable
class HasQueryProtocol(Protocol[types.KeyType_co]):
    """Class with query method."""

    def query(
        self, **kwargs: Any
    ) -> tuple[types.KeyType_co, ...]:  # TODO: unify output signature with eumdac
        """Query item, return their ids."""


@runtime_checkable
class QueryDataSource(
    DataSource[types.KeyType, types.OutputType_co],
    HasQueryProtocol[types.KeyType],
    Protocol[types.KeyType, types.OutputType_co],
):
    """Datasource with query method"""


@runtime_checkable
class MultiKeyQueryDataSource(
    MultiKeyDataSource[types.KeyType, types.OutputType_co],
    HasQueryProtocol[types.KeyType],
    Protocol[types.KeyType, types.OutputType_co],
):
    """Datasource with query method"""


# Dataset


@runtime_checkable
class Dataset(
    DataSource[types.KeyType_contra, types.OutputType_co],
    Protocol[types.KeyType_contra, types.InputType_contra, types.OutputType_co],
):
    """Dataset indexed by a single key."""

    def __setitem__(self, key: types.KeyType_contra, value: types.InputType_contra) -> None:
        """Set new item."""


# single-input transform
@runtime_checkable
class PipelineTransform(Protocol[types.InputType_contra, types.OutputType_co]):
    def __call__(self, inp: types.InputType_contra) -> types.OutputType_co:
        """Transform inp."""


@runtime_checkable
class MultiKeyDataset(
    MultiKeyDataSource[types.KeyType_contra, types.OutputType_co],
    Protocol[types.KeyType_contra, types.InputType_contra, types.OutputType_co],
):
    """Dataset that can accept multiple keys."""

    def __setitem__(self, key: types.KeyType_contra, value: types.InputType_contra) -> None:
        """Set new item."""


@runtime_checkable
class Lock(Protocol[types.LockType_co]):
    """Lockable object."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize lock."""

    @property
    def lock(self) -> types.LockType_co:
        """Lock object."""

    def __call__(self, lock_key: str) -> Lock[types.LockType_co]:
        """Return new lock with different lock_key."""

    def __enter__(self) -> None:
        """Use lock as context manager."""
        return self.lock.__enter__()  # type: ignore  # TODO: fix

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Use lock as context manager."""
        return self.lock.__exit__(exc_type, exc_value, traceback)  # type: ignore  # TODO: fix
