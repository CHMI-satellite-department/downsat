from __future__ import annotations

from typing import Optional, Protocol, TypeVar, Union, runtime_checkable
import datetime

from downsat.etl import protocols


DataType = TypeVar("DataType")
InputType = TypeVar("InputType")
InputType_contra = TypeVar("InputType_contra", contravariant=True)
KeyType = TypeVar("KeyType")
KeyType_co = TypeVar("KeyType_co", covariant=True)
KeyType_contra = TypeVar("KeyType_contra", contravariant=True)
OrigKeyType = TypeVar("OrigKeyType")
OutputType_co = TypeVar("OutputType_co", covariant=True)
OutputType = TypeVar("OutputType")

TimeType = Union[str, datetime.datetime]
TimeRangeType = Union[TimeType, slice]
TimeSlotType = Optional[TimeRangeType]


@runtime_checkable
class MultiKeyDataset(
    protocols.MultiKeyDataSource[KeyType_contra, OutputType_co],
    Protocol[KeyType_contra, InputType_contra, OutputType_co],
):
    """Dataset that can accept multiple keys."""

    def __setitem__(self, key: KeyType_contra, value: InputType_contra) -> None:
        """Set new item."""


@runtime_checkable
class Dataset(
    protocols.DataSource[KeyType_contra, OutputType_co],
    Protocol[KeyType_contra, InputType_contra, OutputType_co],
):
    """Dataset indexed by a single key."""

    def __setitem__(self, key: KeyType_contra, value: InputType_contra) -> None:
        """Set new item."""


@runtime_checkable
class Transform(Protocol[InputType_contra, OutputType_co]):
    """Data transform."""

    def __call__(self, input: InputType_contra) -> OutputType_co:
        """Transform input to output."""


@runtime_checkable
class MultiItemTransform(Protocol[InputType_contra, OutputType_co]):
    """Data transform accepting multiple items."""

    def __call__(
        self, input: InputType_contra | tuple[InputType_contra, ...]
    ) -> OutputType_co | tuple[OutputType_co, ...]:
        """Transform input to output."""
