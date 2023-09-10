from typing import TypeVar
from collections.abc import MutableMapping

from typing_extensions import ParamSpec  # TODO: drop once Python 3.9 is not supported anymore


Params = ParamSpec("Params")

CacheType = TypeVar("CacheType", bound=MutableMapping)
ClassType = TypeVar("ClassType")
DataSourceType = TypeVar("DataSourceType")
InputType = TypeVar("InputType")
InputType_contra = TypeVar("InputType_contra", contravariant=True)
InputOutputType = TypeVar("InputOutputType")
Instance = TypeVar("Instance")
ItemType = TypeVar("ItemType")
KeyType = TypeVar("KeyType")
KeyType_co = TypeVar("KeyType_co", covariant=True)
KeyType_contra = TypeVar("KeyType_contra", contravariant=True)
LockType = TypeVar("LockType")
LockType_co = TypeVar("LockType_co", covariant=True)
OutputType = TypeVar("OutputType")
OutputType_co = TypeVar("OutputType_co", covariant=True)
OutputInputType = TypeVar("OutputInputType")
SaveableType = TypeVar("SaveableType")
ValueType = TypeVar("ValueType")
