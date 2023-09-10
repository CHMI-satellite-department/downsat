from __future__ import annotations

from typing import Any, Generic
from pathlib import Path

from attrs import field, frozen, validators
from diskcache import Cache

from downsat.core.lock import FileRLock
from downsat.etl import types


@frozen
class LockableCache(Generic[types.CacheType, types.LockType]):
    """Cache with lock.

    :param path: Path to the diskcache.
    :param timeout: Timeout in seconds. Optional, default is 30 seconds.
    """

    # TODO: parametrize key and value types using Gneric

    path: Path = field(converter=Path)
    timeout: int = field(converter=int, default=30, validator=validators.ge(0))
    _lock_class: type[types.LockType] = field(default=FileRLock, kw_only=True)  # type: ignore  # TODO: fix, why?
    _cache_class: type[types.CacheType] = field(default=Cache, kw_only=True)  # type: ignore  # TODO: fix, why?
    lock: FileRLock = field(init=False)
    _lock_path: Path = field(init=False)

    def __attrs_post_init__(self) -> None:
        """Initialize lock and cache."""
        lock_path = self.path / ".lock"
        object.__setattr__(self, "_lock_path", lock_path)
        lock = self._lock_class(self._lock_path, timeout=self.timeout)  # type: ignore  # TODO: fix, why?
        object.__setattr__(self, "lock", lock)

    def __getitem__(self, key: str) -> Any:
        """Get item."""
        with self.lock:
            with self._cache_class(self.path) as cache:  # type: ignore  # TODO: fix
                return cache[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """Set item."""
        with self.lock:
            with self._cache_class(self.path) as cache:  # type: ignore  # TODO: fix
                cache[key] = value

    def __delitem__(self, key: str) -> None:
        """Delete item."""
        with self.lock:
            with self._cache_class(self.path) as cache:  # type: ignore  # TODO: fix
                del cache[key]
