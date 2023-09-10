from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from attrs import evolve, field, frozen
import diskcache
import filelock

from downsat.etl import protocols, types


@frozen
class LockBase(protocols.Lock[types.LockType_co], ABC):
    """Base class for locks.

    :param lock_path: Path to the diskcache.
    :param lock_key: Key of the lock in the diskcache. Optional, default is '__default_lock__'.
    """

    lock_path: Path = field(converter=Path)
    lock_key: str = field(default="__default_lock__", converter=str)
    lock: types.LockType_co = field(init=False)

    def __attrs_post_init__(self) -> None:
        """Initialize lock."""
        object.__setattr__(self, "lock", self._create_lock())

    def __call__(self, lock_key: str) -> LockBase[types.LockType_co]:
        """Return new lock with different lock_key."""
        return evolve(self, lock_key=lock_key)

    @abstractmethod
    def _create_lock(self) -> types.LockType_co:
        """Create lock."""
        raise NotImplementedError


@frozen
class DiskCacheRLock(LockBase[diskcache.RLock]):
    """Reentrant lock using diskcache.

    :param lock_path: Path to the diskcache.
    :param lock_key: Key of the lock in the diskcache. Optional, default is '__default_lock__'.

    Example:
        >>> from downsat.core.lock import DiskCacheRLock
        >>> lock = DiskCacheRLock("path/to/diskcache")
        >>> with lock:
        ...     # do something
        >>> with lock("another_lock"):
        ...     # do something else
    """

    lock: diskcache.RLock = field(init=False)

    def _create_lock(self) -> diskcache.RLock:
        """Create lock."""
        with diskcache.Cache(self.lock_path) as cache:
            return diskcache.RLock(cache, self.lock_key)


_file_rlocks: dict[Path, filelock.FileLock] = {}


@frozen
class FileRLock(LockBase[filelock.FileLock]):
    """Reentrant lock using filelock.

    :param lock_path: Path to the lock file.
    :param lock_key: Key of the lock in the diskcache. Optional, default is '__default_lock__'.
    :param timeout: Timeout in seconds. Optional, default is 30 seconds.

    Example:
        >>> from downsat.core.lock import FileRLock
        >>> lock = FileRLock("path/to/lock_file")
        >>> with lock:
        ...     # do something
        >>> with lock("another_lock"):
        ...     # do something else
    """

    timeout: int = field(default=30, converter=int, kw_only=True)

    def _create_lock(self) -> filelock.FileLock:
        """Initialize lock.

        The same lock object is always returend for the same lock_path and lock_key.
        This behavior makes the lock reentrant.

        :returns: Lock object.
        """
        lock_path = self.lock_path / f"{self.lock_key}.lock"
        try:
            return _file_rlocks[lock_path]
        except KeyError:
            lock_path.parent.mkdir(parents=True, exist_ok=True)

            lock = filelock.FileLock(lock_path, timeout=self.timeout)
            _file_rlocks[lock_path] = lock
            return lock
