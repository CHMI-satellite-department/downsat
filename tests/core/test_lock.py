from typing import TYPE_CHECKING
from pathlib import Path
import threading
import time

import pytest
from pytest_cases import fixture_union


if TYPE_CHECKING:
    from downsat.core.lock import DiskCacheRLock, FileRLock, LockBase


@pytest.fixture
def diskcache_rlock(tmp_path: Path) -> "DiskCacheRLock":
    from downsat.core.lock import DiskCacheRLock

    lock_path = tmp_path / "test_lock"
    lock_key = "__test_lock__"
    return DiskCacheRLock(lock_path, lock_key)


@pytest.fixture
def filelock_rlock(tmp_path: Path) -> "FileRLock":
    from downsat.core.lock import FileRLock

    lock_path = tmp_path / "test_lock"
    lock_key = "__test_lock__"
    return FileRLock(lock_path, lock_key)


lock = fixture_union("lock", [diskcache_rlock, filelock_rlock], ids=["diskcache", "filelock"])


def test_call_method(lock: "LockBase") -> None:
    new_key = "__new_lock__"
    new_lock = lock(new_key)
    assert new_lock.lock_key == new_key


def test_context_manager(lock: "LockBase") -> None:
    with lock:
        # works
        pass


def test_lock(lock: "LockBase") -> None:
    def worker() -> None:
        with lock:
            time.sleep(1)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    start_time = time.time()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    end_time = time.time()
    # Ensure the total time is at least double the sleep time of each worker, indicating sequential execution.
    assert end_time - start_time >= 2


def test_reentrant_lock(lock: "LockBase") -> None:
    """Test the lock is reentrant."""

    with lock:
        with lock:
            # can get here with the same lock
            pass

    lock2 = lock.__class__(lock.lock_path, lock.lock_key)

    with lock:
        with lock2:
            # can get here with different instances of the same lock
            pass
