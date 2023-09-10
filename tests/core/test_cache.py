from typing import TYPE_CHECKING
from pathlib import Path

import pytest


if TYPE_CHECKING:
    from downsat.core.cache import LockableCache


@pytest.fixture
def file_locked_disk_cache(tmp_path: Path) -> "LockableCache":
    """Return LockableCache."""
    from downsat.core.cache import LockableCache

    return LockableCache(tmp_path)


def test_lockable_cache(file_locked_disk_cache: "LockableCache") -> None:
    """Test LockableCache works."""

    from multiprocessing import Process
    from threading import Thread

    cache = file_locked_disk_cache
    cache["key"] = "value"
    assert cache["key"] == "value"
    cache["key"] = "value2"
    assert cache["key"] == "value2"

    # test that cache works in multihreaded environment
    def worker_func(cache: "LockableCache") -> None:
        """Thread function."""
        cache["key"] = "value"
        assert cache["key"] == "value"
        cache["key"] = "value2"
        assert cache["key"] == "value2"

    threads = []
    for _ in range(10):
        thread = Thread(target=worker_func, args=(cache,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    assert cache["key"] == "value2"

    # test that it works in multiprocessing environment
    processes = []
    for _ in range(10):
        process = Process(target=worker_func, args=(cache,))
        process.start()
        processes.append(process)
    for process in processes:
        process.join()
    assert cache["key"] == "value2"
