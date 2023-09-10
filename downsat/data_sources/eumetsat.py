from __future__ import annotations

from typing import Any
from functools import partial
from pathlib import Path

from attrs import field, frozen
from diskcache import Cache

from downsat.clients.eumdac import EumdacCollection, EumdacKey
from downsat.core.cache import LockableCache
from downsat.core.file_storage import FileDataset
from downsat.core.lock import DiskCacheRLock, FileRLock
from downsat.core.utils import TimeSlotType
from downsat.etl import protocols
from downsat.etl.class_transforms import cache, query, reduce
from downsat.etl.context import setcontext
from downsat.etl.transforms import Filter, Flatten, UnzipBuffer


@frozen(slots=False)
class _EumdacArchiveFactory:
    """Factory for Eumdac archive datasets."""

    name: str = field()

    def from_env(
        self, **kwargs: Any
    ) -> protocols.MultiKeyDataSource[TimeSlotType, Path] | protocols.MultiKeyDataSource[
        TimeSlotType, list[Path]
    ]:
        """Create a new instance of eumdac archive from environment variables."""
        from downsat.core.config import get_platform  # import here to prevent circular import

        return get_platform().get(self.name, kwargs)

    @property
    def __name__(self) -> str:
        # TODO: this is just a hack to allow using this class with Platform
        #       => remove when Platform or _EumdacArchiveFactory is refactored
        return self.name

    def __call__(
        self,
        credentials: EumdacKey,  # TODO: this should actually be EumdacCredentials, but platform cannot build base class => use EumdacKey for now and add this capability to Platform
        data_path: Path | str,
        *,
        num_workers: int = 0,
        network_filesystem: bool = True,
        flatten: bool = True,
        **query_params: Any,
    ) -> protocols.MultiKeyDataSource[TimeSlotType, Path] | protocols.MultiKeyDataSource[
        TimeSlotType, list[Path]
    ]:
        """Build cached eumdac dataset class.

        :param credentials: Eumdac credentials to be used.
        :param data_path: Path for the file cache.
        :param num_workers: Number of parallel connections to be used. 0 means serial processing.
        :param flatten: Whether to flatten the result of the query. If False, the result is a tuple of tuples.
            If True, the result is a flat tuple of filenames. If the query returns a single file, the result is a single filename.
        :param network_filesystem: Whether to use lock compatible with NFS network filesystem, but less efficient, (default, True) or
            more efficient lock that may fail on network filesystems like NFS and cause database corruption (False).
            The reason why not to use efficient diskcache by default is incompatibility of SQLite with network filesystems.
            **Important:** All processes that access the same cache must use the same value of this parameter otherwise
            database corruption may occur.
        :param **query_params: Extra query parameters to be passed to the eumdac client, such as region etc.
            Donwsat adds some special query parameters on top of those provided by eumdac such as `point` to specify
            single lon-lat point that should be included in the data or `area` to specify satpy area definition
            (string or AreaDefinition) that sould be at least partly covered by the data.
        """
        # TODO: transform to class
        # TODO: bind methods `search_options` and `query`

        # Note: FileDataset returns (Path, ...) but query requests a tuple ds[(data_id, )] => CachedEumdacCollection returns ((Path, ...), )
        # => Flatten at the end to get back (Path, ...)

        if network_filesystem:
            lock_class = FileRLock
            cache_class = LockableCache
        else:
            lock_class = DiskCacheRLock  # type: ignore  # TODO: fix
            cache_class = Cache  # type: ignore  # TODO: fix

        file_cache = partial(
            FileDataset, lock_class=lock_class, cache_class=cache_class, filename_pattern="{name}"
        )
        CachedEumdacCollection = cache(EumdacCollection >> UnzipBuffer, cache=file_cache)  # type: ignore  # TODO: why? fix
        DataFilenames = CachedEumdacCollection >> Filter(
            lambda filename, metadata: filename.stem == metadata["name"]  # type: ignore  # object has no attribute 'stem'
        )
        TimeIndexedEumdacCollection = query(
            DataFilenames,
            by=EumdacCollection._datetime2eumdac_query_params,
            cache=cache_class(Path(data_path) / ".query_diskcache"),
        )
        # Note: query returns (([Path, ...], ), ([Path, ...], ), ...) but we want to return a flat list of unique filenmes => flatten twice
        # or once to get unflattened ([Path, ...], [Path, ...])
        flatten_depth = 2 if flatten else 1
        FlatSource = reduce(TimeIndexedEumdacCollection, partial(Flatten, depth=flatten_depth))  # type: ignore  # TODO: fix - mypy plugin

        flat_source = FlatSource(name=self.name, credentials=credentials, query_params=query_params, data_path=data_path)  # type: ignore  # TODO: fix. **{"name": name, ...} would work
        setcontext(num_workers=num_workers)(flat_source)

        return flat_source


# default datasets for the most common collections
MSG = _EumdacArchiveFactory("MSG")
RSS = _EumdacArchiveFactory("RSS")
Metop = _EumdacArchiveFactory("METOP")
