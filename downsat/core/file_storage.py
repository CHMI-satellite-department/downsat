from __future__ import annotations

from typing import Any, Collection, Generic, Optional
from abc import ABC, abstractproperty
from functools import cached_property
import glob
from pathlib import Path
import shutil

from attrs import Attribute, converters, field, frozen, validators
from trollsift import Parser

from downsat.core.cache import LockableCache
from downsat.core.lock import FileRLock, LockBase
from downsat.core.utils import is_relative_to
from downsat.etl import protocols, types
from downsat.etl.class_transforms import multikey_ds
from downsat.etl.converters import MetaPathOrMetaPathCollection, to_filesystem, to_parser
from downsat.etl.metadata import getmeta, setmeta
from downsat.etl.weakref import Path as MetaPath


# TODO: consider adding compression and encryption support


class InvalidFilenameError(ValueError):
    """Raised when a filename is not valid within the dataset."""


@frozen(slots=False)
class FileLockMixin(ABC, Generic[types.LockType_co]):
    """Abstract mixin class with lock functionality."""

    _lock_class: type[LockBase] = field(default=FileRLock, kw_only=True)

    @abstractproperty
    def _lock_path(self) -> Path:
        """Path where to store the locks."""

    def lock(self, lock_key: str = "__lock__") -> protocols.Lock[types.LockType_co]:
        """Lock object."""
        return self._lock_class(self._lock_path, lock_key)


@frozen(slots=False)
class MetadataCacheMixin(ABC, Generic[types.CacheType]):
    """Abstract mixin class with metadata cache functionality."""

    _cache_class: type[types.CacheType] = field(default=LockableCache, kw_only=True)  # type: ignore  # TODO: why? fix

    @abstractproperty
    def _diskcache_path(self) -> Path:
        """Path where to store the cache."""

    @property
    def _diskcache(self) -> types.CacheType:
        """Diskcache Cache object."""
        return self._cache_class(self._diskcache_path)  # type: ignore  # TODO: why? fix


@frozen(slots=False)
class PathMetaCache:
    """Metadata cache that stores paths relative to given data_path as keys.

    Storing the relative paths allows to move the dataset to a different location.

    :param data_path: Path to the folder were data are to be saved.
    """

    data_path: Path = field(converter=Path)
    diskcache: protocols.Dataset[str | Path | MetaPath, Any, Any] = field()

    def __getitem__(self, key: str | Path | MetaPath) -> Any:
        """Get metadata for the given Path."""
        try:
            relative_key = str(Path(key).relative_to(self.data_path))
        except ValueError:
            raise KeyError(f"Key {key} is not relative to data path {self.data_path}.")

        return self.diskcache[relative_key]

    def __setitem__(self, key: str | Path | MetaPath, value: Any) -> None:
        try:
            relative_key = str(Path(key).relative_to(self.data_path))
        except ValueError:
            raise KeyError(f"Key {key} is not relative to data path {self.data_path}.")

        self.diskcache[relative_key] = value

    def __delitem__(self, key: str | Path | MetaPath) -> None:
        try:
            relative_key = str(Path(key).relative_to(self.data_path))
        except ValueError:
            raise KeyError(f"Key {key} is not relative to data path {self.data_path}.")

        del self.diskcache[relative_key]  # type: ignore  # TODO: fix


@frozen(slots=False)
class _FileDataset(
    FileLockMixin[types.LockType_co],
    MetadataCacheMixin[types.CacheType],
    # Generic[types.OutputType, types.LockType_co, types.CacheType],  # TODO: fix multikey_ds such that it works with this Generic
):
    """Class that can store objects into a filesystem.

    Single objects are stored as files, collections of objects such as mappings are stored as
    directories with mapping keys use as filenames in the directory.

    :param data_path: Path to the folder were data are to be saved.
    :param filename_pattern: Template of the filename. If present, it is used to extract metadata from the filename and
        to verify that the stored filenames, i.e. the keys, follow the pattern.
    :param forbidden_paths: Paths that are forbidden for saving files. If a file is to be saved
        in a forbidden path, a KeyError is raised.
    :param lock_name: Name of the folder where to store locks.
    :param cache_name: Name of the folder where to store the cache.
    """

    data_path: Path = field(converter=Path)

    @data_path.validator
    def _check_data_path(self, attribute: Attribute, value: Path) -> None:  # noqa: U100
        """Check that data_path exists and is a directory. Makedir if self.mkdir is True."""
        if self.mkdir:
            value.mkdir(parents=True, exist_ok=True)

        if not value.exists():
            raise ValueError(f"Data path {value} does not exist.")
        if not value.is_dir():
            raise ValueError(f"Data path {value} is not a directory.")

    mkdir: bool = field(default=True, kw_only=True)
    filename_pattern: Parser | None = field(
        converter=converters.optional(to_parser), default=None, kw_only=True  # type: ignore # TODO: fix type ignore # TODO: add validator
    )

    @filename_pattern.validator
    def _check_pattern_subfolders(self, attribute: Attribute, value: Parser) -> None:  # noqa: U100
        """Check that filename_pattern does not contain subfolders."""
        if value is not None:
            if len(Path(value.fmt).parts) > 1:
                raise NotImplementedError("Filename pattern cannot contain subfolders.")

    forbidden_paths: tuple[Path, ...] = field(
        converter=tuple,
        default=(),
        kw_only=True,
        validator=validators.deep_iterable(
            member_validator=validators.instance_of(Path), iterable_validator=validators.instance_of(tuple)
        ),
    )  # TODO: add validator

    _lock_name: str = field(default="__locks__", kw_only=True)
    _cache_name: str = field(default="__diskcache__", kw_only=True)

    # -- end of attrs --

    @property
    def _lock_path(self) -> Path:
        """Path to the folder where locks are stored."""
        return self.data_path / self._lock_name

    @property
    def _diskcache_path(self) -> Path:
        """Path where to store the cache."""
        return self.data_path / self._cache_name

    @cached_property
    def _metacache(self) -> protocols.Dataset[str | Path | MetaPath, Any, Any]:
        """Metadata cache."""
        return PathMetaCache(data_path=self.data_path, diskcache=self._diskcache)

    @cached_property
    def _parameters(self) -> set[str]:
        """Extract parameters from filename_pattern.

        :returns: Set of valid parameters. Empty set if filename_pattern i snot set
        """
        if self.filename_pattern is not None:
            return set(self.filename_pattern.keys())
        else:
            return set()

    def _key_to_metadata(self, key: str) -> dict[str, Any]:
        """Extract metadata from filename using `self.filename_pattern` and load metadata from metadata cache.

        :param filename: Filename.
        :returns: Metadata extracted from the filename.
        """
        filename = self.data_path / key

        # get metadata from global cache
        try:
            metadata = self._metacache[filename]
        except KeyError:  # TODO: replace with self._diskcache.get(...)
            metadata = {}

        # no filename_pattern -> no metadata
        if self.filename_pattern is None:
            return metadata

        # extract metadata from filename
        pattern_metadata = self.filename_pattern.parse(key)  # TODO: may raise exception - handle it

        return metadata | pattern_metadata  # TODO: what if there's conflict of global and local metadata?

    def _save(self, key: str, value: Any) -> MetaPathOrMetaPathCollection:
        """Save value to a file.

        Uses singledispatch function `to_filesystem` to save the value to a file.

        :param filename: Filename.
        :param value: Value to be saved.
        :returns: Path to the saved file.
        :raises ValueError: Even after filling in all metadata, the key containes unspecified placeholders.
        :raises InvalidFilenameError: Filename is not valid.
        :raises Exception: Any exception raised by `to_filesystem`.
        """
        filename = self.data_path / Path(key)

        # check
        if not self._is_valid_filename(filename, full=True):
            raise InvalidFilenameError(f"{filename} is not a valid filename.")

        # save data
        try:
            result = to_filesystem(value, filename, mkdir=True, overwrite=True, metacache=self._metacache)
        except Exception:
            self._remove_files(filename)
            raise

        return result

    def _is_valid_filename(self, filename: Path, full: bool = True) -> bool:
        """Check if the file is valid.

        Deems files stored in forbidden directories and forbidden filenames as invalid.

        :param filename: Filename.
        :param full: If True, the filename contains full path, otherwise it is relative to `self.data_path`.
        :returns: True if the file is valid.
        """
        if not full:
            filename = self.data_path / filename
        else:
            if not is_relative_to(filename, self.data_path):
                return False

        return all(
            not is_relative_to(filename, path)
            for path in self.forbidden_paths + (self._lock_path, self._diskcache_path)
        )

    def _remove_files(
        self, filenames: str | Path | MetaPath | Collection[str | Path | MetaPath], relative: bool = False
    ) -> None:
        """Remove given files.

        :param filenames: Filename(s) to be removed.
        """
        if isinstance(filenames, (str, Path, MetaPath)):
            filenames = [filenames]

        for filename in filenames:
            filename = Path(filename)
            if relative:
                filename = self.data_path / filename

            # remove file metadata
            try:
                del self._metacache[filename]  # type: ignore  # TODO: fix
            except KeyError:
                pass
            except ValueError:
                # .relative_to did not work
                pass

            try:
                if filename.is_file():
                    Path(filename).unlink(missing_ok=True)
                else:
                    shutil.rmtree(filename, ignore_errors=True)
            except Exception as e:
                raise RuntimeError(f"Could not remove key `{filename}`.") from e

    def _validate_key(self, key: str, metadata: Optional[dict[str, Any]] = None) -> None:
        """Raise KeyError if key is invalid or not consistent with the filename_pattern.

        Filename_pattern is validated only if set and metadata is not None.

        :param key: Key to be validated.
        :param metadata: Metadata to be used for validation.
        :raises KeyError: Key is invalid.
        """
        if not key:
            raise KeyError("Empty key.")

        if self.filename_pattern is not None and metadata is not None:
            expected_key = self.filename_pattern.compose(metadata)

            if key != expected_key:
                raise KeyError(
                    f"Key {key} is not consistent with the filename_pattern {self.filename_pattern}. Expected key {expected_key}."
                )

    def __getitem__(
        self, key: str
    ) -> set[MetaPath]:  # TODO: should be set[types.OutputType]: once multikey_ds is fixed
        """Get filename belonging to the key.

        :param key: Key.
        :returns: Filename belonging to the key.
        :raises KeyError: Key does not exist.
        :raises RuntimeError: More than one file consistent with the key found.
        """
        self._validate_key(key)

        with self.lock(key):
            filename_parser = Parser(key)
            if len(filename_parser.keys()) == 0:
                # key is directly the filename
                filename = MetaPath(self.data_path / key)

                if not filename.exists():
                    raise KeyError(f"{key}")

                if filename.is_dir():
                    # get files in the directory
                    filenames = list([MetaPath(path) for path in filename.rglob("*")])
                else:
                    # this is the file
                    filenames = [filename]

                key_metadata = self._key_to_metadata(key)
            else:
                # partial key -> we need to search for the filename(s)
                filename_mask = filename_parser.globify()
                filenames = [
                    MetaPath(filename) for filename in glob.glob(str(self.data_path / filename_mask))
                ]

                if len(filenames) == 0:
                    raise KeyError(f"{key}")

                # the result may be composed of multiple keys -> return intersection of metadata of all involved keys
                key_metadata_set = None
                for filename in filenames:
                    # key is a file or directory stored directly in data_path
                    key = str(filename.relative_to(self.data_path).parts[0])
                    metadata = self._key_to_metadata(key)
                    if key_metadata_set is None:
                        key_metadata_set = set(metadata.items())
                    else:
                        key_metadata_set &= set(metadata.items())

                key_metadata = dict(key_metadata_set)  # type: ignore  # we know there was at least one filename

            # set metadata for individual paths
            result_set = set(filenames)

            for path in result_set:
                try:
                    setmeta(path, **self._metacache[path])
                except KeyError:
                    pass

            # set metadata for the whole set
            setmeta(result_set, **key_metadata)

        return result_set

    def __setitem__(self, key: str, value: Any) -> None:
        """Save data of a single key.

        Name of the file is given by the key. If self.filename_pattern is not None, the
        key is checked to be consistent with the filename_pattern given value metadata.
        If it is not not, ValueError is raised.

        :param key: Key.
        :param value: Data to be saved.
        :raises ValueError: Key is not consistent with the filename_pattern.
        """
        # compose key and filename from metadata
        metadata = getmeta(value)
        key = Parser(key).compose(metadata)
        if len(Parser(key).keys()) > 0:
            raise ValueError(f"Key {key} contains unspecified metadata.")

        self._validate_key(key, metadata)

        with self.lock(key):
            # remove previous data
            try:
                del self[key]
            except KeyError:
                pass

            # set the new data
            try:
                self._save(key, value)
            except Exception as e:
                self._remove_files(key)
                raise KeyError(f"{key}") from e

    def __delitem__(self, key: str) -> None:
        """Remove key and delete all files associated with it.

        :param key: Key to be removed.
        :raises KeyError: Key does not exist.
        """
        self._validate_key(key)

        with self.lock(key):
            filename_parser = Parser(key)
            if len(filename_parser.keys()) == 0:
                # delete single key
                filenames = [self.data_path / Path(key)]

                if not filenames[0].exists():
                    raise KeyError(f"{key}")
            else:
                # delete keys matching the pattern
                filename_pattern = str(self.data_path / filename_parser.globify())
                filenames = [Path(filename) for filename in glob.glob(filename_pattern)]

                if len(filenames) == 0:
                    raise KeyError(f"{key}")

            self._remove_files(filenames)

    def search(self, **kwargs: Any) -> set[str]:
        """Search the database, return set of keys that match the kwargs.

        **Important**: At this moment, only exact matches of parameters defined in the
        filename_pattern are supported.

        TODO: Implement search over metadata cache.

        :param kwargs: Search parameters. Keys are metadata names and values are metadata values.
            Non-existent metadata names raise ValueError.
        :returns: Set of keys stored in the storage that match the search criteria.
        :raises ValueError: Search contains non-existent metadata names.
        :raises NotImplementedError: Search is not supported without filename_pattern.
        """
        if self.filename_pattern is None:
            raise NotImplementedError(
                "Search is not yet supported without filename_pattern."
            )  # TODO: search metadata cache

        if not set(kwargs.keys()).issubset(self._parameters):
            raise ValueError("Search contains parameter names not defined in filename_pattern.")

        query_pattern = self.filename_pattern.globify(kwargs)

        # find all files matching the pattern
        paths = glob.glob(str(self.data_path / query_pattern))
        # key is equal to the first part of the path relative to the data_path
        keys = set(str(Path(path).relative_to(self.data_path).parts[0]) for path in paths)

        return keys

    def add(self, item: Any, overwrite: bool = True) -> str:
        """Add item to the dataset.

        :param item: Item to be added. It's key is generated automatically based on metadata.
        :param overwrite: If True, overwrite existing item with the same key.
        :returns: Key of the item.
        """

        if self.filename_pattern is None:
            raise NotImplementedError(
                "Adding items is not yet supported without filename_pattern."
            )  # TODO: generate random key

        # generate key from metadata
        metadata = getmeta(item)
        key = self.filename_pattern.compose(metadata)

        # save the item
        with self.lock(key):
            if key in self and not overwrite:
                raise KeyError(f"Key {key} already exists.")

            self[key] = item

        return key

    def keys(self) -> list[str]:
        """Return set of keys in the dataset."""
        file_candidates = list(self.data_path.glob("*"))

        return [
            str(file.relative_to(self.data_path))
            for file in file_candidates
            if self._is_valid_filename(file, full=True)
        ]

    def __len__(self) -> int:
        """Return number of keys in the dataset."""
        return len(self.keys())

    def __contains__(self, key: str) -> bool:
        """Return True if the key exists in the dataset."""
        return (self.data_path / key).exists()


FileDataset = multikey_ds(_FileDataset)
