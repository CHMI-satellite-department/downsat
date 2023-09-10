from __future__ import annotations

from typing import Any, Optional, Union
import atexit
from collections.abc import Mapping
from functools import singledispatch
from io import BytesIO, StringIO
import os
from pathlib import Path
import shutil
import tempfile


try:
    from trollimage.xrimage import XRImage
except ImportError:
    satpy_installed = False
else:
    satpy_installed = True
from trollsift import Parser

from downsat.etl.metadata import getmeta, keepmeta
from downsat.etl.protocols import Dataset
from downsat.etl.weakref import List as MetaList
from downsat.etl.weakref import Path as MetaPath


MetaPathOrMetaPathCollection = Union[
    MetaPath,
    MetaList["MetaPathOrMetaPathCollection"],
    tuple["MetaPathOrMetaPathCollection", ...],
    set["MetaPathOrMetaPathCollection"],
]


@singledispatch
def to_stringio(input: Any) -> StringIO:
    """Convert input to StringIO.

    :param input: Input to be converted.
    :return: StringIO.
    """
    raise NotImplementedError(f"Cannot convert {type(input)} to StringIO.")


@to_stringio.register
@keepmeta
def _(input: str) -> StringIO:
    """Convert string to StringIO.

    :param input: Input to be converted.
    :return: StringIO.
    """
    return StringIO(input)


@to_stringio.register
def _(input: StringIO) -> StringIO:
    """Convert StringIO to StringIO.

    :param input: Input to be converted.
    :return: StringIO.
    """
    return input


@to_stringio.register
@keepmeta
def _(input: list) -> StringIO:
    """Convert list to StringIO.

    Converts each element of the list to StringIO and concatenates them.

    :param input: Input to be converted.
    :return: StringIO.
    """
    return StringIO("\n".join(to_stringio(v).getvalue() for v in input))


@singledispatch
def to_path(input: Any) -> MetaPathOrMetaPathCollection:
    """Convert input to Path .

    :param input: Input to be saved/converted to Path.
    :return: Path or tuple or list of those.
    """
    raise NotImplementedError(f"Cannot convert {type(input)} to Path.")


@to_path.register
def _(input: MetaPath) -> MetaPath:
    """Convert Path to Path.

    :param input: Input to be converted to Path.
    :return: Path.
    """
    return input


@to_path.register
@keepmeta
def _(input: Path) -> MetaPath:
    """Convert Path to Path with attachable attributes.

    :param input: Input to be converted to Path with attachable attributes.
    :return: Path.
    """
    return MetaPath(input)


@to_path.register
def _(input: tuple) -> MetaPathOrMetaPathCollection:
    """Convert tuple to tuple of Paths.

    :param input: Input to be converted to tuple of Paths.
    :return: Tuple of Paths.
    """
    return tuple(to_path(v) for v in input)  # TODO: map?


@to_path.register
@keepmeta
def _(input: list) -> MetaPathOrMetaPathCollection:
    """Convert list to list of Paths.

    Returs a list with the same metadata as the input.

    :param input: Input to be converted.
    :return: List of Paths.
    """
    return MetaList([to_path(v) for v in input])  # TODO: map?


@to_path.register
@keepmeta
def _(input: set) -> MetaPathOrMetaPathCollection:
    """Convert set to set of Patsh.

    Returs a set with the same metadata as the input.

    :param input: Input to be converted.
    :return: Set of Paths.
    """
    return set(to_path(v) for v in input)  # TODO: map?


@singledispatch
def to_filesystem(
    input: Any,
    filename: Optional[Union[Path, str]] = None,
    mkdir: bool = False,
    overwrite: bool = False,
    metacache: Optional[Dataset] = None,  # noqa: U100
) -> MetaPathOrMetaPathCollection:
    """Convert input to Path representing existing file or folder on a disk.

    :param input: Input to be saved/converted to Path.
    :param filename: Output filename. If None, the content is typically saved to a temporary file.
    :param mkdir: Whether to create parent directories.
    :param overwrite: Whether to overwrite existing file or raise an error.
    :param metacache: Dataset to be used as a cache for metadata. Optional.
    :return: Path.
    """
    raise NotImplementedError(f"Cannot convert {type(input)} to filesystem file or folder.")


def _cleanup_temp_dir(dir_path: str | os.PathLike) -> None:
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)


def _get_filename(filename: Optional[Union[Path, str, Parser]], metadata: dict[str, Any]) -> MetaPath:
    """Return filename. Generate temporary filename or build from metadata if requested.

    :param filename: Output filename. Generate temporary filename if None.
    :param metadata: Metadata that can be used to generate filename if it is Parser object.
    :return: MetaPath.
    """
    if filename is None:
        temp_dir = tempfile.mkdtemp()
        atexit.register(_cleanup_temp_dir, temp_dir)  # clean up at program axit

        temp_file = Path(temp_dir) / "temp_file"
        return MetaPath(temp_file)

    if isinstance(filename, Parser):
        return MetaPath(filename.format(**metadata))

    return MetaPath(filename)


def _check_filename(filename: Path | str, mkdir: bool, overwrite: bool) -> None:
    """Check whether the filename is valid and compatible with the flags. Raise error if not.

    :param filename: Output filename.
    :param mkdir: Whether to create parent directories.
    :param overwrite: Whether to overwrite existing file or raise an error.
    :raises FileExistsError: The file exists and overwrite is False.
    """
    filename = Path(filename)
    if filename.exists():
        if filename.is_dir():
            raise FileExistsError(f"File {filename} is a directory.")

        if not overwrite:
            raise FileExistsError(
                f"File {filename} already exists. Set the `overwrite` parameter to True to overwrite."
            )

    if mkdir:
        filename.parent.mkdir(exist_ok=True, parents=True)  # TODO: introduce mkdir_parents parameter


def _stream_to_filesystem(
    input: StringIO | BytesIO,
    filename: Path,
    mkdir: bool = False,
    overwrite: bool = False,
    metacache: Optional[Dataset] = None,
) -> MetaPath:
    """Save stream to file.

    :param input: Input to be saved.
    :param filename: Output filename.
    :param mkdir: Whether to create parent directories.
    :param overwrite: Whether to overwrite existing file or raise an error.
    :param metacache: Dataset to be used as a cache for metadata. Optional.
    :return: Path.
    :raises FileExistsError: The file exists and overwrite is False.
    """
    filename = MetaPath(filename)

    input.seek(0)
    mode = "t" if isinstance(input, StringIO) else "b"

    _check_filename(filename=filename, mkdir=mkdir, overwrite=overwrite)

    with open(filename, "w" + mode) as f:
        f.write(input.read())

    # save metadata
    if metacache is not None:
        metacache[filename] = getmeta(input)

    return filename


@to_filesystem.register
@keepmeta
def _(
    input: Path,
    filename: Optional[Union[Path, str]] = None,
    mkdir: bool = False,  # noqa: U100
    overwrite: bool = False,  # noqa: U100
    metacache: Optional[Dataset] = None,
) -> MetaPath:
    """Convert Path to Path.

    If filename is not None, the output the file is copied, otherwise the input filename is returned.

    :param input: Input to be saved/converted to Path.
    :param filename: Output filename.
    :param mkdir: Whether to create parent directories.
    :param overwrite: Whether to overwrite existing file or raise an error.
    :param metacache: Dataset to be used as a cache for metadata. Optional.
    :return: Path.
    """
    if filename is not None and filename != input:
        # copy file from input to filename
        proper_filename = _get_filename(filename, metadata=getmeta(input))
        shutil.copy(input, proper_filename)

        result = proper_filename
    else:
        result = MetaPath(input)

    # save metadata
    if metacache is not None:
        metacache[result] = getmeta(input)

    return result


@to_filesystem.register
@keepmeta
def _(
    input: StringIO,
    filename: Optional[Union[Path, str]],
    mkdir: bool = False,
    overwrite: bool = False,
    metacache: Optional[Dataset] = None,
) -> MetaPath:
    """Save text stream to file.

    :param input: Input to be saved.
    :param filename: Output filename.
    :param mkdir: Whether to create parent directories.
    :param overwrite: Whether to overwrite existing file or raise an error.
    :param metacache: Dataset to be used as a cache for metadata. Optional.
    :return: Path.
    :raises FileExistsError: The file exists and overwrite is False.
    """
    proper_filename = _get_filename(filename, metadata=getmeta(input))
    return _stream_to_filesystem(
        input=input, filename=proper_filename, mkdir=mkdir, overwrite=overwrite, metacache=metacache
    )


@to_filesystem.register
@keepmeta
def _(
    input: BytesIO,
    filename: Optional[Union[Path, str]],
    mkdir: bool = False,
    overwrite: bool = False,
    metacache: Optional[Dataset] = None,
) -> MetaPath:
    """Save binary stream to file.

    :param input: Input to be saved.
    :param filename: Output filename.
    :param mkdir: Whether to create parent directories.
    :param overwrite: Whether to overwrite existing file or raise an error.
    :param metacache: Dataset to be used as a cache for metadata. Optional.
    :return: Path.
    :raises FileExistsError: The file exists and overwrite is False.
    """
    proper_filename = _get_filename(filename, metadata=getmeta(input))
    return _stream_to_filesystem(
        input=input, filename=proper_filename, mkdir=mkdir, overwrite=overwrite, metacache=metacache
    )


@to_filesystem.register
@keepmeta
def _(
    input: Mapping,
    filename: Union[Path, str],
    mkdir: bool = False,
    overwrite: bool = False,
    metacache: Optional[Dataset] = None,
) -> MetaPath:
    """Save mapping of objects to a folder.

    Creates folder with given name, keys of the mapping represent filenames in that subfolder.

    :param input: Input to be saved.
    :param filename: Output folder.
    :param mkdir: Whether to create parent directories.
    :param overwrite: Whether to overwrite existing directory or raise an error.
    :param metacache: Dataset to be used as a cache for metadata. Optional.
    :return: Path.
    :raises FileExistsError: The file exists and overwrite is False.
    """
    folder = _get_filename(filename, metadata=getmeta(input))
    if folder.exists():
        if overwrite:
            shutil.rmtree(folder)
        else:
            raise FileExistsError(f"Folder {filename} already exists. Set overwrite=True to overwrite.")

    folder.mkdir(parents=mkdir)
    for key, value in input.items():
        to_filesystem(value, folder / key, mkdir=mkdir, overwrite=overwrite, metacache=metacache)

    if metacache is not None:
        metacache[folder] = getmeta(input)

    return folder


@to_filesystem.register
def _(
    input: tuple,
    filename: None = None,
    mkdir: bool = False,
    overwrite: bool = False,
    metacache: Optional[Dataset] = None,
) -> tuple[MetaPathOrMetaPathCollection, ...]:
    """Save tuple of objects.

    :param input: Tuple of inputs to be saved.
    :param filename: Must be None when saving a tuple of objects.
        The objects either have their own filenames or are saved to temporary files.
    :param mkdir: Whether to create parent directories. Applied to all objects in the tuple.
    :param overwrite: Whether to overwrite existing directory or raise an error. Applied to all objects in the tuple.
    :param metacache: Dataset to be used as a cache for metadata. Optional. Applied to all objects in the tuple.
    :return: tuple[Path].
    :raises FileExistsError: The file exists and overwrite is False.
    :raises NotImplementedError: Cannot save tuple of objects to a single file.
    """
    if filename is not None:
        raise NotImplementedError("Cannot save tuple of objects to a single file.")

    return tuple(
        to_filesystem(v, filename=filename, mkdir=mkdir, overwrite=overwrite, metacache=metacache)
        for v in input
    )


@to_filesystem.register
@keepmeta
def _(
    input: set,
    filename: None = None,
    mkdir: bool = False,
    overwrite: bool = False,
    metacache: Optional[Dataset] = None,
) -> set[MetaPathOrMetaPathCollection]:
    """Save set of objects.

    :param input: Tuple of inputs to be saved.
    :param filename: Must be None when saving a set of objects.
        The objects either have their own filenames or are saved to temporary files.
    :param mkdir: Whether to create parent directories. Applied to all objects in the tuple.
    :param overwrite: Whether to overwrite existing directory or raise an error. Applied to all objects in the tuple.
    :param metacache: Dataset to be used as a cache for metadata. Optional. Applied to all objects in the tuple.
    :return: tuple[Path].
    :raises FileExistsError: The file exists and overwrite is False.
    """
    if filename is not None:
        raise NotImplementedError("Cannot save tuple of objects to a single file.")

    return set(
        to_filesystem(v, filename=filename, mkdir=mkdir, overwrite=overwrite, metacache=metacache)
        for v in input
    )


if satpy_installed:

    @to_filesystem.register
    @keepmeta
    def _(
        input: XRImage,
        filename: Union[Path, str],
        mkdir: bool = False,
        overwrite: bool = False,
        metacache: Optional[Dataset] = None,
    ) -> MetaPath:
        """Save satpy xrimage to file.

        :param input: Input to be saved.
        :param filename: Output filename.
        :param mkdir: Whether to create parent directories.
        :param overwrite: Whether to overwrite existing file or raise an error.
        :param metacache: Dataset to be used as a cache for metadata. Optional.
        :return: Path.
        :raises FileExistsError: The file exists and overwrite is False.
        """
        proper_filename = _get_filename(filename, metadata=getmeta(input))
        _check_filename(filename=proper_filename, mkdir=mkdir, overwrite=overwrite)

        input.save(proper_filename)

        # save metadata
        if metacache is not None:
            metacache[proper_filename] = getmeta(input)

        return proper_filename


@singledispatch
def to_parser(input: Any) -> Parser:  # noqa: U100
    """Convert input to Parser."""
    raise NotImplementedError(f"Cannot convert {type(input)} to Parser.")


@to_parser.register
def _(input: str) -> Parser:
    """Convert string value to Parser."""
    return Parser(input)


@to_parser.register
def _(input: Parser) -> Parser:
    """Convert Parser to Parser."""
    return input
