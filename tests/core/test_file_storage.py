from __future__ import annotations

from typing import TYPE_CHECKING, Any
from io import BytesIO, StringIO
import os
from pathlib import Path
import random

import pytest
from pytest_cases import fixture_union


if TYPE_CHECKING:
    from trollimage.xrimage import XRImage


@pytest.fixture
def binary_stream() -> BytesIO:
    from downsat.etl.metadata import setmeta

    stream = BytesIO()
    stream.write(os.urandom(1000))
    stream.seek(0)
    setmeta(stream, random=random.randint(-100, 100), name="binary")
    return stream


@pytest.fixture
def text_stream() -> StringIO:
    from downsat.etl.metadata import setmeta

    stream = StringIO()
    stream.write("abcd" * 10)
    stream.seek(0)
    setmeta(stream, random=random.randint(-100, 100), name="text")
    return stream


@pytest.fixture
def xrimage(random_xrimage: "XRImage") -> "XRImage":
    from downsat.etl.metadata import setmeta

    setmeta(random_xrimage, random=random.randint(-100, 100), name="xrimage")
    return random_xrimage


@pytest.fixture
def mapping(text_stream: StringIO, xrimage: "XRImage") -> dict[str, Any]:
    from downsat.etl.metadata import setmeta
    from downsat.etl.weakref import Dict as MetaDict

    res = MetaDict({"text_stream": text_stream, "xrimage.png": xrimage})
    setmeta(res, random=random.randint(-100, 100), name="mapping")

    return res


file_content = fixture_union("file_content", [binary_stream, text_stream, xrimage, mapping])


@pytest.mark.parametrize(
    "filename_pattern", [None, "{random:05d}_{name}"], ids=["no_pattern", "with_pattern"]
)
def test_file_dataset(file_content: Any, tmp_path: Path, filename_pattern: str | None) -> None:
    """Test FileDataset on different types of content."""
    from trollimage.xrimage import XRImage
    from trollsift import Parser

    from downsat.core.file_storage import FileDataset
    from downsat.core.utils import is_relative_to
    from downsat.etl.metadata import clearmeta, getmeta, setmeta

    if filename_pattern is None:
        key = "key1"
    else:
        key = Parser(filename_pattern).compose(getmeta(file_content))

    if isinstance(file_content, XRImage):
        key = key + ".png"
        if filename_pattern is not None:
            filename_pattern = filename_pattern + ".png"

    setmeta(file_content, _extra="extra")  # add metadata that do not appear in the filename_pattern
    metadata = getmeta(file_content)
    storage = FileDataset(data_path=tmp_path, filename_pattern=filename_pattern)  # type: ignore  # TODO: fix

    # setitem works
    assert len(storage) == 0  # type: ignore  # TODO: fix
    storage[key] = file_content  # type: ignore  # TODO: fix
    assert len(storage) == 1  # type: ignore  # TODO: fix
    assert key in storage  # type: ignore  # TODO: fix

    # getitem works
    item = storage[key]

    assert isinstance(item, set)
    assert all(isinstance(path, Path) for path in item)
    if filename_pattern is None:
        key_path = tmp_path / key
    else:
        key_path = tmp_path / Parser(filename_pattern).compose(metadata)

    assert all(is_relative_to(path, key_path) for path in item)

    assert all(path.exists() for path in item)
    assert all(path.stat().st_size > 0 for path in item)  # file is not empty

    # getitem raises on non-existing key
    with pytest.raises(KeyError):
        storage["non-existing"]

    # getitem preserves metadata stored in the filename / dirname
    if filename_pattern is not None:
        recovered_metadata = getmeta(item)
        for metakey in ["random", "name"]:
            if f"{{{metakey}}}" in filename_pattern:
                assert recovered_metadata[metakey] == metadata[metakey]

    # getitem preserves all metadata
    item_metadata = getmeta(item)
    assert item_metadata == metadata

    # getitem preserves metadata of dict values
    for path in item:
        if isinstance(file_content, dict):
            assert getmeta(path) == getmeta(file_content[path.name])
        else:
            assert getmeta(path) == getmeta(file_content)

    # multi-key getitem works
    multi_items = storage[key, key]  # type: ignore  # TODO: ensure FileDataset is instance of protocols.MultiKeyQueryDataSource
    assert len(multi_items) == 2  # type: ignore  # TODO: fix by using Generic in FileStorageBase
    assert isinstance(multi_items, tuple)
    assert multi_items[0] == multi_items[1] == item
    assert getmeta(multi_items[0]) == getmeta(multi_items[1]) == getmeta(file_content)

    # content can be recovered
    if isinstance(file_content, (BytesIO, StringIO)):
        mode = "rt" if isinstance(file_content, StringIO) else "rb"
        with open(next(iter(item)), mode) as f:
            data = f.read()
        file_content.seek(0)
        assert data == file_content.read()
    # TODO: check also XRImage and dict

    # delitem works
    del storage[key]  # type: ignore  # TODO: fix
    assert len(storage) == 0  # type: ignore  # TODO: fix
    assert key not in storage  # type: ignore  # TODO: fix

    # delitem destroys metadata (saving new element w/ different metadata does not get metadata of the deleted ones)
    modified_metadata = getmeta(file_content) | {"_extra": "extra2", "dummy": 3}
    clearmeta(file_content)
    setmeta(file_content, **modified_metadata)
    storage[key] = file_content  # type: ignore  # TODO: fix
    assert getmeta(storage[key]) == modified_metadata


def test_file_dataset_serialization_error(xrimage: "XRImage", tmp_path: Path) -> None:
    """Test SingleFileDataset raises error when trying to store non-serializable content."""
    from downsat.core.file_storage import FileDataset

    storage = FileDataset(data_path=tmp_path)  # type: ignore  # TODO: fix
    # xrimage is not serializable to a filename without proper image extension
    with pytest.raises(KeyError):
        storage["key1"] = xrimage  # type: ignore  # TODO: fix

    # nothing should have been stored
    assert len(storage) == 0  # type: ignore  # TODO: fix


def test_file_dataset_key_not_matching_pattern(text_stream: StringIO, tmp_path: Path) -> None:
    """Test FileDataset raises error when trying to store content with key not matching the pattern."""
    from downsat.core.file_storage import FileDataset

    storage = FileDataset(data_path=tmp_path, filename_pattern="{name}")  # type: ignore  # TODO: fix
    with pytest.raises(KeyError):
        storage["key1"] = text_stream  # type: ignore  # TODO: fix

    # nothing should have been stored
    assert len(storage) == 0  # type: ignore  # TODO: fix


def test_file_dataset_mkdir(tmp_path: Path) -> None:
    """Test FileDataset creates directories when needed."""
    from downsat.core.file_storage import FileDataset

    test_path = tmp_path / "dir1" / "dir2"

    # test_path does not exist -> raise
    with pytest.raises(ValueError):
        FileDataset(data_path=test_path, mkdir=False)  # type: ignore  # TODO: fix

    # test_path will be created
    storage = FileDataset(data_path=test_path, mkdir=True)  # type: ignore  # TODO: fix

    # storage works
    storage["key1"] = StringIO()  # type: ignore  # TODO: fix
    assert (test_path / "key1").exists()


@pytest.mark.parametrize("pattern", ["{random:05d}_{name}"], ids=["file_pattern"])
def test_file_dataset_search(
    tmp_path: Path, pattern: str, text_stream: StringIO, binary_stream: BytesIO
) -> None:
    """Test that FileDataset.search works."""
    from downsat.core.file_storage import FileDataset
    from downsat.etl.metadata import getmeta, setmeta

    storage = FileDataset(data_path=tmp_path, mkdir=False, filename_pattern=pattern)  # type: ignore  # TODO: fix

    # save test data
    storage[storage.filename_pattern.compose(getmeta(text_stream))] = text_stream  # type: ignore
    storage[storage.filename_pattern.compose(getmeta(binary_stream))] = binary_stream  # type: ignore
    metadata = getmeta(binary_stream)
    setmeta(
        binary_stream, random=metadata["random"] + 5
    )  # add extra item with common name but different random value
    storage[storage.filename_pattern.compose(getmeta(binary_stream))] = binary_stream  # type: ignore
    assert len(storage) == 3  # type: ignore  # TODO: fix

    # search works
    keys = storage.search(name="text")  # type: ignore  # TODO: fix
    assert len(keys) == 1
    assert isinstance(keys, set)
    assert isinstance(keys.pop(), str)

    # search works with multiple output candidates
    assert len(storage.search(name="binary")) == 2  # type: ignore  # TODO: fix

    # search works with multiple criteria
    assert len(storage.search(**metadata)) == 1  # type: ignore  # TODO: fix

    # empty search returns empty set
    assert len(storage.search()) == 3  # type: ignore  # TODO: fix

    # non-existing search returns empty set
    assert len(storage.search(name="non-existing")) == 0  # type: ignore  # TODO: fix


@pytest.mark.parametrize("pattern", ["{random:05d}_{name}"], ids=["file_pattern"])
def test_file_dataset_add(tmp_path: Path, pattern: str, text_stream: StringIO) -> None:
    """Test that FileDataset.add works."""
    from downsat.core.file_storage import FileDataset
    from downsat.etl.metadata import getmeta

    storage = FileDataset(data_path=tmp_path, mkdir=False, filename_pattern=pattern)  # type: ignore  # TODO: fix
    key = storage.add(text_stream)  # type: ignore  # TODO: fix

    # item was correctly stored
    assert len(storage) == 1  # type: ignore  # TODO: fix
    assert key in storage  # type: ignore  # TODO: fix
    assert getmeta(storage[key]) == getmeta(text_stream)
    assert getmeta(storage[key].pop()) == getmeta(text_stream)  # type: ignore  # TODO: fix

    expected_key = storage.filename_pattern.compose(getmeta(text_stream))  # type: ignore

    assert storage[key] == {storage.data_path / expected_key}  # type: ignore  # TODO: fix
    assert key == expected_key  # type: ignore


@pytest.mark.parametrize("pattern", ["{random:05d}_{name}"], ids=["file_pattern"])
def test_file_dataset_partial_key(
    tmp_path: Path, pattern: str, text_stream: StringIO, binary_stream: BytesIO
) -> None:
    from trollsift import Parser

    from downsat.core.file_storage import FileDataset
    from downsat.etl.metadata import getmeta

    storage = FileDataset(data_path=tmp_path, mkdir=False, filename_pattern=pattern)  # type: ignore  # TODO: fix

    storage.add(text_stream)  # type: ignore  # TODO: fix
    storage.add(binary_stream)  # type: ignore  # TODO: fix
    assert len(storage) == 2  # type: ignore  # TODO: fix

    # can get partial key
    all_data = storage[pattern]
    assert len(all_data) == 2

    partial_key = Parser(pattern).compose({"name": "text"}, allow_partial=True)  # type: ignore
    data = storage[partial_key]
    assert len(data) == 1

    # assert partial key retains metadata
    assert getmeta(all_data) == dict(set(getmeta(text_stream).items()) & set(getmeta(binary_stream).items()))
    assert getmeta(data) == getmeta(text_stream)
    assert getmeta(data.pop()) == getmeta(text_stream)  # type: ignore  # TODO: fix

    # can delete partial key
    del storage[partial_key]  # type: ignore  # TODO: fix  # delete one key
    assert len(storage) == 1  # type: ignore  # TODO: fix

    storage.add(text_stream)  # type: ignore  # TODO: fix  # add key to have 2 keys again
    del storage[pattern]  # type: ignore  # TODO: fix  # delete multiple keys
    assert len(storage) == 0  # type: ignore  # TODO: fix

    storage.add(binary_stream)  # type: ignore  # TODO: fix  # add key to have 1 key again

    # no result with partial key raises
    with pytest.raises(KeyError):
        storage[partial_key]

    # can save with partial_key, metadata is filled in from value
    storage[partial_key] = text_stream  # type: ignore  # TODO: fix
    assert len(storage) == 2  # type: ignore  # TODO: fix
