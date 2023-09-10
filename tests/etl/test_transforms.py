from __future__ import annotations

import pytest


def test_filter() -> None:
    from downsat.etl.metadata import getmeta, setmeta
    from downsat.etl.transforms import Filter
    from downsat.etl.weakref import List

    # Test that only elements with even values are kept
    filter_fun = lambda v, metadata: v % 2 == 0  # noqa: U100, E731
    filter_seq = Filter(filter_fun)
    assert filter_seq([1, 2, 3, 4]) == [2, 4]

    # Test that the filter function uses metadata correctly
    filter_fun = lambda v, metadata: v > metadata.get("max", 0)  # noqa: E731
    filter_seq = Filter(filter_fun)
    data = List([10, 20, 30])
    setmeta(data, max=15)
    assert filter_seq(data) == [20, 30]
    assert isinstance(filter_seq(data), list)
    assert getmeta(filter_seq(data)) == {"max": 15}


def test_unzip_transform() -> None:
    from io import BytesIO
    from zipfile import BadZipFile, ZipFile

    from downsat.etl.metadata import getmeta, setmeta
    from downsat.etl.transforms import UnzipBuffer

    transform = UnzipBuffer()
    test_metadata = {"a": 1, "b": "c"}

    # Test with an empty zip file
    empty_zip_io = BytesIO()
    with ZipFile(empty_zip_io, "w") as zipf:
        pass
    empty_zip_io.seek(0)
    assert transform(empty_zip_io) == {}

    # Test with a non-empty zip file
    test_zip_io = BytesIO()
    with ZipFile(test_zip_io, "w") as zipf:
        zipf.writestr("file1.txt", "hello")
        zipf.writestr("file2.txt", "world")
    test_zip_io.seek(0)
    setmeta(test_zip_io, **test_metadata)

    extracted = transform(test_zip_io)
    assert set(extracted.keys()) == {"file1.txt", "file2.txt"}
    assert extracted["file1.txt"].getvalue() == b"hello"
    assert extracted["file2.txt"].getvalue() == b"world"
    assert getmeta(extracted) == test_metadata

    # Test with non-zip file
    non_zip = BytesIO(b"This is not a zip file.")
    with pytest.raises(BadZipFile):
        transform(non_zip)


def test_flatten_datasource() -> None:
    from downsat.etl.transforms import Flatten

    flat_source: Flatten[int] = Flatten(depth=1)

    assert flat_source((1, (2, 3))) == (1, 2, 3)  # type: ignore  # TODO: fix - recursive type in Flatten definition
    assert flat_source(1) == 1
    assert flat_source((1, (2, (3,)))) == (1, 2, (3,))  # type: ignore  # TODO: fix - recursive type in Flatten definition

    # flatten twice in a row
    flat_source2: Flatten[int] = Flatten(depth=2)
    assert flat_source2((1, (2, 3))) == (1, 2, 3)  # type: ignore  # TODO: fix - recursive type in Flatten definition
    assert flat_source2(1) == 1
    assert flat_source2((1, (2, (3,)))) == (1, 2, 3)  # type: ignore  # TODO: fix - recursive type in Flatten definition
