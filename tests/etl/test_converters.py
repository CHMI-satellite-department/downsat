from typing import TYPE_CHECKING, Any, Callable
from io import StringIO
from pathlib import Path

import pytest
from trollsift import Parser

from downsat.etl import converters
from downsat.etl.weakref import List as MetaList


if TYPE_CHECKING:
    from trollimage.xrimage import XRImage


def test_xrimage_to_filesystem(random_xrimage: "XRImage", tmp_path: Path) -> None:
    from PIL import Image
    import numpy as np

    from downsat.etl.converters import to_filesystem

    path: Path = to_filesystem(random_xrimage, tmp_path / "test.png")  # type: ignore
    assert path.exists()
    assert path == tmp_path / "test.png"
    assert path.stat().st_size > 0

    # load the image back using PIL and check that the values are the same as xrimage_item.data
    img = Image.open(path).convert("RGB")
    img_data = np.array(img) / 255  # normalize to 0..1
    assert np.allclose(img_data, random_xrimage.data.values, atol=0.01)


def test_mapping_to_filesystem(random_xrimage: "XRImage", tmp_path: Path) -> None:
    from downsat.etl.converters import to_filesystem

    text_buf = StringIO("Text buffer")

    dict_to_save = {"file1.txt": text_buf, "image.png": random_xrimage}
    path: Path = to_filesystem(dict_to_save, tmp_path / "test_folder")  # type: ignore
    assert path.exists()
    assert path == tmp_path / "test_folder"
    files = list(path.glob("*"))
    assert len(files) == 2
    for fname in dict_to_save:
        assert tmp_path / "test_folder" / fname in files


@pytest.mark.parametrize(
    "input",
    [
        "test string",
        Parser("test parser"),
    ],
    ids=["string", "parser"],
)
def test_to_parser(input: Any) -> None:
    from downsat.etl.converters import to_parser

    result = to_parser(input)
    assert isinstance(result, Parser)


@pytest.mark.parametrize(
    "input",
    [
        42,
        None,
        [1, 2, 3],
    ],
)
def test_to_parser_invalid_input(input: Any) -> None:
    from downsat.etl.converters import to_parser

    with pytest.raises(NotImplementedError):
        to_parser(input)


@pytest.mark.parametrize(
    "func,input",
    [
        (converters.to_stringio, StringIO("test_string")),
        (converters.to_stringio, MetaList(["test", "string"])),
    ],
    ids=["to_stringio-stringio", "to_stringio-list"],
)  # TODO: add more test cases
def test_keeps_metadata(func: Callable, input: Any) -> None:
    from downsat.etl.metadata import getmeta, setmeta

    metadata = {"a": 5, "b": "test"}
    setmeta(input, **metadata)
    output = func(input)
    assert getmeta(output) == metadata
