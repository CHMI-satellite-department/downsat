from __future__ import annotations

from io import StringIO
from pathlib import Path


def test_multidatasource(tmp_path: Path) -> None:
    from downsat.core.file_storage import FileDataset
    from downsat.etl.datasource import MultiDataSource

    dataset1 = FileDataset(data_path=tmp_path / "dataset1")  # type: ignore  # TODO: fix
    dataset2 = FileDataset(data_path=tmp_path / "dataset2")  # type: ignore  # TODO: fix

    datasource = MultiDataSource([dataset1, dataset2], search_strategy="first")  # type: ignore  # TODO: fix

    dataset1["a"] = StringIO("a")  # type: ignore  # TODO: fix  # TODO: FileDataset should use toStringIO decorator
    dataset2["b"] = StringIO("b")  # type: ignore  # TODO: fix  # TODO: FileDataset should use toStringIO decorator

    # multidataset works
    with open(datasource["a"][0]) as f:
        assert f.read() == "a"

    with open(datasource["b"][0]) as f:
        assert f.read() == "b"

    # first dataset is prioritized
    dataset1["b"] = StringIO("c")  # type: ignore  # TODO: fix  # TODO: FileDataset should use toStringIO decorator

    with open(datasource["b"][0]) as f:
        assert f.read() == "c"

    # strategy 'all' should return all matches
    datasource = MultiDataSource([dataset1, dataset2], search_strategy="all")  # type: ignore  # TODO: fix
    value_b = datasource["b"]
    assert len(value_b) == 2

    with open(value_b[0]) as f:
        val1 = f.read()

    with open(value_b[1]) as f:
        val2 = f.read()

    assert (val1 == "b" and val2 == "c") or (val1 == "c" and val2 == "b")

    # strategy 'all' should return unique items only
    datasource = MultiDataSource([dataset1, dataset2, dataset2], search_strategy="all")  # type: ignore  # TODO: fix
    assert len(datasource["b"]) == 2
