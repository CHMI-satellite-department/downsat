from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict
import datetime
from io import BytesIO
from pathlib import Path, PosixPath
import time
from unittest.mock import patch
from zipfile import ZipFile

import arrow
from pyresample import AreaDefinition
import pytest
from pytest import fixture
from pytest_cases import fixture_union

from downsat import LonLat


if TYPE_CHECKING:
    from downsat import EumdacKey
    from downsat.etl import protocols


@fixture
def valid_msg_datetime() -> str:
    return "202211011530"


@fixture
def valid_msg_datetime_range() -> str:
    return "2021110113"


@fixture
def metop_datasource_and_slot(
    metop_archive: "protocols.MultiKeyDataSource",
) -> tuple["protocols.MultiKeyDataSource", str]:
    return metop_archive, "202201011130"


@fixture
def msg_datasource_and_slot(
    msg_archive: "protocols.MultiKeyDataSource",
) -> tuple["protocols.MultiKeyDataSource", str]:
    return msg_archive, "202211011530"


@fixture
def rss_datasource_and_slot(
    rss_archive: "protocols.MultiKeyDataSource",
) -> tuple["protocols.MultiKeyDataSource", str]:
    return rss_archive, "202211011530"


fixture_union(
    "eumdac_datasource_and_slot",
    [msg_datasource_and_slot, rss_datasource_and_slot, metop_datasource_and_slot],
    ids=["msg", "rss", "metop"],
    unpack_into="eumdac_datasource, eumdac_datasource_test_date",
)


@pytest.mark.order(0)
def test_eumdac_caching(eumdac_datasource: "protocols.DataSource", eumdac_datasource_test_date: str) -> None:

    # download new data from data store
    start_time = time.time()
    path1 = eumdac_datasource[eumdac_datasource_test_date]
    duration_1 = time.time() - start_time

    # get data from file cache
    start_time = time.time()
    path2 = eumdac_datasource[eumdac_datasource_test_date]
    duration_2 = time.time() - start_time

    # cache works
    assert path1 == path2
    assert duration_1 > 7  # data must be downloaded on the first request
    assert duration_2 < 7  # data were already cached


def test_msg(
    msg_archive_path: Path,
    eumdac_key: "EumdacKey",
    valid_msg_datetime: str,
    valid_msg_datetime_range: str,
) -> None:
    from downsat import MSG

    msg = MSG(eumdac_key, msg_archive_path)

    # download new data from data store
    path1 = msg[valid_msg_datetime]

    # single key should return list
    assert isinstance(path1, tuple)
    assert len(path1) == 1
    assert isinstance(path1[0], Path)
    assert path1[0].exists()  # type: ignore  # TODO: Fix type hints of FlattenDataSource

    # multiple keys should return flattened list
    msg = MSG(eumdac_key, msg_archive_path, num_workers=3)
    paths = msg[valid_msg_datetime, valid_msg_datetime_range]
    assert isinstance(paths, tuple)
    assert len(paths) == 5
    assert all(isinstance(path, (Path, PosixPath)) for path in paths)


@pytest.mark.parametrize("path_as_str", [False, True], ids=["path_as_Path", "path_as_str"])
def test_msg_data_path_type(
    msg_archive_path: Path | str, eumdac_key: "EumdacKey", valid_msg_datetime: str, path_as_str: bool
) -> None:
    from downsat import MSG

    if path_as_str:
        # test that the object can accept string paths
        msg_archive_path = str(msg_archive_path)

    msg = MSG(eumdac_key, msg_archive_path)
    path1 = msg[valid_msg_datetime]

    # single key should return list
    assert isinstance(path1, tuple)
    assert len(path1) == 1
    assert isinstance(path1[0], Path)
    assert path1[0].exists()  # type: ignore  # TODO: Fix type hints of FlattenDataSource


@pytest.mark.parametrize(
    "time_slot",
    ["202211011530", datetime.datetime(2022, 11, 1, 15, 30), arrow.get("2022-11-01 15:30")],
    ids=["str", "datetime", "arrow"],
)
def test_msg_input_formats(
    tmp_path: Path, eumdac_key: "EumdacKey", time_slot: str | datetime.datetime
) -> None:
    """Test that MSG data source accepts different time slot formats."""
    from dateutil.tz import tzutc

    from downsat import MSG
    from downsat.etl.metadata import setmeta

    msg = MSG(eumdac_key, tmp_path)

    # build sample eumdac client output  # TODO: turn into fixture
    zip_io = BytesIO()
    with ZipFile(zip_io, "w") as zip_file:
        zip_file.writestr("some_file", b"some_data")
    setmeta(zip_io, name="some_file")

    # mock query and load_data_by_id methods of EumdacCollection not to actually download the data
    # but test that the query is called with proper time range
    with patch("downsat.clients.eumdac.EumdacCollection.query", autospec=True) as mock_query:
        with patch(
            "downsat.clients.eumdac.EumdacCollection._load_data_by_id", autospec=True
        ) as mock_load_data:
            mock_query.return_value = ("some_file",)
            mock_load_data.return_value = zip_io

            msg[time_slot]  # TODO: mock and test that the query gets proper dtstart and dtend
            mock_query.assert_called_once()
            assert mock_query.call_args.kwargs["dtstart"] == datetime.datetime(
                2022, 11, 1, 15, 30, tzinfo=tzutc()
            )
            assert mock_query.call_args.kwargs["dtend"] == datetime.datetime(
                2022, 11, 1, 15, 31, tzinfo=tzutc()
            )


@pytest.mark.parametrize("flatten", [True, False], ids=["flatten", "no_flatten"])
def test_msg_flatten(msg_archive_path: Path, eumdac_key: "EumdacKey", flatten: bool) -> None:
    from downsat import MSG

    msg = MSG(eumdac_key, msg_archive_path, flatten=flatten)

    # single time slot
    single_value = msg["202211011530"]
    assert isinstance(single_value, tuple)
    assert len(single_value) == 1
    assert isinstance(single_value[0], Path)  # TODO: shouldn't this be list[Path] in case flatten=False?

    # multiple time slots
    multiple_values = msg["202211011530", "202211011545"]
    assert isinstance(multiple_values, tuple)
    assert len(multiple_values) == 2
    if flatten:
        assert all(isinstance(path, Path) for path in multiple_values)
    else:
        assert all(isinstance(path, list) for path in multiple_values)
        assert all(len(path) == 1 for path in multiple_values)  # type: ignore
        assert all(isinstance(path[0], Path) for path in multiple_values)  # type: ignore


def test_rss(
    rss_archive_path: Path, eumdac_key: "EumdacKey", valid_msg_datetime: str, valid_msg_datetime_range: str
) -> None:
    from downsat import RSS

    rss = RSS(eumdac_key, rss_archive_path)

    # download new data from data store
    path1 = rss[valid_msg_datetime]

    # single key should return list
    assert isinstance(path1, tuple)
    assert path1[0].exists()  # type: ignore  # TODO: fix - create proper overload of the factory function that generates MSG and RSS and whose ouput type depends on the value of the flatten argument

    # multiple keys should return flattened list
    rss = RSS(eumdac_key, rss_archive_path, num_workers=3)
    paths = rss[valid_msg_datetime, valid_msg_datetime_range]
    assert isinstance(paths, tuple)
    assert len(paths) == 13
    assert all(isinstance(path, Path) for path in paths)


class ValidGeoQueryParams(TypedDict, total=False):
    area: str | "AreaDefinition"
    point: LonLat
    geo: str
    bbox: str


def test_metop(
    metop_archive_path: Path,
    eumdac_key: "EumdacKey",
    valid_metop_datetime: str,
    valid_metop_geo: ValidGeoQueryParams,
) -> None:
    from downsat import Metop

    metop = Metop(eumdac_key, metop_archive_path, **valid_metop_geo)

    # download new data from data store
    path1 = metop[valid_metop_datetime]

    # single key should return list
    assert isinstance(path1, tuple)
    assert path1[0].exists()  # type: ignore  # TODO: Fix type hints of FlattenDataSource


@pytest.mark.parametrize("satellite", ["MSG", "RSS", "Metop"], ids=["MSG", "RSS", "Metop"])
def test_from_env(tmp_path: Path, satellite: str) -> None:
    import importlib

    # dynamically import the archive class/function
    module = importlib.import_module("downsat")
    satellite_object = getattr(module, satellite)

    # make sure the archives like MSG or Metop can be build from environment variables
    satellite_object.from_env(data_path=tmp_path)
