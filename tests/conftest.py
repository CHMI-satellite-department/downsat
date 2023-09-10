from __future__ import annotations

from typing import TYPE_CHECKING, Generator
import datetime
from pathlib import Path
import tempfile

from _pytest.fixtures import SubRequest
from pyresample import AreaDefinition
import pytest
from pytest_cases import fixture, fixture_union, param_fixture

from downsat.core.models import LonLat


if TYPE_CHECKING:
    from trollimage.xrimage import XRImage

    from downsat.clients.eumdac import EumdacCollection, EumdacKey, EumdacUser, TimeSlotType
    from downsat.clients.spacetrack import SpaceTrackKey
    from downsat.etl import protocols


# --- eumdac
@fixture
def eumdac_key() -> "EumdacKey":
    from downsat.clients.eumdac import EumdacKey

    try:
        return EumdacKey.from_env()
    except KeyError as e:
        pytest.skip(f"Eumdac credentials not found in env variable {e}.")


@fixture
def eumdac_user(eumdac_key: "EumdacKey") -> "EumdacUser":
    from downsat.clients.eumdac import EumdacUser

    return EumdacUser(name="John Smith", key=eumdac_key, description="John Smith's eumdac key")


@fixture(
    params=[
        "202211040805",
        datetime.datetime(2022, 11, 4, 8, 5),
        ("202211040805", datetime.datetime(2022, 11, 4, 8, 10)),
    ],
    ids=["str", "datetime", "range"],
)
def valid_msg_time(request: "SubRequest") -> "TimeSlotType":
    return request.param


@fixture
def eumdac_msg_collection(valid_msg_time: "TimeSlotType") -> tuple[str, "TimeSlotType"]:
    return ("EO:EUM:DAT:MSG:HRSEVIRI", valid_msg_time)


eumdac_collection_name_and_time = fixture_union("eumdac_collection_name_and_time", [eumdac_msg_collection])


@fixture(unpack_into="eumdac_collection,data_timeslot")
def eumdac_collection_and_time(
    eumdac_collection_name_and_time: str, eumdac_user: "EumdacUser"
) -> tuple["EumdacCollection", "TimeSlotType"]:
    from downsat.clients.eumdac import EumdacCollection

    return (
        EumdacCollection(name=eumdac_collection_name_and_time[0], credentials=eumdac_user),
        eumdac_collection_name_and_time[1],
    )


# --- Metop data
valid_metop_geo = param_fixture(
    "valid_metop_geo",
    [
        {"area": "germ"},
        {"point": LonLat(lon=14.46, lat=50.0)},
        {},
        {"geo": "POINT(14.46 50.0)"},
        {
            "area": AreaDefinition(
                "test_area",
                "",
                "test_area",
                {
                    "ellps": "WGS84",
                    "lat_0": "90",
                    "lat_ts": "50",
                    "lon_0": "0",
                    "no_defs": "None",
                    "proj": "stere",
                    "type": "crs",
                    "units": "m",
                    "x_0": "0",
                    "y_0": "0",
                },
                425,
                425,
                (-155100, -4441495, 868899, -3417495),
            )
        },
    ],
    ids=["str_area", "point", "none", "geo_point", "area_def"],
)


valid_metop_datetime = param_fixture(
    "valid_metop_datetime",
    [
        (datetime.datetime(2022, 1, 1, 11, 30), datetime.datetime(2022, 1, 1, 14, 0)),
        slice(datetime.datetime(2022, 1, 1, 11, 30), datetime.datetime(2022, 1, 1, 14, 0)),
        datetime.datetime(2022, 1, 1, 11, 30),
    ],
    ids=["datetime_list", "datetime_range", "single_datetime"],
)


# --- spacetrack
@fixture
def spacetrack_key() -> "SpaceTrackKey":
    from downsat.clients.spacetrack import SpaceTrackKey

    try:
        return SpaceTrackKey.from_env()
    except KeyError as e:
        pytest.skip(f"SpaceTrack credentials not found in env variable. {e}.")


@fixture
def norad_cat_id() -> int:
    return 38771


@fixture
def spacetrack_satellite_name() -> str:
    return "METOP-B"


spacetrack_object_id = fixture_union("spacetrack_object_id", [norad_cat_id, spacetrack_satellite_name])

# -- global archives


@pytest.fixture(scope="session")
def msg_archive_path() -> Generator[Path, None, None]:
    """Global MSG archive for tests that do not need to repeatedly download data."""
    with tempfile.TemporaryDirectory() as temp_dir_path:
        yield Path(temp_dir_path) / "MSG"


@pytest.fixture(scope="session")
def metop_archive_path() -> Generator[Path, None, None]:
    """Global METOP archive for tests that do not need to repeatedly download data."""
    with tempfile.TemporaryDirectory() as temp_dir_path:
        yield Path(temp_dir_path) / "METOP"


@pytest.fixture(scope="session")
def rss_archive_path() -> Generator[Path, None, None]:
    """Global RSS archive for tests that do not need to repeatedly download data."""
    with tempfile.TemporaryDirectory() as temp_dir_path:
        yield Path(temp_dir_path) / "RSS"


@pytest.fixture
def msg_archive(msg_archive_path: Path, eumdac_key: "EumdacKey") -> "protocols.MultiKeyDataSource":
    from downsat import MSG

    return MSG(eumdac_key, msg_archive_path)


@pytest.fixture
def rss_archive(rss_archive_path: Path, eumdac_key: "EumdacKey") -> "protocols.MultiKeyDataSource":
    from downsat import RSS

    return RSS(eumdac_key, rss_archive_path)


@pytest.fixture
def metop_archive(metop_archive_path: Path, eumdac_key: "EumdacKey") -> "protocols.MultiKeyDataSource":
    from downsat import RSS

    return RSS(eumdac_key, metop_archive_path)


# test items


@pytest.fixture
def random_xrimage() -> "XRImage":
    import numpy as np
    from trollimage.xrimage import XRImage
    import xarray as xr

    # Create a random xarray DataArray with 3 dimensions: x, y and bands
    data = np.random.rand(100, 100, 3)
    da = xr.DataArray(data, dims=["y", "x", "bands"], coords={"bands": ["R", "G", "B"]})

    # Convert the DataArray to an RGB XRImage
    return XRImage(da)
