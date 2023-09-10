from __future__ import annotations

from typing import TYPE_CHECKING, Any
import datetime
import time

import pytest
from pytest_cases import fixture, fixture_union


if TYPE_CHECKING:
    from pyresample import AreaDefinition

    from downsat import LonLat
    from downsat.clients.eumdac import EumdacCollection, EumdacKey, EumdacUser
    from downsat.core.utils import TimeSlotType


@fixture
def eumdac_key() -> "EumdacKey":
    from downsat.clients.eumdac import EumdacKey

    try:
        return EumdacKey.from_env()
    except KeyError as e:
        pytest.skip(f"Eumdac credentials not found in env variable. {e}.")


@fixture
def eumdac_user(eumdac_key: "EumdacKey") -> "EumdacUser":
    from downsat.clients.eumdac import EumdacUser

    return EumdacUser(name="John Smith", key=eumdac_key, description="John Smith's eumdac key")


@fixture
def valid_msg_datetime_range() -> slice:
    return slice(datetime.datetime(2022, 11, 4, 8, 0), datetime.datetime(2022, 11, 4, 8, 25))


@fixture
def valid_msg_datetime_single() -> datetime.datetime:
    return datetime.datetime(2022, 11, 4, 8, 5)


valid_msg_time = fixture_union("valid_msg_time", [valid_msg_datetime_single, valid_msg_datetime_range])


@fixture
def eumdac_msg_collection(valid_msg_time: "TimeSlotType") -> tuple[str, "TimeSlotType"]:
    return ("EO:EUM:DAT:MSG:HRSEVIRI", valid_msg_time)


eumdac_collection_name_and_time = fixture_union(
    "eumdac_collection_name_and_time",
    [eumdac_msg_collection],
    unpack_into="eumdac_collection_name,data_timeslot",
)


@fixture
def eumdac_collection(eumdac_collection_name: str, eumdac_user: "EumdacUser") -> "EumdacCollection":
    from downsat.clients.eumdac import EumdacCollection

    return EumdacCollection(name=eumdac_collection_name, credentials=eumdac_user)


@fixture
def invalid_eumdac_query_params() -> dict[str, Any]:
    return {"sat": {"options": "MSG3"}}


def test_eumdac_key(eumdac_key: "EumdacKey") -> None:
    assert eumdac_key.credentials == (eumdac_key.key, eumdac_key.secret)


def test_eumdac_user(eumdac_user: "EumdacUser") -> None:
    assert eumdac_user.credentials == eumdac_user.key.credentials


def test_invalid_eumdac_key() -> None:
    """EumdacClient validates during init that the given eumdac credentials are valid."""
    from downsat.clients.eumdac import EumdacKey

    with pytest.raises(ValueError):
        EumdacKey("invalid", "credentials")


def test_eumdac_collection(eumdac_collection: "EumdacCollection", data_timeslot: "TimeSlotType") -> None:
    from io import BytesIO

    from downsat.etl.metadata import getmeta

    # EumdacCollection has datastore property
    datastore = eumdac_collection.datastore

    # EumdacCollection has collection property
    collection = eumdac_collection.collection
    assert collection == datastore.get_collection(eumdac_collection.name)

    # EumdacCollection can query
    if not isinstance(data_timeslot, slice):
        data_timeslot = slice(data_timeslot, data_timeslot)
    files = eumdac_collection.query(dtstart=data_timeslot.start, dtend=data_timeslot.stop)
    assert len(files) > 0
    expected_filename = files[0]

    # EumdacCollection can download
    data = eumdac_collection[files]  # type: ignore  # TODO: ensure EumdacCollection is instance of protocols.MultiKeyQueryDataSource
    if len(files) == 1:
        assert isinstance(data[0], BytesIO)  # type: ignore  # TODO: fix, classical __getitem__(self, key | tuple[key, ...]) -> result | tuple[result, ...] problem
        assert getmeta(data[0])["name"] == expected_filename  # type: ignore  # TODO: fix, classical __getitem__(self, key | tuple[key, ...]) -> result | tuple[result, ...] problem
        assert len(data[0].read()) > 0  # type: ignore  # TODO: fix, classical __getitem__(self, key | tuple[key, ...]) -> result | tuple[result, ...] problem
    else:
        assert len(data) == len(files)  # type: ignore  # TODO: fix, classical __getitem__(self, key | tuple[key, ...]) -> result | tuple[result, ...] problem


def test_eumdac_collection_fixed_query_params(
    eumdac_key: "EumdacKey",
    eumdac_collection_name_and_time: tuple[str, "TimeSlotType"],
    invalid_eumdac_query_params: dict[str, Any],
) -> None:
    from downsat.clients.eumdac import EumdacCollection

    collection_name, time = eumdac_collection_name_and_time
    if not isinstance(time, slice):
        time = slice(time, time)

    # it works without default params
    valid_collection = EumdacCollection(collection_name, eumdac_key)
    assert len(valid_collection.query(dtstart=time.start, dtend=time.stop))

    # it does not work with an invalid default query param
    invalid_collection = EumdacCollection(
        collection_name, eumdac_key, query_params=invalid_eumdac_query_params
    )
    assert len(invalid_collection.query(dtstart=time.start, dtend=time.stop)) == 0


def test_collection_name_alias(eumdac_key: "EumdacKey") -> None:
    from downsat.clients.eumdac import EumdacCollection

    collection = EumdacCollection("MSG", eumdac_key)

    assert collection.collection_name == "EO:EUM:DAT:MSG:HRSEVIRI"

    assert (
        len(
            collection.query(
                dtstart=datetime.datetime(2022, 11, 4, 8, 5), dtend=datetime.datetime(2022, 11, 4, 8, 15)
            )
        )
        > 0
    )


def test_eumdac_collection_parallel_download(
    eumdac_key: "EumdacKey",
) -> None:
    from downsat.clients.eumdac import EumdacCollection
    from downsat.etl.context import setcontext

    time_range = (datetime.datetime(2022, 11, 4, 8, 0), datetime.datetime(2022, 11, 4, 8, 35))

    # time 1 cpu
    collection_1 = EumdacCollection("MSG", eumdac_key)
    setcontext(num_workers=1)(collection_1)
    ids = collection_1.query(dtstart=time_range[0], dtend=time_range[1])

    assert len(ids) == 3

    start_time_1 = time.time()
    collection_1[ids]  # type: ignore  # TODO: ensure EumdacCollection is instance of protocols.MultiKeyQueryDataSource
    time_1_cpu = time.time() - start_time_1

    # time n cpu
    collection_n = EumdacCollection("MSG", eumdac_key)
    setcontext(num_workers=len(ids))(collection_n)

    start_time_n = time.time()
    collection_n[ids]  # type: ignore  # TODO: ensure EumdacCollection is instance of protocols.MultiKeyQueryDataSource
    time_num_workers = time.time() - start_time_n

    assert time_num_workers < time_1_cpu


def test_invalid_eumdac_key_from_env() -> None:
    from downsat.clients.eumdac import EumdacKey

    # EumdacKey shoud check key validity on init
    with pytest.raises(ValueError):
        EumdacKey.from_env(key="nonexisting_key")

    with pytest.raises(ValueError):
        EumdacKey.from_env(secret="nonexisting_secret")


def test_geo_query(
    eumdac_key: "EumdacKey",
    valid_metop_geo: dict[str, str | "AreaDefinition" | "LonLat"],
    valid_metop_datetime: tuple[datetime.datetime, datetime.datetime],
) -> None:
    from downsat.clients.eumdac import EumdacCollection

    single_datetime = False
    if isinstance(valid_metop_datetime, slice):
        valid_metop_datetime = [valid_metop_datetime.start, valid_metop_datetime.stop]
    elif isinstance(valid_metop_datetime, datetime.datetime):
        valid_metop_datetime = [valid_metop_datetime] * 2
        single_datetime = True

    # geo, point and area work in init
    collection = EumdacCollection("METOP", eumdac_key, query_params=valid_metop_geo)
    data = collection.query(dtstart=valid_metop_datetime[0], dtend=valid_metop_datetime[1])

    if "point" in valid_metop_geo or "geo" in valid_metop_geo:
        assert len(data) == 1
    elif "bbox" in valid_metop_geo or "area" in valid_metop_geo:
        assert len(data) == 2
    else:
        # no region of interest
        assert len(data) == 2 if single_datetime else 5

    # geo, point and area work in the query
    collection = EumdacCollection("METOP", eumdac_key)
    data = collection.query(dtstart=valid_metop_datetime[0], dtend=valid_metop_datetime[1], **valid_metop_geo)

    if "point" in valid_metop_geo or "geo" in valid_metop_geo:
        assert len(data) == 1
    elif "bbox" in valid_metop_geo or "area" in valid_metop_geo:
        assert len(data) == 2
    else:
        # no region of interest
        assert len(data) == 2 if single_datetime else 5
