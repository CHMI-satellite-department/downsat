from __future__ import annotations

from typing import TYPE_CHECKING
import datetime

import pytest
from pytest import fixture


if TYPE_CHECKING:
    from downsat.clients.spacetrack import DailyTLE, SpaceTrackKey


@fixture
def daily_tle(spacetrack_object_id: int | str, spacetrack_key: "SpaceTrackKey") -> "DailyTLE":
    from downsat.clients.spacetrack import DailyTLE

    return DailyTLE(object_id=spacetrack_object_id, credentials=spacetrack_key)


def test_single_tle(daily_tle: "DailyTLE") -> None:
    from downsat.etl.metadata import getmeta

    date = "2022-06-13"

    tles = daily_tle[date]

    assert isinstance(tles, list)
    assert len(tles) == 5
    assert all(isinstance(tle, str) for tle in tles)

    # the whole container as well as individual fragments have metadata
    assert all(len(getmeta(tle)) > 0 for tle in tles)
    assert getmeta(tles)["key"] == date
    assert getmeta(tles)["date"].isoformat() == date


def test_multiple_tle(daily_tle: "DailyTLE") -> None:
    from downsat.etl.metadata import getmeta

    date1 = "2022-06-13"
    date2 = "2022-06-14"

    tles = daily_tle[date1, date2]  # type: ignore  # TODO: fix

    assert isinstance(tles, tuple)
    assert len(tles) == 2

    assert all(isinstance(tles_, list) for tles_ in tles)
    assert len(tles[0]) == 5
    assert len(tles[1]) == 6
    assert all(isinstance(tle, str) for tle in tles[0] + tles[1])

    # the whole container as well as individual fragments have metadata
    assert all(len(getmeta(tle)) > 0 for tle in tles[0] + tles[1])
    assert getmeta(tles[0])["key"] == date1
    assert getmeta(tles[0])["date"].isoformat() == date1
    assert getmeta(tles[1])["key"] == date2
    assert getmeta(tles[1])["date"].isoformat() == date2


def test_invalid_tle_key(daily_tle: "DailyTLE") -> None:
    """Test that given invalid key, daily_tle raises a KeyError.

    Invalid key can be a future date or not a date in iso format.
    """

    # future date should fail
    future_date = datetime.datetime.now().date() + datetime.timedelta(days=1)
    with pytest.raises(KeyError):
        daily_tle[future_date.isoformat()]

    # not a date should fail
    with pytest.raises(KeyError):
        daily_tle["not a date"]

    # not a string should fail
    with pytest.raises(KeyError):
        daily_tle[datetime.date.fromisoformat("2022-06-13")]  # type: ignore  # this is what we actually test
