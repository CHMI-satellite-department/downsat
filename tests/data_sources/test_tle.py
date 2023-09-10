from __future__ import annotations

from typing import TYPE_CHECKING
import datetime
from pathlib import Path

import pytest


if TYPE_CHECKING:
    from downsat.clients.spacetrack import SpaceTrackKey
    from downsat.core.utils import TimeSlotType


@pytest.mark.parametrize(
    "tle_dates",
    [
        "2023-06-13",
        datetime.date(2023, 6, 13),
        datetime.datetime(2023, 6, 13, 13, 30),
        "202302",
    ],
    ids=["str_iso_day", "date_day", "datetime_day", "str_month"],
)
def test_tle(
    spacetrack_object_id: int | str,
    spacetrack_key: "SpaceTrackKey",
    tle_dates: "TimeSlotType",
    tmp_path: Path,
) -> None:
    from downsat.data_sources.tle import DailyTLE

    tle = DailyTLE(object_id=spacetrack_object_id, credentials=spacetrack_key, data_path=tmp_path)  # type: ignore  # TODO: fix

    # must not crash
    daily_tle = tle[tle_dates]  # type: ignore  # TODO: fix

    assert isinstance(daily_tle, tuple)
    assert all(isinstance(tle_, Path) for tle_ in daily_tle)
    assert len(daily_tle) > 0
    assert all(tle_.exists() for tle_ in daily_tle)
    assert all(tle_.stat().st_size > 0 for tle_ in daily_tle)
    assert all(tle_.suffix == "" for tle_ in daily_tle)


def test_tle_compatibility_with_pyorbital(
    spacetrack_key: "SpaceTrackKey",
    tmp_path: Path,
) -> None:
    from pyorbital import tlefile

    from downsat.data_sources.tle import DailyTLE

    tle_archive = DailyTLE(object_id="METOP-A", credentials=spacetrack_key, data_path=tmp_path / "METOP-A")  # type: ignore  # TODO: fix

    # must not crash
    tle = tlefile.read("METOP-A", tle_file=str(tle_archive["201106121804"][0]))  # type: ignore
    assert isinstance(tle, tlefile.Tle)
