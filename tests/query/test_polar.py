from typing import TYPE_CHECKING
import datetime
from pathlib import Path


if TYPE_CHECKING:
    from downsat import SpaceTrackKey


def test_find_visible_polar_passes(tmp_path: Path, spacetrack_key: "SpaceTrackKey") -> None:
    from functools import partial

    from downsat import DailyTLE, LonLat
    from downsat.query.polar import find_visible_polar_passes

    tle_archive = partial(DailyTLE, credentials=spacetrack_key, data_path=tmp_path / "tle")
    lonlat = LonLat(lon=14.41854, lat=50.07366)  # Prague
    time = "2023-06-20 9:00"
    dt = datetime.timedelta(hours=1)
    satellite = "METOP-A"

    # can find visible passes, loads horizon automatically
    passes = find_visible_polar_passes(lonlat, time, tle_archive, satellite=satellite, dt=dt)
    assert len(passes) == 1

    # works for multiple satellites
    multi_sat_passes = find_visible_polar_passes(
        lonlat, time, tle_archive, satellite=["METOP-A", "METOP-B"], dt=dt
    )
    assert "METOP-A" in multi_sat_passes
    assert "METOP-B" in multi_sat_passes
    assert multi_sat_passes["METOP-A"] == passes

    # work with default satellites
    default_sat_passes = find_visible_polar_passes(lonlat, time, tle_archive, dt=dt)
    assert "METOP-A" in default_sat_passes
    assert "METOP-B" in default_sat_passes
    assert default_sat_passes["METOP-A"] == passes

    # custom horizon works
    custom_horizon_passes = find_visible_polar_passes(
        lonlat, time, tle_archive, dt=dt, horizon={"METOP-A": 1}
    )
    assert len(custom_horizon_passes["METOP-A"]) <= len(passes)
    custom_horizon_passes = find_visible_polar_passes(
        lonlat, time, tle_archive, dt=dt, horizon={"METOP-A": 90}
    )
    assert len(custom_horizon_passes["METOP-A"]) == 0  # horizon is too high

    # does not crash on future date because of missing TLE (if not too far away in the future)
    time_now = datetime.datetime.now() + datetime.timedelta(days=2)
    dt = datetime.timedelta(days=5)
    passes = find_visible_polar_passes(lonlat, time_now, tle_archive, satellite=satellite, dt=dt)
    assert len(passes) > 0
    # passes must be withing specified range
    assert all(
        (pass_.datetime.replace(tzinfo=None) >= time_now - dt)
        & (pass_.datetime.replace(tzinfo=None) <= time_now + dt)
        for pass_ in passes
    )
