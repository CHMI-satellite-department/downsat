from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Protocol, Sequence, TypeVar, overload
import datetime

import numpy as np
from pyorbital.orbital import Orbital

from downsat.core.models import LonLat
from downsat.core.utils import TimeSlotType, parse_time
from downsat.data_sources.satellite_info import SatelliteInfo, satellite_info_leo
from downsat.etl import protocols


if TYPE_CHECKING:
    from arrow import Arrow

    # a trick how to distinguish between str and Sequence[str] in overloaded definition
    _T_co = TypeVar("_T_co", covariant=True)

    class SequenceNotStr(Sequence[_T_co], Protocol[_T_co]):  # type: ignore[misc]
        """Protocol for sequences that are not strings.

        source: https://github.com/python/typing/issues/256#issuecomment-1442633430
        """

        ...


@overload
def find_visible_polar_passes(
    coords: LonLat,
    time: TimeSlotType,
    tle_archive: Callable[[int | str], protocols.DataSource],
    satellite: str,
    satellite_info_archive: SatelliteInfo = satellite_info_leo,
    dt: datetime.timedelta | None = None,
    horizon: float | dict[str, float] | None = None,
    altitude: float = 0,
    max_tle_age: int = 14,
) -> tuple["Arrow", ...]:
    """Find times when a point at surface is visible by a specific polar satellite."""


@overload
def find_visible_polar_passes(
    coords: LonLat,
    time: TimeSlotType,
    tle_archive: Callable[[int | str], protocols.DataSource],
    satellite: "SequenceNotStr[str]" | None = None,
    satellite_info_archive: SatelliteInfo = satellite_info_leo,
    dt: datetime.timedelta | None = None,
    horizon: float | dict[str, float] | None = None,
    altitude: float = 0,
    max_tle_age: int = 14,
) -> dict[str, tuple["Arrow", ...]]:
    """Find times when a point at surface is visible by multiple polar satellites."""


def find_visible_polar_passes(
    coords: LonLat,
    time: TimeSlotType,
    tle_archive: Callable[[int | str], protocols.DataSource],
    satellite: str | Sequence[str] | None = None,
    satellite_info_archive: SatelliteInfo = satellite_info_leo,
    dt: datetime.timedelta | None = None,
    horizon: float | dict[str, float] | None = None,
    altitude: float = 0,
    max_tle_age: int = 14,
) -> tuple["Arrow", ...] | dict[str, tuple["Arrow", ...]]:
    """Calculate the moments when a specific ground point is observable by one or more polar satellites at their maximum elevation.

    :param coords: Geographic coordinates of the ground point of interest.
    :param time: Specific time or time interval of interest. If a single time is given, the dt parameter must be specified.
    :param tle_archive: Partially initialized TLE archive. The `satellite` parameter is used to complete the initialization.
        Ex: `tle_archive=partial(DailyTLE, credentials=SpaceTrack.from_env(), data_path="/path/to/tle/archive")`
    :param satellite: Satellite name(s) or None. If None, all polar satellites from the `satellite_info_archive` will be considered.
        If multiple satellites are processed, the result is a dictionary with satellite names as keys and lists of passes as values.
    :param satellite_info_archive: Partially initialized satellite info archive. Initialized using the 'satellite' argument.
    :param dt: Halfwidth of the time interval. If None, `time` must be an interval; otherwise time must be a single time point.
    :param horizon: Minimum elevation (in degrees) that a satellite must be above the horizon to have line of sight with the
        specified point during its passage. This value is typically set to 90° - half of the full satellite's scanning angle.
        It can be ommited for known satellites. In such case the value is taken from the `downsat.config.satellites.yaml` file.
        If not found, a ValueError is raised. Can also be a dictionary mapping satellite names to horizon angles.
    :parm altitutde: Altitude of the ground point of interest in meters. Default is 0 (sea level).
    :param max_tle_age: Maximum age in days of TLE data to consider. Default is 14 days.
    :return: Tuple of times when the satellite at its maximum elevation can observe the ground point. Each time corrsponds to
        one satellite orbit. If multiple satellites are queried, the result is a dictionary mapping satellite names to tuples of
        observation times. If the satellite is not visible at all, an empty tuple is returned.
    :raises ValueError: If `time` and `dt` are inconsistent.
    :raises ValueError: If `horizon` is not specified and the satellite is not in the `satelite_info_archive`.
    """
    if satellite is None:
        # get all known satellites
        satellite = satellite_info_archive.keys()

    if not isinstance(satellite, str):
        return {
            sat: find_visible_polar_passes(
                coords=coords,
                time=time,
                tle_archive=tle_archive,
                satellite=sat,
                satellite_info_archive=satellite_info_archive,
                dt=dt,
                horizon=horizon,
                altitude=altitude,
                max_tle_age=max_tle_age,
            )
            for sat in satellite
        }

    if isinstance(horizon, dict):
        horizon = horizon.get(satellite)

    if horizon is None:
        # get horizon from satellite info archive
        try:
            satellite_data = satellite_info_archive[satellite]
        except KeyError:
            raise ValueError(
                f"The horizon parameter must be specified. Cannot find parameters of satellite `{satellite}`."
            )

        try:
            horizon = 90 - satellite_data.max_swath_angle  # type: ignore
        except AttributeError:
            raise ValueError(
                "The horizon parameter must be specified. {satellite} does not seem to be a polar satellite - cannot find its swath angle."
            )

    # construct time interval given by the user
    if dt is None:
        time_interval = parse_time(time, interval=True)
        if time_interval.start == time_interval.stop:
            raise ValueError("The `dt` parameter must be specified if the time parameter is a single time.")
    else:
        try:
            middle_time = parse_time(time, interval=False)
        except ValueError:
            raise ValueError(
                "The dt parameter must be None if the time parameter is an interval and vice versa."
            )

        time_interval = slice(middle_time - dt, middle_time + dt)  # type: ignore  # TODO: fix

    # adjust time interval such that the length is a round number of hours
    dt = (time_interval.stop - time_interval.start) / 2
    window_span = dt * 2  # type: ignore  # TODO: fix
    window_hours = int(max(1, np.ceil(window_span.total_seconds()) / 3600))  # type: ignore # at least one hour # TODO: fix type:
    window_start = time_interval.start + dt - datetime.timedelta(hours=window_hours)
    date = time_interval.stop.date()

    # find satellite passes
    daily_tle_archive = tle_archive(satellite)
    for _ in range(max_tle_age):  # max `max_tle_age` days old TLE
        tle_file = daily_tle_archive[date]  # type: ignore  # TODO: fix
        if len(tle_file) > 0 and tle_file[0].stat().st_size > 0:
            # found TLE with non-zero size
            break
        date -= datetime.timedelta(days=1)
    else:
        # no valid TLE file found after searching through the max_tle_age range
        raise ValueError(
            f"No valid TLE file found for satellite `{satellite}` within the last {max_tle_age} days."
        )

    orb = Orbital(satellite, tle_file=str(tle_file[0]))
    pass_times = orb.get_next_passes(
        utc_time=window_start,
        length=window_hours,
        lon=coords.lon,
        lat=coords.lat,
        alt=altitude,
        tol=0.001,
        horizon=horizon,
    )

    # pass time ~ (rise_time, fall_time, max_elevation_time)
    # we want max_elevation_time only
    # and only those that fall into the user-defined time interval

    # TODO: keep tle metadata
    return tuple(
        pass_time[2]
        for pass_time in pass_times
        if pass_time[2] >= time_interval.start and pass_time[2] <= time_interval.stop
    )
