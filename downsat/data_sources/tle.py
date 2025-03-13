from __future__ import annotations

import datetime
from functools import partial
import os
from pathlib import Path

from downsat.clients.spacetrack import DailyTLE as DailyTLESource
from downsat.clients.spacetrack import SpaceTrackKey
from downsat.core.file_storage import FileDataset
from downsat.core.utils import TimeSlotType, parse_time
from downsat.etl.class_transforms import cache, reduce
from downsat.etl.converters import to_filesystem, to_stringio
from downsat.etl.metadata import getmeta
from downsat.etl.transforms import Flatten


def time2date(time: TimeSlotType) -> tuple[str, ...]:
    time_range = parse_time(time, interval=True)

    # generate list of days
    start_date = time_range.start.date()
    end_date = time_range.stop.date()

    dates = [start_date + datetime.timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    # remove future dates
    today = datetime.date.today()
    dates = [date for date in dates if date <= today]

    return tuple(date.isoformat() for date in dates)


def DailyTLE(
    object_id: int | str, credentials: SpaceTrackKey, data_path: str | os.PathLike
) -> tuple[Path, ...]:
    """Get datasource of daily TLE data for a given satellite ID or name.

    :param object_id: NORAD catalog ID of the object or its name, e.g. "METOP-A".
    :param credentials: Credentials for space-track client.
    :param data_path: Path to store the downloaded TLE data. The data will be stored in a subdirectory named after the satellite ID or name.
    """
    # TODO: find a way how to transform init objects of a dataasource class (or any class) without need to instantiate it
    DailyTLE = reduce(
        time2date
        >> cache(
            DailyTLESource >> to_stringio,
            cache=FileDataset,  # type: ignore  # TODO: fix
            skip_if=lambda _, data: getmeta(data)["date"] >= datetime.date.today(),
        )
        >> to_filesystem,
        partial(Flatten, depth=2),  # type: ignore  # TODO: fix
    )

    # TODO: at this moment, the data may be downloaded and cached twice, once when the object_id is int,
    # i.e. norad ID of the satellite, and once when it is str, i.e. name of the satellite
    # -> set data_path=data_path / str(satellite_name) to disambiguate
    return DailyTLE(object_id=object_id, credentials=credentials, data_path=Path(data_path) / str(object_id))  # type: ignore  # TODO: fix - mypy plugin
