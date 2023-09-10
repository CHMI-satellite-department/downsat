from __future__ import annotations

from typing import Literal, overload
from pathlib import Path

import arrow
import importlib_resources as resources
import yaml

from downsat.core.typing import TimeSlotType, TimeType


with resources.as_file(resources.files("downsat.config") / "downsat.yaml") as downsat_config_file:
    with open(downsat_config_file, "rt", encoding="utf-8") as f:
        downsat_config = yaml.safe_load(f)


# time is None
@overload
def parse_time(time: Literal[None], interval: Literal[False] | Literal[None] = None) -> None:
    ...


@overload
def parse_time(time: Literal[None], interval: Literal[True]) -> slice:
    ...


# single time
@overload
def parse_time(time: TimeType, interval: Literal[False]) -> arrow.Arrow:
    ...


@overload
def parse_time(time: TimeType, interval: Literal[True]) -> slice:
    ...


@overload
def parse_time(time: TimeType, interval: Literal[None] = None) -> arrow.Arrow:
    ...


# time interval
@overload
def parse_time(time: slice, interval: Literal[None] | Literal[True] = None) -> slice:
    ...


@overload
def parse_time(time: slice, interval: Literal[False]) -> arrow.Arrow | None:
    ...


# implementation
def parse_time(time: TimeSlotType, interval: bool | None = None) -> arrow.Arrow | slice | None:
    """Parse time or time interval to arrow.

    :param time: Time or tuple of times to be converted.
    :param interval: Return time interval (slice) if the input is single datetime? If False, raise ValueError
        if the output should be an interval.
    :returns: Time or time interval converted to arrow.
    :raises arrow.parser.ParserError: String cannot be converted to time.
    :raises ValueError: The input is a time interval and the interval parameter is False.
    """
    # None
    if time is None:
        if interval is True:
            return slice(None, None)
        else:
            return None

    # time interval
    if isinstance(time, slice):
        if time.step is not None:
            raise NotImplementedError(f"Time slice step is not None: {slice.step}")  # TODO: implement
        start = parse_time(time.start, interval=False)
        end = parse_time(time.stop, interval=False)

        if interval is False:
            if start == end:
                return start
            else:
                raise ValueError("Forbidden to return time interval. Please set `interval` to True or None.")

        return slice(start, end)

    # single time
    result: arrow.Arrow | slice | None = None
    if not isinstance(time, arrow.Arrow):
        for span, formats in downsat_config["datetime"]["special_formats"].items():
            for format in formats:
                try:
                    single_result = arrow.get(time, format)
                except arrow.parser.ParserError:
                    continue
                else:
                    if interval is not False:
                        result = single_result.span(span)  # type: ignore # has incompatible type "str"; expected "Literal"
                    else:
                        result = single_result
                    break
            else:
                # continue the outer loop if the inner one was not broken
                continue
            break  # the inner loop was broken, break the outer one as well

    if result is None:
        result = arrow.get(time)

    if interval and not isinstance(result, (list, tuple)):
        return slice(result, result)
    elif isinstance(result, (list, tuple)):
        return slice(*result)
    else:
        return result


def is_relative_to(path: Path, other: Path) -> bool:
    """Implementation of Path.is_relative_to for python 3.8.

    TODO: Remove once python 3.8 is deprecated.
    """
    try:
        return path.is_relative_to(other)  # type: ignore  # python 3.8 compatibility
    except AttributeError:
        pass

    try:
        path.relative_to(other)
    except ValueError:
        return False
    else:
        return True
