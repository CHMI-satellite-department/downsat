from __future__ import annotations

import datetime

import arrow
import pytest


def test_parse_time_none() -> None:
    from downsat.core.utils import parse_time

    assert parse_time(None) is None
    assert parse_time(None, interval=True) == slice(None, None)
    assert parse_time(None, interval=False) is None
    assert parse_time(slice(None, None)) == slice(None, None)
    assert parse_time(slice(None, None), interval=True) == slice(None, None)
    assert parse_time(slice(None, None), interval=False) is None


@pytest.mark.parametrize(
    "time",
    [datetime.datetime.now(), "2022-11-04T15:30:59", arrow.get("2022-11-04T15:30:59")],
    ids=["datetime", "str", "arrow"],
)
def test_parse_time_single_time(time: str | datetime.datetime) -> None:
    import arrow

    from downsat.core.utils import parse_time

    ref_time = arrow.get(time)

    assert parse_time(time) == ref_time
    assert parse_time(time, interval=True) == slice(ref_time, ref_time)
    assert parse_time(time, interval=False) == ref_time


@pytest.mark.parametrize(
    "time",
    [
        slice("2022-11-04T15:30:00", "2022-11-04T15:50:00"),
        slice(arrow.get("2022-11-04T15:30:00"), arrow.get("2022-11-04T15:50:00")),
        slice("2022-11-04T15:30:00", arrow.get("2022-11-04T15:50:00")),
    ],
    ids=["str+str", "arrow+arrow", "str+arrow"],
)
def test_parse_explicit_time_interval(time: slice) -> None:
    import arrow

    from downsat.core.utils import parse_time

    ref_time = slice(arrow.get(time.start), arrow.get(time.stop))

    assert parse_time(time) == ref_time
    assert parse_time(time, interval=True) == ref_time
    with pytest.raises(ValueError):
        parse_time(time, interval=False)
    assert parse_time(slice(time.start, time.start), interval=False) == ref_time.start


@pytest.mark.parametrize(
    "time,span",
    [("2022-11", "month"), ("20221104", "day"), ("1.1.2021 16h", "hour"), ("16:30 1.1.2021", "minute")],
)
def test_parse_implicit_time_interval(time: str, span: str) -> None:
    import arrow

    from downsat.core.utils import parse_time

    time_range = parse_time(time)
    assert isinstance(time_range, slice)
    assert time_range.start.span(span)[1] == time_range.stop

    single_time = parse_time(time, interval=False)
    assert isinstance(single_time, arrow.Arrow)
