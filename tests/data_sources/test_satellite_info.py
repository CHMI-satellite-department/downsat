import pytest


def test_can_get_satellite() -> None:
    from downsat.core.models import Satellite
    from downsat.data_sources.satellite_info import satellite_info, satellite_info_geo, satellite_info_leo

    # can get single satellite
    metop_a = satellite_info["METOP-A"]

    assert isinstance(metop_a, Satellite)
    assert metop_a.min_swath_angle == metop_a.max_swath_angle == 55.37  # type: ignore
    assert len(metop_a.instruments) > 0  # type: ignore
    assert "AVHRR" in metop_a.instruments  # type: ignore

    # can get multiple satellites at once
    metop_a, metop_b = satellite_info["METOP-A", "METOP-B"]  # type: ignore  # TODO: fix

    assert metop_a.norad_id == 29499
    assert metop_b.norad_id == 38771  # type: ignore

    # satellite_info leo should work
    metop_a, metop_b = satellite_info_leo["METOP-A", "METOP-B"]  # type: ignore
    assert metop_a.norad_id == 29499
    assert metop_b.norad_id == 38771  # type: ignore

    # there's no geosttionary satellite yet
    with pytest.raises(KeyError):
        satellite_info_geo["METEOSAT-11"]
