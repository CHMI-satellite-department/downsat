from typing import TYPE_CHECKING
from pathlib import Path


if TYPE_CHECKING:
    from downsat.clients.eumdac import EumdacKey
    from downsat.clients.spacetrack import SpaceTrackKey


def test_downloading_data_msg(msg_archive_path: Path, eumdac_key: "EumdacKey") -> None:
    from downsat import MSG

    # --- downloading data
    key = eumdac_key

    msg = MSG(credentials=key, data_path=msg_archive_path, num_workers=3)
    msg = MSG.from_env(data_path=msg_archive_path)
    msg = MSG.from_env(credentials=key, data_path=msg_archive_path, num_workers=3)
    msg = MSG.from_env(data_path=msg_archive_path, num_workers=3)

    msg["202211041230"]  # download will take few minutes
    msg["202211041230"]  # will take few seconds

    # --- Bulk download
    msg["2022-11-04 12:30", "2022-11-05 12h"]

    # --- Narrowing down the search
    msg = MSG.from_env(data_path=msg_archive_path, sat="MSG4")

    from downsat import EumdacCollection

    EumdacCollection(name="MSG", credentials=key).collection.search_options


def test_downloading_data_metop(metop_archive_path: Path) -> None:
    # --- Downloading data to cover certain point of region of interest
    from downsat import Metop

    metop = Metop.from_env(data_path=metop_archive_path, area="eurol")
    metop["2022110411"]

    from downsat import LonLat

    metop = Metop.from_env(data_path=metop_archive_path, point=LonLat(lon=14.46, lat=50.0))
    metop["2022110411"]


def test_downloading_data_satpy(msg_archive_path: Path, is_satpy_available: bool) -> None:  # noqa: U100
    from downsat import MSG

    msg = MSG.from_env(data_path=msg_archive_path, sat="MSG4")

    # --- Satpy
    from satpy import Scene

    scene = Scene(filenames=msg["2022-11-04 12:30"], reader="seviri_l1b_native")
    assert isinstance(scene, Scene)

    # --- SatpyProduct
    from trollimage.xrimage import XRImage

    from downsat.satpy import SatpyProduct

    msg = MSG.from_env(data_path=msg_archive_path, num_workers=3)
    natural_color = SatpyProduct(msg, "natural_color", area="eurol")

    # get the product as trollimage.xrimage.XRImage
    image = natural_color["2022-11-04 12:30"]
    assert isinstance(image, XRImage)

    # get multiple products at once
    image1, image2 = natural_color["2022-11-04 12:30", "2022-11-04 12:45"]
    assert isinstance(image1, XRImage)
    assert isinstance(image2, XRImage)

    # --- SatpyScene
    from downsat.satpy import SatpyScene

    msg = MSG.from_env(data_path=msg_archive_path, num_workers=3, flatten=True)
    scn = SatpyScene(msg, reader="seviri_l1b_native", channels="IR_108", area="eurol")
    scene = scn["2022-11-04 12:30"]
    assert isinstance(scene, Scene)

    # --- SatpyScene - multiple scenes

    msg = MSG.from_env(data_path=msg_archive_path, num_workers=3, flatten=False)
    scenes = SatpyScene(msg, reader="seviri_l1b_native", channels="IR_108", area="eurol", flatten=False)

    scns = scenes["2022-11-04 12:30", "2022-11-04 12:45"]
    assert len(scns) == 2


def test_tle(tmp_path: Path, spacetrack_key: "SpaceTrackKey") -> None:
    from downsat import DailyTLE

    tle_archive = DailyTLE(object_id="METOP-B", credentials=spacetrack_key, data_path=tmp_path)  # type: ignore  # TODO: fix by mypy plugin
    tle = tle_archive["2023-06-21"]  # type: ignore

    assert isinstance(tle, tuple)
    assert len(tle) == 1
    assert str(tle[0]) == str(tmp_path / "METOP-B" / "2023-06-21")


def test_satellite_info() -> None:
    from downsat import satellite_info

    metop_a = satellite_info["METOP-A"]
    metop_a, metop_b = satellite_info["METOP-A", "METOP-B"]  # type: ignore


def test_find_visible_polar_passes(tmp_path: Path, spacetrack_key: "SpaceTrackKey") -> None:
    import datetime
    from functools import partial

    from downsat import DailyTLE, LonLat
    from downsat.query.polar import find_visible_polar_passes

    prague = LonLat(lon=14.41854, lat=50.07366)
    time = "2023-06-20 9:00"
    dt = datetime.timedelta(hours=1)  # time tolerance +- 1 hour
    tle_archive = partial(DailyTLE, credentials=spacetrack_key, data_path=tmp_path / "tle")

    metop_a_passes = find_visible_polar_passes(prague, time, tle_archive, satellite="METOP-A", dt=dt)
    assert metop_a_passes[0].datetime == datetime.datetime(
        2023, 6, 20, 8, 32, 24, 435207, tzinfo=datetime.timezone.utc
    )

    custom_swath_angle = 40  # [deg]
    metop_a_passes_custom = find_visible_polar_passes(
        prague, time, tle_archive, satellite="METOP-A", dt=dt, horizon=90 - custom_swath_angle
    )
    assert len(metop_a_passes_custom) == 0

    all_passes = find_visible_polar_passes(prague, time, tle_archive, dt=dt)
    assert len(all_passes) == 2
    assert "METOP-A" in all_passes
    assert "METOP-B" in all_passes
    assert all_passes["METOP-A"] == metop_a_passes
