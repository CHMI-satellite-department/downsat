from __future__ import annotations

from typing import TYPE_CHECKING
import datetime
from pathlib import Path

import pytest


if TYPE_CHECKING:
    from downsat import EumdacKey
    from downsat.etl import protocols


@pytest.mark.parametrize("channels", [["IR_108"], "IR_108"], ids=["channels_list", "channels_str"])
@pytest.mark.parametrize("area", [None, "germ"], ids=["no_area", "germ"])
def test_to_satpy_scene(
    msg_archive: "protocols.MultiKeyDataSource", area: str | None, channels: list[str] | str
) -> None:
    from satpy import Scene

    from downsat.data_sources.satpy import ToSatpyScene
    from downsat.etl.class_transforms import reduce

    scene_archive = reduce(msg_archive, ToSatpyScene)(reader="seviri_l1b_native", area=area, channels=channels)  # type: ignore  # TODO: fix by mypy plugin

    scn = scene_archive["202211011530"]
    assert isinstance(scn, Scene)
    assert scn["IR_108"].attrs["start_time"] == datetime.datetime(2022, 11, 1, 15, 30, 10, 169513)
    assert scn["IR_108"].attrs["end_time"] == datetime.datetime(2022, 11, 1, 15, 45, 10, 114296)

    # TODO: test that the scene was resampled


def test_to_satpy_scene_no_channels_to_resample() -> None:
    from downsat.data_sources.satpy import ToSatpyScene

    with pytest.raises(ValueError):
        ToSatpyScene(area="germ")


@pytest.mark.parametrize("area", [None, "germ"], ids=["no_area", "germ"])
def test_satpy_scene(msg_archive_path: Path, eumdac_key: "EumdacKey", area: str | None) -> None:
    from satpy import Scene

    from downsat import MSG
    from downsat.satpy import SatpyScene

    msg = MSG(eumdac_key, msg_archive_path)
    scene_archive = SatpyScene(msg, reader="seviri_l1b_native", area=area, channels=["IR_108"])

    scn = scene_archive["202211011530"]
    assert isinstance(scn, Scene)
    assert scn["IR_108"].attrs["start_time"] == datetime.datetime(2022, 11, 1, 15, 30, 10, 169513)
    assert scn["IR_108"].attrs["end_time"] == datetime.datetime(2022, 11, 1, 15, 45, 10, 114296)

    # TODO: test that the scene was resampled

    # multiple keys should return single scene if msg datasource is flattened
    scn = scene_archive["202211011530", "202211011545"]
    assert isinstance(scn, Scene)

    # multiple keys should return tuple of scenes if msg datasource is not flattened
    msg = MSG(eumdac_key, msg_archive_path, flatten=False)
    scene_archive = SatpyScene(msg, reader="seviri_l1b_native", area=area, channels=["IR_108"], flatten=False)
    scn = scene_archive["202211011530", "202211011545"]
    assert isinstance(scn, tuple)
    assert len(scn) == 2
    assert all(isinstance(s, Scene) for s in scn)


@pytest.mark.parametrize("composite", ["IR_108", "natural_color"], ids=["IR_108", "natural_color"])
@pytest.mark.parametrize("area", [None, "germ"], ids=["no_area", "germ"])
def test_to_satpy_product(
    msg_archive: "protocols.MultiKeyDataSource", area: str | None, composite: str
) -> None:
    from trollimage.xrimage import XRImage

    from downsat.data_sources.satpy import ToSatpyProduct, ToSatpyScene
    from downsat.etl.class_transforms import reduce
    from downsat.etl.metadata import getmeta
    from downsat.satpy import SatpyScene

    scene_archive = reduce(msg_archive, ToSatpyScene)(reader="seviri_l1b_native", area=area, channels=composite)  # type: ignore  # TODO: fix by mypy plugin
    product_archive = (scene_archive >> ToSatpyProduct)(composite=composite)  # type: ignore  # TODO: fix by mypy plugin

    image = product_archive["202211011530"]
    assert isinstance(image, XRImage)

    # the XRImage has metadata
    scene_archive = SatpyScene(msg_archive, reader="seviri_l1b_native", area=area, channels=composite)
    scn = scene_archive["202211011530"]
    assert getmeta(scn[composite]).items() <= getmeta(image).items()  # type: ignore
    assert len(getmeta(image)) > 0

    # TODO: test that the scene was resampled and the product looks as expected


@pytest.mark.parametrize("area", [None, "germ"], ids=["no_area", "germ"])
def test_satpy_product(msg_archive_path: Path, eumdac_key: "EumdacKey", area: str | None) -> None:
    from trollimage.xrimage import XRImage

    from downsat import MSG
    from downsat.satpy import SatpyProduct

    msg = MSG(eumdac_key, msg_archive_path)
    product_archive = SatpyProduct(msg, "natural_color", reader="seviri_l1b_native", area=area)

    prod = product_archive["202211011530"]
    assert isinstance(prod, XRImage)

    # TODO: test that the scene was resampled

    # multiple keys should return single scene if msg datasource is flattened
    # TODO: think more if it is OK that this produces tuple of XRImages instead of single XRImage
    # when doing the same on a Scene produces a single merged Scene
    # the reason is that SatpyProduct does SatpyScene >> ToSatpyProduct and the >> creates
    # independent processing branches for each key
    # TODO: fix - this behavior is clearly incorrect
    prod = product_archive["202211011530", "202211011545"]
    assert isinstance(prod, tuple)
    assert len(prod) == 2
    assert all(isinstance(s, XRImage) for s in prod)

    # multiple keys should return tuple of scenes if msg datasource is not flattened
    msg = MSG(eumdac_key, msg_archive_path, flatten=False)
    product_archive = SatpyProduct(msg, "natural_color", reader="seviri_l1b_native", area=area, flatten=False)
    prod = product_archive["202211011530", "202211011545"]
    assert isinstance(prod, tuple)
    assert len(prod) == 2
    assert all(isinstance(s, XRImage) for s in prod)


def test_cached_satpy_product(msg_archive: "protocols.MultiKeyDataSource", tmp_path: Path) -> None:
    from downsat.etl.metadata import getmeta
    from downsat.satpy import SatpyProduct

    product_archive_with_cache = SatpyProduct(
        msg_archive, reader="seviri_l1b_native", area="germ", composite="IR_108", cache_path=tmp_path
    )

    # single key getitem works
    prod = product_archive_with_cache["202211011530"]
    assert isinstance(prod, Path)
    assert prod.exists()
    assert prod.suffix == ".png"

    # PIL can read the image
    from PIL import Image

    img = Image.open(prod)
    assert img.size == (1024, 1024)

    # the output has metadata
    assert len(getmeta(prod)) > 0

    # multikey getitem works
    prods = product_archive_with_cache["202211011530", "202211011545"]
    assert isinstance(prods, tuple)
    assert len(prods) == 2
    assert all(isinstance(s, Path) for s in prods)
    assert all(s.exists() for s in prods)

    for prod in prods:
        img = Image.open(prod)
        assert img.size == (1024, 1024)
