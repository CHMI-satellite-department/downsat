__version__ = "0.0.1"

from downsat.clients.eumdac import EumdacCollection, EumdacKey, EumdacUser
from downsat.clients.spacetrack import SpaceTrackKey
from downsat.core.models import LonLat
from downsat.core.platform import Platform
from downsat.data_sources.eumetsat import MSG, RSS, Metop
from downsat.data_sources.satellite_info import (
    SatelliteInfo,
    satellite_info,
    satellite_info_geo,
    satellite_info_leo,
)
from downsat.data_sources.satpy import SatpyProduct, SatpyScene
from downsat.data_sources.tle import DailyTLE


__all__ = [
    "DailyTLE",
    "EumdacKey",
    "EumdacUser",
    "EumdacCollection",
    "LonLat",
    "Metop",
    "MSG",
    "Platform",
    "RSS",
    "SatelliteInfo",
    "SpaceTrackKey",
    "satellite_info_geo",
    "satellite_info_leo",
    "satellite_info",
    "SatpyProduct",
    "SatpyScene",
]
