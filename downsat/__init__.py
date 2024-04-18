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


try:
    from downsat.data_sources.satpy import (  # TODO: this is deprecated -> remove; users should import this from downsat.satpy
        SatpyProduct,
        SatpyScene,
    )
except ImportError:
    _satpy_available = False
else:
    _satpy_available = True
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
]

if _satpy_available:
    __all__ += ["SatpyProduct", "SatpyScene"]
