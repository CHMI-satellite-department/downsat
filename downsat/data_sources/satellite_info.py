from attrs import field, frozen
from functools import cached_property
from importlib_resources import files

from downsat.core.io import yaml_converter
from downsat.core.models import Satellite, Satellites
from downsat.etl.class_transforms import multikey_ds
from downsat.etl.protocols import DataSource


@frozen  # (slots=False)
@multikey_ds
class SatelliteInfo(DataSource[str, Satellite]):
    """Data source for satellite information.

    The data is loaded from a simple database stored in yaml file `downsat.config.satellites.yaml`.

    :param leo: If True, load LEO satellites. Default is True.
    :param geo: If True, load GEO satellites. Default is True.
    """

    leo: bool = field(default=True)
    geo: bool = field(default=True)
    satellites: Satellites = field(init=False)

    def __attrs_post_init__(self) -> None:
        """Load satellite information from yaml file."""
        with files("downsat.config").joinpath("satellites.yaml").open(  # TODO: make this configurable
            "rt", encoding="utf-8"
        ) as f:
            object.__setattr__(self, "satellites", yaml_converter.loads(f.read(), Satellites))
        
        # TODO: check uniqueness of NORAD IDs

    def __getitem__(self, name: str | int) -> Satellite:
        """Get satellite by name or NORAD ID."""
        # TODO: cache
        for satellite in self._all_satellites:
            if name in satellite.names:
                return satellite
            if isinstance(name, int) and satellite.norad_id == name:
                return satellite
        raise KeyError(f"Satellite {name} not found.")

    @cached_property
    def _all_satellites(self) -> list[Satellite]:
        """Get list of all satellites."""
        satellites_to_check = []
        if self.leo:
            satellites_to_check.extend(self.satellites.leo)
        if self.geo:
            satellites_to_check.extend(self.satellites.geo)
        return satellites_to_check

    def keys(self) -> tuple[str, ...]:
        """Get names of all satellites."""
        keys: tuple[str, ...] = ()
        for satellite in self._all_satellites:
            keys += satellite.names
            keys += (satellite.norad_id,)
        return keys


    def name_to_norad_id(self, name: str) -> int:
        """Convert satellite name to NORAD ID.
        
        raises KeyError if satellite name is not found.
        """
        return self[name].norad_id


satellite_info = SatelliteInfo()
satellite_info_leo = SatelliteInfo(leo=True, geo=False)
satellite_info_geo = SatelliteInfo(leo=False, geo=True)
