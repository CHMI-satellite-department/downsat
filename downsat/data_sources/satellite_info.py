from attrs import field, frozen
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

    def __getitem__(self, name: str) -> Satellite:
        """Get satellite by name."""
        if self.leo:
            try:
                return self.satellites.leo[name]  # type: ignore
            except KeyError:
                pass
        if self.geo:
            try:
                return self.satellites.geo[name]  # type: ignore
            except KeyError:
                pass

        raise KeyError(f"Sattelite {name} not found.")

    def keys(self) -> tuple[str, ...]:
        """Get names of all satellites."""
        keys: tuple[str, ...] = ()
        if self.leo:
            keys += tuple(self.satellites.leo.keys())
        if self.geo:
            keys += tuple(self.satellites.geo.keys())
        return keys


satellite_info = SatelliteInfo()
satellite_info_leo = SatelliteInfo(leo=True, geo=False)
satellite_info_geo = SatelliteInfo(leo=False, geo=True)
