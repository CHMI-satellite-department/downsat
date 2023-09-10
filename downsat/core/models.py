from __future__ import annotations

from typing import Optional
import datetime

from attrs import converters, field, frozen
from attrs.validators import deep_mapping, instance_of, optional


@frozen(slots=False)
class LonLat:
    lon: float = field(converter=float)
    lat: float = field(converter=float)


@frozen(slots=False)
class OnboardInstrument:
    """Base class for representation of a satellite onboard instrument.

    :param model: Model name of the instrument. Optional
    """

    model: Optional[str] = field(default=None, validator=optional(instance_of(str)), kw_only=True)


@frozen(slots=False)
class PolarInstrument(OnboardInstrument):
    """Representation of a polar satellite onboard instrument.

    :param swath_angle: Half angle of the scanner in degrees.
    """

    swath_angle: float = field(converter=float)

    @property
    def min_swath_angle(self) -> float:
        """Minimum swath angle in degrees."""
        return self.swath_angle

    @property
    def max_swath_angle(self) -> float:
        """Maximum swath angle in degrees."""
        return self.swath_angle


@frozen(slots=False)
class CrossTrackScanner(PolarInstrument):
    """Representation of a cross-track scanner.

    :param scan_rate: Scanning frequency [Hz]
    """

    scan_rate: float = field(converter=float)


@frozen(slots=False)
class Satellite:
    """Base class for representation of a satellite."""

    norad_id: int = field(converter=int)


@frozen(slots=False)
class PolarSatellite(Satellite):
    """Representation of a polar satellite."""

    instruments: dict[str, PolarInstrument] = field(
        converter=dict,
        validator=deep_mapping(key_validator=instance_of(str), value_validator=instance_of(PolarInstrument)),
    )

    @property
    def max_swath_angle(self) -> float:
        """Maximum swath angle of all instruments in degrees."""
        return max(instrument.max_swath_angle for instrument in self.instruments.values())

    @property
    def min_swath_angle(self) -> float:
        """Minimum swath angle of all instruments in degrees."""
        return min(instrument.min_swath_angle for instrument in self.instruments.values())


@frozen(slots=False)
class GeostationarySatellite(Satellite):
    """Representation of a geostationary satellite.

    :param latitudes: Time series of satellite's positions. Each point represents a time of change and a new position
    """

    latitudes: tuple[tuple[datetime.datetime, float], ...] = field(
        converter=tuple
    )  # time series of satellite's position  # TODO: sort, validate


@frozen(slots=False)
class Satellites:
    """Representation of a group of polar satellites."""

    leo: dict[str, PolarSatellite] = field(
        factory=dict,  # type: ignore  # TODO: fix
        converter=converters.optional(dict),
        validator=deep_mapping(key_validator=instance_of(str), value_validator=instance_of(PolarSatellite)),
    )
    geo: dict[str, GeostationarySatellite] = field(
        factory=dict,  # type: ignore  # TODO: fix
        converter=converters.optional(dict),
        validator=deep_mapping(
            key_validator=instance_of(str), value_validator=instance_of(GeostationarySatellite)
        ),
    )
