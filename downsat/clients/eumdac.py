"""Connection to EUMETSAT Data Store"""
from __future__ import annotations

from typing import Any
from abc import ABC, abstractproperty
import datetime
from functools import cached_property
from io import BytesIO

from attrs import Attribute, field, frozen
from attrs.validators import deep_mapping, instance_of, optional
import eumdac
import importlib_resources as resources
from pyresample import AreaDefinition
from requests.exceptions import HTTPError


try:
    from satpy.resample import get_area_def
except ImportError:
    satpy_imported = False
else:
    satpy_imported = True
import yaml

from downsat.core.models import LonLat
from downsat.core.platform import FromEnvMixin
from downsat.core.utils import TimeSlotType, parse_time
from downsat.etl import abc
from downsat.etl.class_transforms import multikey_ds
from downsat.etl.metadata import setmeta


# load configuration of eumdac client
with resources.as_file(resources.files("downsat.config.clients") / "eumdac.yaml") as eumdac_config_file:
    with open(eumdac_config_file, "rt", encoding="utf-8") as f:
        eumdac_config = yaml.safe_load(f)


class EumdacCredentials(ABC):
    """Abstract class describing interface of Eumdac credentials."""

    def __attrs_post_init__(self) -> None:
        # access the token to see if it is valid (fail early if not)
        try:
            self.token.access_token
        except HTTPError as e:
            if e.response.status_code == 401:
                raise ValueError("Invalid eumdac token - unauthorized access.") from e
            else:
                raise RuntimeError(
                    f"Connection using eumdac access token failed with HTTP code {e.response.status_code}"
                ) from e

    @abstractproperty
    def credentials(self) -> tuple[str, str]:
        """Authentication credentials for eumdac client."""

    @cached_property
    def token(self) -> eumdac.token.AccessToken:
        """Convert self to eumdac access token."""
        return eumdac.AccessToken(self.credentials)


@frozen(slots=False)
class EumdacKey(FromEnvMixin, EumdacCredentials):
    """Eumdac client key and secret.

    Provides method `from_env` to instantiate using credentials stored in the environment.

    :param key: Eumdac client key.
    :param secret: Eumdac client secret.

    Example - instantiate from environment variables:
        >>> key = EumdacKey.from_env()

    Example - instantiate manually:
        >>> key = EumdacKey(key="some_eumdac_key", secret="some_eumdac_secret")
    """

    key: str = field(validator=instance_of(str))
    secret: str = field(validator=instance_of(str))

    @property
    def credentials(self) -> tuple[str, str]:
        """Authentication credentials for eumdac client."""
        return (self.key, self.secret)


@frozen(slots=False)
class EumdacUser(EumdacCredentials):
    """Eumdac user with his/her name, description and EumdacKey.

    :param name: User name.
    :param key: User's EumdacKey.
    :param description: Optional description of the user.
    """

    name: str = field(validator=instance_of(str))
    key: EumdacKey = field(validator=instance_of(EumdacKey))
    description: str | None = field(default=None, validator=optional(instance_of(str)))

    @property
    def credentials(self) -> tuple[str, str]:
        """Authentication credentials for eumdac client."""
        return self.key.credentials


def _convert_eumdac_query_params(query_params: dict[str, Any]) -> dict[str, Any]:
    """Convert query parameters defined in this module to a format known by eumdac client.

    Exclusive parameters defining spatial region to be covered by the data:
        geo: WKT definition of an geographical extent
        bbox: lon lat of a bounding box
        point: LatLon definition of a point to be covered
        area: AreaDef or satpy area name

    :param query_params: Query parameters as kwargs.
    """

    if "area" in query_params:
        if "bbox" in query_params:
            raise ValueError("Query parameters `area` and `bbox` are exclusive.")

        area_def_param = query_params["area"]
        if isinstance(area_def_param, str):
            if not satpy_imported:
                raise ImportError(
                    "Missing `satpy` dependency for parsing string area definition. Please install downsat[pytroll]."
                )
            area_def = get_area_def(area_def_param)
        elif isinstance(area_def_param, AreaDefinition):
            area_def = area_def_param
        else:
            raise TypeError("`area` must be string or pyresample.AreaDefinition")

        # convert to format requested by eumdac client
        query_params["bbox"] = ", ".join(str(coord) for coord in area_def.area_extent_ll)
        del query_params["area"]

    if "point" in query_params:
        if "geo" in query_params:
            raise ValueError("Query parameters `point` and `geo` are exclusive.")

        point = query_params["point"]
        if not isinstance(point, LonLat):
            raise TypeError("Query parameter `point` must have type `LonLat`.")

        # convert to WKT format requested by eumdac client
        query_params["geo"] = f"POINT({point.lon} {point.lat})"
        del query_params["point"]

    return query_params


@frozen(slots=False)
@multikey_ds
class EumdacCollection(abc.DataSource[str, BytesIO]):
    """Eumdac data collection with dict-like access to the data by data name.

    :param name: Name of Eumdac collection or alias defined in eumdac.yaml.
    :param credentials: Credentials used to connect to Eumetsat Data Store using eumdac library.
    :param query_params: Query parameters passed to the eumdac.collection.search.
    :param aliases: Definition of aliases for eumdac collection names.
    """

    name: str = field(validator=instance_of(str))
    credentials: EumdacCredentials = field(
        validator=instance_of(EumdacCredentials)  # type: ignore  # check instance of an abstract class
    )

    @credentials.validator
    def is_token_valid(self, attribute: Attribute, credentials: EumdacCredentials) -> None:  # noqa: U100
        """Access eumdac token to check that it is valid."""
        credentials.token

    query_params: dict[str, Any] = field(
        factory=dict,
        converter=_convert_eumdac_query_params,
        validator=deep_mapping(
            key_validator=instance_of(str),
            value_validator=lambda _0, _1, _2: None,  # type: ignore
            mapping_validator=instance_of(dict),
        ),
        kw_only=True,
    )  # TODO: validate query keys during init
    aliases: dict[str, str] = field(
        default=eumdac_config["alias"].copy(),
        validator=deep_mapping(
            key_validator=instance_of(str),
            value_validator=lambda _0, _1, _2: None,  # type: ignore
            mapping_validator=instance_of(dict),
        ),
        kw_only=True,
    )

    @property
    def collection_name(self) -> str:
        """Get full eumdac collection name."""
        return self.aliases.get(self.name, self.name)

    @property
    def datastore(self) -> eumdac.datastore.DataStore:
        """Get instance of eumdac DataStore."""
        return eumdac.DataStore(self.credentials.token)

    @property
    def collection(self) -> eumdac.collection.Collection:
        """Get instance of eumdac data Collection."""
        return self.datastore.get_collection(self.collection_name)

    def query(self, **kwargs: Any) -> tuple[str, ...]:
        """Search for data fulfilling given conditions.

        :param kwargs: Search parameters used to update `query_param` defined during class init.
        :returns: Tuple of data identifiers that can be used with __getitem__ to download the data.
        """
        query_params = self.query_params.copy()
        query_params.update(kwargs)
        query_params = _convert_eumdac_query_params(query_params)
        products = self.collection.search(**query_params)

        return tuple(product.metadata["properties"]["identifier"] for product in products)

    def _load_data_by_id(
        self,
        item: str,
    ) -> BytesIO:
        """Load data identified by their id from eumdac collection.

        :param item: Id of data to be downloaded.
        :returns: In-memory buffe containing the data. Data name provided by eumdac is stored in buffer.name property.
        """
        products = [product for product in self.collection.search(title=item)]
        if len(products) == 0:
            raise KeyError(f"{item}")
        elif len(products) > 1:
            raise RuntimeError(f"found duplicate records for {item=}")

        buffer = BytesIO()
        with products[0].open() as stream:
            buffer.write(stream.read())
            buffer.seek(0)
        setmeta(buffer, name=item)

        return buffer

    def __getitem__(self, item: str) -> BytesIO:
        """Get data specified by their id(s).

        :param item: Id of data to be downloaded.
        :returns: In-memory buffer containing the data. Data name provided by eumdac is stored in buffer.name property.
        """
        return self._load_data_by_id(item=item)

    @staticmethod
    def _datetime2eumdac_query_params(key: TimeSlotType) -> dict[str, Any]:
        """Transform datetime to eumdac query params"""
        time_slice = parse_time(key, interval=True)
        if time_slice.start is not None:
            # remove seconds from dtstart
            dtstart = time_slice.start.datetime.replace(second=0, microsecond=0)
        else:
            dtstart = None
        if time_slice.stop is not None:
            # remove seconds from dtend
            dtend = time_slice.stop.datetime.replace(second=0, microsecond=0)

            # dtend must be at least one minute after dtstart
            if dtstart is not None:
                if dtend == dtstart:
                    # eumdac does not like dtstart == dtend so we add one minute to dtend
                    dtend = dtstart + datetime.timedelta(minutes=1)
                elif dtend < dtstart:
                    raise ValueError(f"{dtend=} must be greater than {dtstart=}")
        else:
            dtend = None

        return {"dtstart": dtstart, "dtend": dtend}
