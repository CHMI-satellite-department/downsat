from __future__ import annotations

from typing import Any, List
import datetime
from functools import cached_property
import os

from attrs import field, frozen
from attrs.validators import instance_of
import importlib_resources as resources
import spacetrack
from spacetrack import SpaceTrackClient
import yaml

from downsat.etl import abc
from downsat.etl.class_transforms import multikey_ds
from downsat.etl.metadata import setmeta
from downsat.etl.weakref import List as MetaList
from downsat.etl.weakref import MetaStr


# load configuration of spacetrack client
with resources.as_file(
    resources.files("downsat.config.clients") / "spacetrack.yaml"
) as spacetrack_config_file:
    with open(spacetrack_config_file, "rt", encoding="utf-8") as f:
        spacetrack_config = yaml.safe_load(f)


@frozen(slots=False)
class SpaceTrackKey:
    """Space-track.com client username and password."""

    # TODO: deduplicate code of SpaceTrackKey and EumdacKey

    username: str = field(validator=instance_of(str))
    password: str = field(validator=instance_of(str))

    @classmethod
    def from_env(
        cls,
        env_username: str = spacetrack_config["env_variables"]["spacetrack_username"],
        env_password: str = spacetrack_config["env_variables"]["spacetrack_password"],
    ) -> SpaceTrackKey:
        """Create SpaceTrackKey from environment variables.

        :param env_username: Name of environment variable holding space-track username. Default value is defined by 'env_variables'/'spacetrack_username' in spacetrack.yaml.
        :param env_password: Name of environment variable holding space-track password. Default value is defined by 'env_variables'/'spacetrack_password' in spacetrack.yaml.
        :returns: SpaceTrackCredentials.
        :raises KeyError: Environment variable with the username or password not found.
        """

        try:
            username = os.environ[env_username]
        except KeyError:
            raise KeyError(f'Env variable with spacetrack username "{env_username}" not found.')
        try:
            password = os.environ[env_password]
        except KeyError:
            raise KeyError(f'Env variable with spacetrack password "{env_password}" not found.')

        return cls(username=username, password=password)

    @property
    def credentials(self) -> tuple[str, str]:
        """Authentication credentials for eumdac client."""
        return (self.username, self.password)


@frozen(slots=False)
@multikey_ds
class DailyTLE(abc.DataSource[str, List[str]]):
    """TLE data for one day.

    :param object_id: NORAD catalog ID of the object or its name.
    :param credentials: Credentials for space-track.com client.
    """

    object_id: int | str = field(validator=instance_of((str, int)))
    credentials: SpaceTrackKey = field(validator=instance_of(SpaceTrackKey))

    @cached_property
    def client(self) -> SpaceTrackClient:
        return SpaceTrackClient(*self.credentials.credentials)

    def _json2tle(self, json_tle: dict[str, Any]) -> str:
        """Convert JSON TLE to string TLE.

        :param json_tle: JSON TLE.
        :returns: String TLE.
        """
        # convert TLE to string
        tle_line_keys = sorted(key for key in json_tle.keys() if key.startswith("TLE_LINE"))
        tle = MetaStr("\n".join([json_tle[tle_line_key].strip() for tle_line_key in tle_line_keys]))

        # add metadata
        tle_metadata = {k: v for k, v in json_tle.items() if not k.startswith("TLE_LINE")}
        setmeta(tle, **tle_metadata)

        return tle

    def __getitem__(self, key: str) -> list[str]:
        """Get TLE file for one day.

        :param day: Specification of the day.
        :returns: TLE data of the object for a given day.
        """
        try:
            date = datetime.date.fromisoformat(key)
        except (ValueError, TypeError) as e:
            raise KeyError(key) from e
        today = datetime.date.today()
        if date > today:
            raise KeyError(f"Future date {key}")

        if isinstance(self.object_id, str):
            object_id_kwargs: dict[str, str | int] = {"object_name": self.object_id}
        else:
            object_id_kwargs = {"norad_cat_id": self.object_id}

        # download TLE
        json_tles = self.client.tle(
            **object_id_kwargs,
            epoch=spacetrack.operators.inclusive_range(date, date + datetime.timedelta(days=1)),
            orderby="TLE_LINE1",
        )

        # convert to string + metadata
        json_tles = MetaList([self._json2tle(json_tle) for json_tle in json_tles])
        setmeta(json_tles, key=key, date=date, object_id=self.object_id)

        return json_tles
