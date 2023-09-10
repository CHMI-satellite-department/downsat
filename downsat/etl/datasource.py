from __future__ import annotations

from enum import Enum

from attrs import field, frozen
from attrs.validators import deep_iterable, instance_of

from downsat.etl import protocols, types
from downsat.etl.abc import MultiKeyDataSource
from downsat.etl.utils.data_manipulation import flatten


class KeySearchStrategy(str, Enum):
    """Strategy to search for keys in a MultiDataSource."""

    ALL = "all"
    FIRST = "first"


@frozen
class MultiDataSource(MultiKeyDataSource[types.KeyType_contra, types.OutputType_co]):
    """Multi-key data source composed of multiple sub-datasources.

    :param datasources: Tuple of sub-datasources.
    :param search_strategy: Strategy to search for keys in the sub-datasources. If `all`, all sub-datasources are searched for a key.
        and a flattened tuple of unique results is returned. If `first`, only the first occurence of a key is returned.

    :param datasources: Tuple of sub-datasources.
    """

    datasources: tuple[protocols.MultiKeyDataSource[types.KeyType_contra, types.OutputType_co], ...] = field(
        converter=tuple,
        validator=deep_iterable(
            member_validator=instance_of(protocols.MultiKeyDataSource), iterable_validator=instance_of(tuple)  # type: ignore
        ),
    )
    search_strategy: KeySearchStrategy = field(default=KeySearchStrategy.ALL)

    def __getitem__(
        self, key: types.KeyType_contra | tuple[types.KeyType_contra, ...]
    ) -> types.OutputType_co | tuple[types.OutputType_co, ...]:
        """Return one or more items from any of the sub-datasources.

        :param key: Index or id of the item or tuple of those.
        :returns: Item or items.
        """
        if isinstance(key, tuple):
            return tuple(self[k] for k in key)  # type: ignore # TODO: use mapping
        else:
            values = []
            for datasource in self.datasources:
                try:
                    values.append(datasource[key])
                except KeyError:
                    pass
                else:
                    if self.search_strategy == KeySearchStrategy.FIRST:
                        break

            if len(values) == 0:
                raise KeyError(key)
            else:
                return tuple(set(flatten(tuple(values), depth=1)))  # type: ignore  # TODO: fix
