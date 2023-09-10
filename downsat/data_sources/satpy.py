from __future__ import annotations

from typing import TYPE_CHECKING, Any, Union
import datetime
from functools import partial
from pathlib import Path

from attrs import Attribute, asdict, field, frozen
from attrs.validators import deep_mapping, instance_of, min_len, optional
from diskcache import Cache
from trollsift import Parser


try:
    from satpy import Scene
    from satpy.writers import get_enhanced_image
except ImportError:
    raise ImportError("Missing `satpy` dependency. Please install downsat[pytroll].")

from downsat.core.cache import LockableCache
from downsat.core.file_storage import _FileDataset
from downsat.core.lock import DiskCacheRLock, FileRLock
from downsat.etl import protocols, types
from downsat.etl.abc import PipelineTransform
from downsat.etl.class_transforms import cache, reduce
from downsat.etl.metadata import keepmeta, setmeta
from downsat.etl.transforms import Flatten
from downsat.etl.weakref import Path as MetaPath


if TYPE_CHECKING:
    from trollimage.xrimage import XRImage


def convert_channels_to_list(channels: None | str | list[str]) -> None | list[str]:
    if channels is None:
        return None

    if isinstance(channels, str):
        return [channels]
    else:
        return channels


@frozen(slots=False)
class ToSatpyScene(
    PipelineTransform[Union[Path, tuple[Path, ...]], Scene]
):  # TODO: just Path and let PathTransform handle Path-> tuple[Path, ...]?
    """Transform from satellite filenames to satpy Scene.

    :param reader: The name of the reader to use. If not given, the reader is
        determined from the filenames automatically by satpy.
    :param channels: The names of the channels to load. If not given, all channels.
    :param area: The name of the area (in satpy conventions) used to resample the scene.
        If not given, no resampling is done. If given, the `channels` to resample must be
        specified.
    :param resample_kwargs: Keyword arguments passed to the `resample` method of the scene.
    """

    reader: str | None = field(default=None, kw_only=True, validator=optional(instance_of(str)))
    channels: list[str] | None = field(
        default=None, kw_only=True, converter=convert_channels_to_list, validator=[optional(instance_of(list)), optional(min_len(1))]  # type: ignore  # attrs issue
    )
    area: str | None = field(
        default=None, kw_only=True
    )  # TODO: validate against areas known to satpy; make type annotation consistent with satpy type annotation
    resample_kwargs: dict[str, Any] = field(
        factory=dict,
        kw_only=True,
        converter=dict,
        validator=deep_mapping(key_validator=instance_of(str), value_validator=lambda x: True),  # type: ignore  # noqa: U100
    )

    @area.validator
    def _channels_to_resample_exist(self, attribute: Attribute, area: str | None) -> None:  # noqa: U100
        if area is not None and self.channels is None:
            raise ValueError(
                "Cannot resample without specifying channels to resample. Please set the `channels` attribute."
            )

    def __call__(self, inp: Path | tuple[Path, ...]) -> Scene:
        if not isinstance(inp, tuple):
            inp = (inp,)

        scn = Scene(filenames=inp, reader=self.reader)
        if self.channels is not None:
            scn.load(self.channels)

        if self.area is not None:
            scn = scn.resample(self.area, **self.resample_kwargs)

        # add self attributes to metadata, rename some
        new_metadata = asdict(self)
        new_metadata["channels"] = new_metadata.pop("loaded_channels", None)
        setmeta(scn, **new_metadata)

        return scn


@frozen(slots=False)
class ToSatpyProduct(PipelineTransform[Scene, "XRImage"]):
    """Transform satpy Scene to a product image and return the image as a xrimage.

    :param composite: The name of the composite to use.
    """

    composite: str = field(validator=instance_of(str))

    def __call__(self, inp: Scene) -> "XRImage":
        # generate image
        image = keepmeta(get_enhanced_image)(inp[self.composite])

        # add metadata
        setmeta(image, **asdict(self))

        return image


def SatpyScene(
    source: protocols.MultiKeyDataSource[types.KeyType, Path]
    | protocols.MultiKeyDataSource[types.KeyType, list[Path]],
    reader: str | None = None,
    channels: str | list[str] | None = None,
    area: str | None = None,
    flatten: bool = True,
    resample_kwargs: dict[str, Any] | None = None,
) -> protocols.MultiKeyDataSource[types.KeyType, Union[Scene, tuple[Scene]]]:
    """Create a pipeline transform that converts satellite filenames to satpy Scenes.

    :param source: The data source of satellite filenames, e.g. MSG(key, data_path).
    :param reader: The name of the reader to use. If not given, the reader is infered by satpy.
    :param channels: The names of the channels to load. If not given, all channels.
    :param area: The name of the area (in satpy conventions) used to resample the scene.
        If not given, no resampling is done. If given, the `channels` to resample must be
        specified.
    :param flatten: Whether the result should be a single Scene or a tuple of Scenes if
        multiple keys are given.
    :param resample_kwargs: Keyword arguments passed to the `resample` method of the scene.

    Example:
    >>> msg = MSG(key, data_path, flatten=False)
    >>> scn = SatpyScene(msg, reader="seviri_l1b_native", channels=["IR_108"], flatten=False)
    >>>
    >>> scn["202211011530", "202211011545"]
    """
    resample_kwargs = resample_kwargs or {}

    if flatten:
        return reduce(source, ToSatpyScene)(  # type: ignore  # TODO: fix
            reader=reader, channels=channels, area=area, resample_kwargs=resample_kwargs  # type: ignore  # TODO: fix
        )
    else:
        # TODO: why source >> ToSatpyScene(reader=reader, channels=channels, area=area, resample_kwargs=resample_kwargs) does not work?
        return (source >> ToSatpyScene)(  # type: ignore  # TODO: fix by mypy plugin
            reader=reader, channels=channels, area=area, resample_kwargs=resample_kwargs
        )


# @multikey_ds  # TODO: make it multikey
@frozen(slots=False)
class _ProductFileDataset(_FileDataset):
    """File dataset with keys transformed to partial keys during __getitem__, __setitem__ and __delitem__.

    The partial keys are generated by composing the key_pattern with the key.

    # TODO: This is a hack to allow partial keys in FileDataset. Remove when >> produces ModifiedInputDataset.
    """

    def _transform_key(self, key: str) -> str:
        return self.filename_pattern.compose(  # type: ignore  # used only from SatpyProduct -> filename_patter always present
            {"start_time": datetime.datetime.strptime(key, "%Y%m%d%H%M")}, allow_partial=True
        )

    def __getitem__(self, key: str) -> set[MetaPath]:
        return super().__getitem__(self._transform_key(key))

    def __setitem__(self, key: str, value: Any) -> None:
        return super().__setitem__(self._transform_key(key), value)

    # __delitem is never used by the user and since FileDataset.__setitem__ actually calls __delitem__,
    # the key would get converted twice
    # TODO: This whole class is a hack and its purpose should be achieved by implementing _transform_key >> FileDataset
    #       that supports __getitem__, __setitem__, __delitem__, so we just don't implement __delitem__ here.
    # def __delitem__(self, key: str) -> None:
    #     return super().__delitem__(self._transform_key(key))


def SatpyProduct(
    source: protocols.MultiKeyDataSource[types.KeyType, Path]
    | protocols.MultiKeyDataSource[types.KeyType, list[Path]],
    composite: str,
    reader: str | None = None,
    area: str | None = None,
    flatten: bool = True,
    cache_path: Path | str | None = None,
    filename_pattern: str | None = None,
    network_filesystem: bool = True,
    resample_kwargs: dict[str, Any] | None = None,
) -> protocols.MultiKeyDataSource[types.KeyType, Union["XRImage", tuple["XRImage"]]]:
    """Create a pipeline transform that converts satellite filenames to product images.

    :param source: The data source of satellite filenames, e.g. MSG(key, data_path).
    :param composite: The name of the composite to use.
    :param area: The name of the area (in satpy conventions) used to resample the scene.
        If not given, no resampling is done. If given, the `channels` to resample must be
        specified.
    :param flatten: Flag that should be consistent with the `flatten` flag of the `source` data source.
    :param cache_path: The path to the cache directory where to store the results.
        If not given, no caching is done.
    :param filename_pattern: The pattern of the filenames in the cache. Default: "{start_time:%Y%m%d%H%M}-{composite}-{area}.png"
        where "{area}" may be None. The pattern is formatted with the metadata of the image that correspond
        to satpy Scene attrs + the value of composite and area provided to this function. The {start_time} placeholder is replaced
        with the value of __getitem__/__setitem__/__delitem__ key.
    :param network_filesystem: Whether to use lock compatible with NFS network filesystem, but less efficient, (default, True) or
        more efficient lock that may fail on network filesystems like NFS and cause database corruption (False).
        The reason why not to use efficient diskcache by default is incompatibility of SQLite with network filesystems.
        **Important:** All processes that access the same cache must use the same value of this parameter otherwise
        database corruption may occur.
    :param resample_kwargs: Keyword arguments passed to the `resample` method of the scene.

    Example:
    >>> msg = MSG(key, data_path, flatten=False)
    >>> scn = SatpyScene(msg, 'HRV', reader="seviri_l1b_native", channels=["IR_108"], flatten=False)
    >>>
    >>> scn["202211011530", "202211011545"]

    # TODO: now when the user requests two times from a single time slots, they are saved twice - use query to avoid this
    # TODO: allow {satellite} placeholder to allow storing data from multiple satellites in a single folder
    # TODO: when cache_path is supplied, user should get Dataset with __delitem__ method that deletes the file from cache
    """

    scene_archive = SatpyScene(
        source, reader=reader, area=area, channels=composite, flatten=flatten, resample_kwargs=resample_kwargs
    )  # TODO: infer flatten from source so that user does not have to keep it consistent manually
    et = scene_archive >> ToSatpyProduct  # type: ignore  # TODO: fix by mypy plugin

    if cache_path is not None:
        if network_filesystem:
            lock_class = FileRLock
            cache_class = LockableCache
        else:
            lock_class = DiskCacheRLock  # type: ignore  # TODO: fix
            cache_class = Cache  # type: ignore  # TODO: fix

        # prepare filename_pattern
        filename_pattern = filename_pattern or "{start_time:%Y%m%d%H%M}-{composite}-{area}.png"
        # fill in area in string format or None, must use Parser to allow partial filling
        filename_pattern = Parser(filename_pattern).compose(
            {"area": area, "composite": composite}, allow_partial=True
        )

        # add cache
        etl = cache(
            et,
            cache=partial(
                _ProductFileDataset,
                data_path=cache_path,
                lock_class=lock_class,
                cache_class=cache_class,  # type: ignore  # TODO: why? fix
                filename_pattern=filename_pattern,
            ),
        )
        etl = reduce(reduce(etl, partial(Flatten, depth=1)), lambda x: x.pop() if len(x) == 1 else x)  # type: ignore  # TODO: fix
    else:
        etl = et

    return etl(composite=composite)  # type: ignore  # TODO: fix
