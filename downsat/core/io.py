from __future__ import annotations

from typing import Any

from attrs import asdict, fields
from cattrs.converters import BaseConverter
from cattrs.preconf.pyyaml import make_converter as make_yaml_converter
from cattrs.strategies import include_subclasses

from downsat.core.models import OnboardInstrument, Satellite
from downsat.core.platform import Platform, RegisteredComponent


def _satellite_union_strategy(union: Any, converter: BaseConverter) -> None:
    """Disambiguate Satellite subclasses when structuring from a mapping.

    Polar satellites may omit ``instruments`` (defaults to ``{}``), so cattrs'
    default unique-required-field disambiguation cannot use ``instruments``.
    """

    def pick_class(val: Any) -> type:
        if not isinstance(val, dict):
            raise TypeError(f"Expected mapping for satellite config, got {type(val).__name__}")
        from downsat.core.models import GeostationarySatellite, PolarSatellite

        if "latitudes" in val:
            return GeostationarySatellite
        if "instruments" in val:
            return PolarSatellite
        return Satellite

    def structure_satellite_union(val: Any, _: Any) -> Any:
        return converter.structure(val, pick_class(val))

    converter.register_structure_hook(union, structure_satellite_union)


yaml_converter = make_yaml_converter()
include_subclasses(Satellite, yaml_converter, union_strategy=_satellite_union_strategy)
include_subclasses(OnboardInstrument, yaml_converter)


# Note: the following canonical way below is unfortunately not compatible with python 3.9
#       => we use custom hook instead
# register structure hook for `type` and `function` used in RegisteredComponent
# def structure_type(value: Any, _: type) -> type:
#     if not isinstance(value, type):
#         raise TypeError(f"Expected type, got {type(value)}")
#
#     return value
#
#
# def structure_function(value: Any, _: FunctionType) -> FunctionType:
#     if not isinstance(value, FunctionType):
#         raise TypeError(f"Expected function, got {type(value)}")
#
#     return value
#
#
# yaml_converter.register_structure_hook(type, structure_type)
# yaml_converter.register_structure_hook(FunctionType, structure_function)  # type: ignore  # TODO: fix?
# yaml_converter.register_structure_hook(
#     Union[type, FunctionType],
#     lambda v, t: structure_type(v, t) if isinstance(v, type) else structure_function(v, t),  # type: ignore  # TODO: fix?
# )
#
#
# skip init=False attributes during unstructuring
# platform_unstructure_hook = make_dict_unstructure_fn(
#     Platform, yaml_converter, _instantiated_classes=override(omit=True)
# )
# yaml_converter.register_unstructure_hook(Platform, platform_unstructure_hook)


def unstructure_attrs_without_init_false(obj: Any) -> dict[str, Any]:
    obj_dict = asdict(obj)

    # drop init=False attributes
    for f in fields(obj.__class__):
        if not f.init:
            del obj_dict[f.name]

    return obj_dict


def structure_platform(platform_dict: dict[str, Any], _: type[Platform]) -> Platform:
    platform_dict["registered_components"] = {
        k: RegisteredComponent(**v) for k, v in platform_dict["registered_components"].items()
    }
    return Platform(**platform_dict)


yaml_converter.register_unstructure_hook(Platform, unstructure_attrs_without_init_false)
yaml_converter.register_structure_hook(Platform, structure_platform)
