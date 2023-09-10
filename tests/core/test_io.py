from __future__ import annotations

from typing import TYPE_CHECKING

from pytest_cases import parametrize_with_cases


if TYPE_CHECKING:
    from downsat import Platform


def case_platform() -> "Platform":
    from downsat import Platform

    return Platform()


@parametrize_with_cases("test_object", cases=".", prefix="case_")
def test_structure_unstructure(test_object: type) -> None:
    from attrs import fields

    from downsat.core.io import yaml_converter

    # unstructure
    test_object_unstructured = yaml_converter.unstructure(test_object)

    # structure
    test_object_structured = yaml_converter.structure(test_object_unstructured, test_object.__class__)

    # compare
    assert test_object == test_object_structured

    # all init=True attributes should be present, and no init=False attributes
    for field in fields(test_object.__class__):  # type: ignore  #  Argument 1 to "fields" has incompatible type "Type[type]"; expected "Type[AttrsInstance]"
        if field.init:
            assert field.name in test_object_unstructured
        else:
            assert field.name not in test_object_unstructured
