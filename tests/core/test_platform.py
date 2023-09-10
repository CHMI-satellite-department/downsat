from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "default_parameters", [set(), {"a", "b"}], ids=["no_default_parameters", "with_default_parameters"]
)
def test_class_instantiation(default_parameters: set[str]) -> None:
    """Test instantiation of Platform class."""
    from attrs import field, frozen

    from downsat import Platform

    platform = Platform()

    @frozen(slots=False)
    class TestClass:
        a: int = field()
        b: int = field()

    params = {"a": 1, "b": 2}

    platform.register(TestClass, ("a", "b"), {k: v for k, v in params.items() if k in default_parameters})

    # instantiate with all parameters set
    assert platform.get("TestClass", params) == TestClass(a=1, b=2)

    # instantiate with all parameters overridden
    assert platform.get("TestClass", {"a": 3, "b": 4}) == TestClass(a=3, b=4)

    # instantiate with only required parameters set
    assert platform.get(
        "TestClass", {k: params[k] for k in params if k not in default_parameters}
    ) == TestClass(a=1, b=2)

    # instantiate from type
    assert platform.get(
        TestClass, {k: params[k] for k in params if k not in default_parameters}
    ) == TestClass(a=1, b=2)


def test_loading_env_parameters() -> None:
    """Test that Platform can load parameters from environment variables."""
    import os

    from downsat import Platform

    os.environ["PLATFORM_TEST"] = "1"
    platform = Platform(env_parameter_names={"test.env": "PLATFORM_TEST"})

    # platform respects `env_parameter_names`
    assert platform["test.env"] == "1"

    # platform can load unregistered parameters
    assert platform["platform.test"] == "1"


def test_from_env() -> None:
    """Test that Platform can be initialized from configuration files."""

    from downsat import EumdacKey, Platform

    platform = Platform.from_env()

    # platform can load EumdacKey
    assert platform["EumdacKey.key"]
    assert platform["EumdacKey.secret"]

    platform.get("EumdacKey")
    platform.get(EumdacKey)


def test_class_with_subclasses() -> None:
    """That that Platform can create classes whose attributes are classes themselves."""
    from downsat import EumdacKey, EumdacUser, Platform

    platform = Platform.from_env()

    # platform can load EumdacUser
    platform.get("EumdacUser")
    user = platform.get(EumdacUser)

    assert isinstance(user.key, EumdacKey)


@pytest.mark.parametrize("satellite", ["MSG", "RSS", "Metop"], ids=["MSG", "RSS", "Metop"])
def test_eumetsat_archives(tmp_path: Path, satellite: str) -> None:
    """Test that Platfrom can instantiate MSG, RSS and Metop classes."""
    import importlib
    import os

    from downsat import Platform

    # dynamically import the archive class/function
    module = importlib.import_module("downsat")
    satellite_object = getattr(module, satellite)

    platform = Platform.from_env()

    # platform can provide satellite object when data_path is provided as parameter
    platform.get(satellite_object, {"data_path": tmp_path})  # type: ignore  # TODO: fix

    # platform can provide satellite object when data_path provided in environment variable
    os.environ[f"DOWNSAT_{satellite.upper()}_PATH"] = str(tmp_path)
    platform.get(satellite.upper())  # TODO: move upper to platform so that it is not needed here
