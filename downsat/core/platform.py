from __future__ import annotations

from typing import Any, FrozenSet, TypeVar
import os
from pathlib import Path
from types import FunctionType

from attrs import field, frozen
from attrs.validators import deep_iterable, deep_mapping, instance_of
import importlib_resources as resources
import yaml


T = TypeVar("T")


class FromEnvMixin:
    """Mixin class that add `from_env` class method that builds the class using current platform."""

    @classmethod
    def from_env(cls: type[T], **kwargs: Any) -> T:
        """Build class using current platform."""
        from downsat.core.config import get_platform  # import here to prevent circular import

        return get_platform().get(cls, kwargs)


@frozen(slots=False)
class RegisteredComponent:  # TODO: rename, since now it can be also function
    """Class or function registered in the platform.

    :param class_type: Class type.
    :param default_parameters: Default parameters.
    """

    class_type: type | FunctionType = field()
    required_parameters: tuple[str, ...] = field(
        converter=tuple, factory=tuple, validator=deep_iterable(member_validator=instance_of(str))
    )
    default_parameters: dict[str, Any] = field(
        factory=dict,
        validator=deep_mapping(
            key_validator=instance_of(str),
            value_validator=lambda _a, _b, _c: None,  # type: ignore  # do not validate values
            mapping_validator=instance_of(dict),
        ),
    )
    types: dict[str, type] = field(init=False)

    @types.default
    def _infere_types_of_required_parameters(self) -> dict[str, type]:
        """Infere types of required parameters from class signature."""
        from inspect import signature

        return {
            name: parameter.annotation for name, parameter in signature(self.class_type).parameters.items()
        }

    @property
    def name(self) -> str:
        """Class name."""
        try:
            return self.class_type.__name__
        except AttributeError:
            return self.class_type.__class__.__name__  # type: ignore  # TODO: fix


@frozen(slots=False)
class Platform:
    """Platform automatically instantiates registered classes or functions with platform-specific parameters.

    Platform provides dict-like access to configuration parameters for all registered components
    and a method `get` that instantiates given registered component and allows passing
    additional parameters or overriding the ones set in the environment.

    Configuration parameter `a` belonging to class `A` can be accessed with key `"A.a"`.

    Priority of parameter value lookup:
    - parameter used during instantiation
    - default parameter registered for the component
    - parameter from environment variable

    Platform guarantees that registered component with given parameters is instantiated only once.

    Example:
        >>> from downsat.core.platform import Platform
        >>> platform = Platform()  # loads default configuration from `downsat.config.platform.yaml`
        >>> platform["EumdacKey.key"]  # access configuration parameter
        'my-secret-key'
        >>> platform.get_object("EumdacKey")  # instantiate registered component
        <downsat.clients.eumdac.EumdacKey at 0x7f1f2c1d6a90>

    :param env_parameter_names: Mapping from class parameter names to environment variable names.
        If not specified, parameter name like `A.a` is mapped to environment variable `A_A`.
    :param registered_components: Mapping from class types to default parameters.
    """

    # public attrs
    registered_components: dict[str, RegisteredComponent] = field(
        factory=dict
    )  # TODO: rename, since now it can be also function
    env_parameter_names: dict[str, str] = field(factory=dict)

    # attrs for internal use
    _instantiated_classes: dict[tuple[str, FrozenSet[tuple[str, Any]]], Any] = field(factory=dict, init=False)
    _registered_functions_by_id: dict[int, tuple[str, RegisteredComponent]] = field(init=False)

    @_registered_functions_by_id.default
    def _infere_registered_functions_by_id(self) -> dict[int, tuple[str, RegisteredComponent]]:
        """Registered components that represent functions indexed by id."""
        return {
            id(registered_component.class_type): (name, registered_component)
            for name, registered_component in self.registered_components.items()
            if isinstance(registered_component.class_type, FunctionType)
        }

    # methods
    @classmethod
    def from_env(cls, config_file: str | Path | None = None) -> Platform:
        """Build platform from yaml configuration file.

        :param config_file: Path to yaml configuration file. If not specified, default configuration from `downsat.config.platform.yaml` is used.
        :return: Platform object.
        """
        from downsat.core.io import yaml_converter  # import here to prevent circular import

        if config_file is None:
            with resources.as_file(
                resources.files("downsat.config") / "platform.yaml"
            ) as default_config_file:
                return cls.from_env(default_config_file)
        else:
            with open(config_file, "rt", encoding="utf-8") as f:
                config = yaml.load(f, Loader=yaml.Loader)

        return yaml_converter.structure(config, Platform)

    def register(
        self,
        class_type: type,
        required_parameters: tuple[str, ...],
        default_parameters: dict[str, Any] | None = None,
        overwrite: bool = False,
    ) -> None:
        """Register a class and its default parameters.

        Not all class parameters need to be specified. Parameters not specified in the
        default parameters will be required during instantiation. Parameters specified
        can be overridden during instantiation.

        :param class_type: Class to be registered.
        :param required_parameters: Required parameters for the class.
        :param default_parameters: Default parameters for the class.

        # TODO: infer required parameters from class signature
        """
        class_name = self._get_component_name(class_type)
        default_parameters = default_parameters or {}

        # check input values
        if class_name in self.registered_components and not overwrite:
            raise ValueError(f"Class {class_type} is already registered. Use `overwrite=True` to overwrite.")

        # register
        registered_component = RegisteredComponent(
            class_type=class_type,
            required_parameters=required_parameters,  # type: ignore  # TODO: fix
            default_parameters=default_parameters,
        )
        self.registered_components[class_name] = registered_component

    def is_registered(self, cls: type | str) -> bool:
        """Check if class is registered.

        :param cls: Class type or class name.
        :return: True if class is registered, False otherwise.
        """
        class_name = self._get_component_name(cls)
        return class_name in self.registered_components

    def _split_key(self, key: str) -> tuple[str, str]:
        """Split key into class name and parameter name.

        :param key: Key in format `class_name.parameter_name`.
        :return: Tuple of class name and parameter name.
        :raises KeyError: If key is not in format `class_name.parameter_name`.
        """
        try:
            class_name, parameter_name = key.split(".", 1)
        except ValueError as e:
            raise KeyError(f"Invalid key {key}. Key must be in format `class_name.parameter_name`.") from e

        return class_name, parameter_name

    def _get_env_parameter_name(self, key: str) -> str:
        """Get environment variable name for given key.

        :param key: Key in format `class_name.parameter_name`.
        """
        default_key = key.replace(".", "_").upper()
        return self.env_parameter_names.get(key, default_key)

    def _get_component_name(self, component: Any) -> str:
        """Get component name.

        :param component: Component type or component name.
        :return: Component name.
        """
        if isinstance(component, str):
            return component

        try:
            return component.__name__
        except AttributeError:
            return component.__class__.__name__

    def _get_parameter_from_env(self, key: str) -> Any:
        """Get parameter value from environment variable.

        Name of the environment variable is either specified in `env_parameter_names` or
        is in format `class_name.parameter_name` converted to uppercase and with dots
        replaced by underscores, i.e. `A.a` -> `A_A`.

        :param key: Key in format `class_name.parameter_name`.
        :return: Parameter value stored in environment variable.
        :raises KeyError: If environment variable is not set.
        """
        env_name = self._get_env_parameter_name(key)

        return os.environ[env_name]

    def __getitem__(self, key: str) -> Any:
        """Get configuration parameter.

        Key must be in format `class_name.parameter_name`.

        :param key: Parameter name.
        :return: Parameter value.
        :raises KeyError: If parameter is not found or key is not in format `class_name.parameter_name`.
        """
        class_name, parameter_name = self._split_key(key)
        try:
            # existing default parameter of registered component
            registered_component = self.registered_components[class_name]
            return registered_component.default_parameters[
                parameter_name
            ]  # TODO: implement as parameter_accessor
        except KeyError:
            # registered component does not have this default parameter or component not registered
            return self._get_parameter_from_env(key)

    def _get_parameters_for_class(
        self, name: str, registered_component: RegisteredComponent, parameters: dict[str, Any]
    ) -> dict[str, Any]:
        """Get parameters for class instantiation.

        If parameter of a registered component is a registered component itself, it is instantiated
        with the same parameters.

        :param name: Name of the class (its key in the platform).
        :param registered_component: Registered component.
        :param parameters: Additional parameters that take priority over default or environment parameters.
        :return: Parameters to instantiate the class.
        :raises KeyError: If required parameter is not specified.

        # TODO: provide way how to differentiate between parameters for the main class and parameters for the nested classes (its parameters)
        """
        final_parameters = {}
        for (
            parameter
        ) in (
            registered_component.required_parameters
        ):  # TODO: go also through optional parameters of the class-> allows to override their default values
            if parameter in parameters:
                # parameter was passed to `get` => use it
                final_parameters[parameter] = parameters[parameter]
                continue

            parameter_type = registered_component.types[parameter]
            if self.is_registered(parameter_type):
                # parameter is a registered component => instantiate it
                final_parameters[parameter] = self.get(
                    parameter_type, parameters
                )  # TODO: this may lead to an infinite loop => detect and raise error
                # TODO: test also if the component has `from_env` class method and call that one if self.get fails
            else:
                # try to get parameters from default parameters or environment variables
                full_parameter_name = f"{name}.{parameter}"
                try:
                    final_parameters[parameter] = self[full_parameter_name]
                except KeyError as e:
                    env_parameter_name = self._get_env_parameter_name(full_parameter_name)
                    raise KeyError(
                        f"Required parameter `{parameter}` for class {full_parameter_name} was not specified."
                        " Specify it in `get` or as default parameter or environment variable "
                        f"{env_parameter_name}."
                    ) from e

        return final_parameters

    def _instantiate_component(
        self, name: str, registered_component: RegisteredComponent, parameters: dict[str, Any]
    ) -> Any:
        """Instantiate registered component with given parameters.

        This method is cached -> registered component with given parameters is instantiated only once.

        :param name: Name of the class (its key in the platform).
        :param registered_component: Registered component.
        :param parameters: Parameters for the class that take priority over default or environment parameters.
        :return: Instantiated class.
        :raises TypeError: If parameters are invalid.
        :raises KeyError: If required parameter is not specified.
        """
        # get parameters
        parameters = self._get_parameters_for_class(name, registered_component, parameters)

        key = (registered_component.name, frozenset(parameters.items()))

        # instantiate
        if key not in self._instantiated_classes:
            self._instantiated_classes[key] = registered_component.class_type(**parameters)

        return self._instantiated_classes[key]

    def get(self, component: str | type | FunctionType, parameters: dict[str, Any] | None = None) -> Any:
        """Instantiate registered component with given parameters.

        :param component: Class to be instantiated. Either class type or class name.
        :param parameters: Parameters for the class.
        :return: Instantiated class.
        :raises TypeError: If parameters are invalid.
        :raises KeyError: If required parameter is not specified.
        """
        parameters = parameters or {}
        if isinstance(component, FunctionType):
            # get registered function by id
            function_id = id(component)

            try:
                class_name, registered_component = self._registered_functions_by_id[function_id]
            except KeyError:
                raise KeyError(
                    f"Function `{self._get_component_name(component)}` with id {function_id} is not registered."
                ) from None
        else:
            # get registered class by name
            class_name = self._get_component_name(component)

            try:
                registered_component = self.registered_components[class_name]
            except KeyError:
                raise KeyError(f"Class `{class_name}` is not registered.")

        return self._instantiate_component(class_name, registered_component, parameters)
