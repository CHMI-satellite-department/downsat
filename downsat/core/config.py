from __future__ import annotations

from downsat.core.platform import Platform


_default_platform = Platform.from_env()


def get_platform() -> Platform:
    return _default_platform
