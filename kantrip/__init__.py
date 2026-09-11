"""Kantrip package metadata.

Importing this package is intentionally side-effect free. Runtime directories,
logging and consoles are initialized only by command handlers.
"""

from importlib.metadata import PackageNotFoundError, version

APP_NAME = "kantrip"
APP_BANNER = r"""     _               _        _
    | | ____ _ _ __ | |_ _ __(_)_ __
    | |/ / _` | '_ \| __| '__| | '_ \
    |   < (_| | | | | |_| |  | | |_) |
    |_|\_\__,_|_| |_|\__|_|  |_| .__/
                               |_|"""

try:
    APP_VERSION = version(APP_NAME)
except PackageNotFoundError:
    APP_VERSION = "0.0.0"

__version__ = APP_VERSION

__all__ = ["APP_BANNER", "APP_NAME", "APP_VERSION", "__version__"]
