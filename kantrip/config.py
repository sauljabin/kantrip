"""Load and validate Kantrip profile configuration."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import yaml
from jsonschema import Draft202012Validator, FormatChecker

CONFIG_FILENAME = "config.yaml"
SCHEMA_FILENAME = "profile-v1.schema.json"


class ConfigurationError(ValueError):
    """Raised when Kantrip configuration cannot be loaded safely."""


@dataclass(frozen=True)
class Configuration:
    """A validated Kantrip configuration document."""

    path: Path
    values: dict[str, Any]

    @property
    def profiles(self) -> dict[str, dict[str, Any]]:
        """Return the validated profile mapping."""
        return cast(dict[str, dict[str, Any]], self.values["profiles"])

    def profile(self, name: str) -> dict[str, Any]:
        """Return one profile or raise an actionable configuration error."""
        try:
            return self.profiles[name]
        except KeyError as error:
            raise ConfigurationError(f"profile '{name}' was not found") from error


def resolve_config_path(environment: Mapping[str, str] | None = None) -> Path:
    """Resolve the configuration path using Kantrip's documented precedence."""
    env = os.environ if environment is None else environment
    configured = env.get("KANTRIP_CONFIG")
    if configured:
        return Path(configured).expanduser()

    xdg_config_home = env.get("XDG_CONFIG_HOME")
    if xdg_config_home:
        return Path(xdg_config_home) / "kantrip" / CONFIG_FILENAME

    home = env.get("HOME")
    if home:
        return Path(home) / ".config" / "kantrip" / CONFIG_FILENAME
    return Path.home() / ".config" / "kantrip" / CONFIG_FILENAME


def load_configuration(
    path: Path | None = None,
    *,
    environment: Mapping[str, str] | None = None,
) -> Configuration:
    """Load a YAML document and validate it against the bundled v1 schema."""
    config_path = path if path is not None else resolve_config_path(environment)
    try:
        contents = config_path.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise ConfigurationError(f"configuration file was not found: {config_path}") from error
    except OSError as error:
        raise ConfigurationError(f"configuration file could not be read: {config_path}") from error

    try:
        values = yaml.safe_load(contents)
    except yaml.MarkedYAMLError as error:
        location = ""
        if error.problem_mark is not None:
            location = (
                f" at line {error.problem_mark.line + 1}, column {error.problem_mark.column + 1}"
            )
        raise ConfigurationError(f"configuration contains invalid YAML{location}") from error

    if not isinstance(values, dict):
        raise ConfigurationError("configuration must be a YAML object")

    validator = Draft202012Validator(_load_schema(), format_checker=FormatChecker())
    errors = sorted(
        validator.iter_errors(values),
        key=lambda item: tuple(str(part) for part in item.absolute_path),
    )
    if errors:
        validation_error = errors[0]
        location = ".".join(str(part) for part in validation_error.absolute_path) or "document root"
        raise ConfigurationError(f"configuration does not match schema at {location}")

    return Configuration(path=config_path, values=values)


def _load_schema() -> dict[str, Any]:
    packaged_path = Path(__file__).parent / "schemas" / SCHEMA_FILENAME
    source_path = Path(__file__).parents[1] / "schemas" / SCHEMA_FILENAME
    schema_path = packaged_path if packaged_path.is_file() else source_path
    return cast(dict[str, Any], json.loads(schema_path.read_text(encoding="utf-8")))


__all__ = [
    "CONFIG_FILENAME",
    "Configuration",
    "ConfigurationError",
    "load_configuration",
    "resolve_config_path",
]
