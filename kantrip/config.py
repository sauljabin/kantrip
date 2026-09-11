"""Load and validate Kantrip profile configuration."""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import yaml
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

CONFIG_FILENAME = "config.yaml"
SCHEMA_FILENAME = "profile.schema.json"


class ConfigurationError(ValueError):
    """Raised when Kantrip configuration cannot be loaded safely."""


DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"


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
    missing_ok: bool = False,
) -> Configuration:
    """Load a YAML document and validate it against the bundled schema."""
    config_path = path if path is not None else resolve_config_path(environment)
    try:
        contents = config_path.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        if missing_ok:
            return Configuration(path=config_path, values={"profiles": {}})
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

    _validate(values)

    return Configuration(path=config_path, values=values)


def add_profile(
    profile_name: str,
    path: Path | None = None,
    *,
    bootstrap_servers: tuple[str, ...] = (DEFAULT_BOOTSTRAP_SERVER,),
    description: str | None = None,
    environment: Mapping[str, str] | None = None,
) -> Configuration:
    """Add a plaintext profile, creating configuration when necessary."""
    config_path = path if path is not None else resolve_config_path(environment)
    configuration = load_configuration(config_path, missing_ok=True)
    if profile_name in configuration.profiles:
        raise ConfigurationError(f"profile '{profile_name}' already exists")

    values = deepcopy(configuration.values)
    profile: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "kafka": {
            "bootstrapServers": list(bootstrap_servers),
            "transport": "plaintext",
            "auth": {"type": "none"},
        },
    }
    if description is not None:
        profile["description"] = description
    values["profiles"][profile_name] = profile
    _validate(values)
    _write_configuration(config_path, values)
    return Configuration(path=config_path, values=values)


def remove_profile(
    profile_name: str,
    path: Path | None = None,
    *,
    environment: Mapping[str, str] | None = None,
) -> Configuration:
    """Remove a profile and atomically persist the remaining configuration."""
    config_path = path if path is not None else resolve_config_path(environment)
    configuration = load_configuration(config_path, missing_ok=True)
    if profile_name not in configuration.profiles:
        raise ConfigurationError(f"profile '{profile_name}' was not found")

    values = deepcopy(configuration.values)
    del values["profiles"][profile_name]
    _validate(values)
    _write_configuration(config_path, values)
    return Configuration(path=config_path, values=values)


def _validate(values: dict[str, Any]) -> None:
    validator = Draft202012Validator(_load_schema(), format_checker=FormatChecker())
    errors = sorted(
        validator.iter_errors(values),
        key=lambda item: tuple(str(part) for part in item.absolute_path),
    )
    if not errors:
        return
    validation_error = errors[0]
    location = ".".join(str(part) for part in validation_error.absolute_path) or "document root"
    detail = _validation_detail(validation_error)
    suffix = f": {detail}" if detail else ""
    raise ConfigurationError(f"configuration does not match schema at {location}{suffix}")


def _validation_detail(error: ValidationError) -> str | None:
    """Describe structural errors without including configuration values."""
    if error.validator == "additionalProperties" and isinstance(error.instance, dict):
        properties = error.schema.get("properties", {})
        unknown = sorted(str(key) for key in error.instance if key not in properties)
        if unknown:
            label = "field" if len(unknown) == 1 else "fields"
            return f"unknown {label}: {', '.join(unknown)}"
    if error.validator == "required" and isinstance(error.instance, dict):
        missing = sorted(str(key) for key in error.validator_value if key not in error.instance)
        if missing:
            label = "field" if len(missing) == 1 else "fields"
            return f"missing required {label}: {', '.join(missing)}"
    return None


def _write_configuration(path: Path, values: dict[str, Any]) -> None:
    """Atomically replace configuration with a private validated YAML document."""
    temporary_path: Path | None = None
    descriptor: int | None = None

    try:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".config-", suffix=".tmp", dir=path.parent
        )
        temporary_path = Path(temporary_name)
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            descriptor = None
            yaml.safe_dump(values, stream, sort_keys=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    except OSError as error:
        raise ConfigurationError(f"configuration could not be updated: {path}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def _load_schema() -> dict[str, Any]:
    packaged_path = Path(__file__).parent / "schemas" / SCHEMA_FILENAME
    source_path = Path(__file__).parents[1] / "schemas" / SCHEMA_FILENAME
    schema_path = packaged_path if packaged_path.is_file() else source_path
    return cast(dict[str, Any], json.loads(schema_path.read_text(encoding="utf-8")))


__all__ = [
    "CONFIG_FILENAME",
    "Configuration",
    "ConfigurationError",
    "add_profile",
    "load_configuration",
    "remove_profile",
    "resolve_config_path",
]
