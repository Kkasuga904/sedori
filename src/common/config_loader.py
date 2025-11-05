from __future__ import annotations

import functools
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


logger = logging.getLogger(__name__)


class ConfigError(RuntimeError):
    """Raised when configuration files cannot be loaded or parsed."""


@functools.lru_cache(maxsize=1)
def load_settings(env: Optional[str] = None) -> Dict[str, Any]:
    """
    Load project configuration with optional environment-specific overrides.

    Args:
        env: Optional environment name (e.g. ``dev`` or ``staging``) used to
            locate a file in ``config/env/<env>.yml`` that extends the defaults.

    Returns:
        Nested dictionary containing configuration values.

    Raises:
        ConfigError: If the configuration files cannot be read or are invalid.
    """

    base_path = Path(__file__).resolve().parents[2]
    defaults_path = base_path / "config" / "settings.yml"
    if not defaults_path.exists():
        raise ConfigError(f"Missing configuration file: {defaults_path}")

    try:
        with defaults_path.open("r", encoding="utf-8") as handle:
            settings: Dict[str, Any] = yaml.safe_load(handle) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {defaults_path}: {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"Could not read {defaults_path}: {exc}") from exc

    if env:
        env_path = base_path / "config" / "env" / f"{env}.yml"
        if env_path.exists():
            try:
                with env_path.open("r", encoding="utf-8") as handle:
                    overrides: Dict[str, Any] = yaml.safe_load(handle) or {}
            except yaml.YAMLError as exc:
                raise ConfigError(f"Invalid YAML in {env_path}: {exc}") from exc
            except OSError as exc:
                raise ConfigError(f"Could not read {env_path}: {exc}") from exc
            _deep_update(settings, overrides)
        else:
            logger.warning("Environment override %s not found at %s", env, env_path)

    return settings


def _deep_update(destination: Dict[str, Any], source: Dict[str, Any]) -> None:
    """Recursively merge ``source`` into ``destination``."""

    for key, value in source.items():
        if (
            isinstance(value, dict)
            and key in destination
            and isinstance(destination[key], dict)
        ):
            _deep_update(destination[key], value)
        else:
            destination[key] = value

