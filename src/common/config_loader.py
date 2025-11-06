from __future__ import annotations

import functools
import logging
import os
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import yaml
from pydantic import ValidationError

from src.common.config import Settings


logger = logging.getLogger(__name__)


class ConfigError(RuntimeError):
    """Raised when configuration files cannot be loaded or parsed."""


@functools.lru_cache(maxsize=1)
def load_settings(env: Optional[str] = None) -> Settings:
    """Load configuration from disk, environment overrides, and validate."""

    base_path = Path(__file__).resolve().parents[2]
    defaults_path = base_path / "config" / "settings.yml"
    if not defaults_path.exists():
        raise ConfigError(f"Missing configuration file: {defaults_path}")

    try:
        with defaults_path.open("r", encoding="utf-8") as handle:
            settings: Dict[str, Any] = yaml.safe_load(handle) or {}
    except yaml.YAMLError as exc:  # pragma: no cover - YAML parsing errors are rare
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

    _apply_env_overrides(settings, os.environ)

    try:
        return Settings.model_validate(settings)
    except ValidationError as exc:
        raise ConfigError(f"Configuration validation error: {exc}") from exc


def _apply_env_overrides(settings: Dict[str, Any], environ: Mapping[str, str]) -> None:
    """Apply environment variables prefixed with ``SEDORI__`` as overrides."""

    prefix = "SEDORI__"
    for key, value in environ.items():
        if not key.startswith(prefix):
            continue
        path = key[len(prefix) :].lower().split("__")
        _assign_nested(settings, path, value)


def _assign_nested(target: Dict[str, Any], path: list[str], value: Any) -> None:
    cursor = target
    for segment in path[:-1]:
        if segment not in cursor or not isinstance(cursor[segment], dict):
            cursor[segment] = {}
        cursor = cursor[segment]
    cursor[path[-1]] = value


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
