from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Iterable, Mapping, MutableMapping, Optional

_MASK = "***REDACTED***"


class SecretRedactor(logging.Filter):
    """Logging filter that redacts configured secrets from log records."""

    def __init__(self, secrets: Mapping[str, str], mask: str = _MASK) -> None:
        super().__init__()
        self._mask = mask
        self._secrets = {key: value for key, value in secrets.items() if value}

    def update(self, secrets: Mapping[str, str]) -> None:
        self._secrets.update({key: value for key, value in secrets.items() if value})

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401
        """Redact known secret values from the record message and args."""

        if not self._secrets:
            return True

        if isinstance(record.msg, str):
            record.msg = self._redact(record.msg)

        if record.args:
            sanitized = []
            for arg in record.args:
                if isinstance(arg, str):
                    sanitized.append(self._redact(arg))
                else:
                    sanitized.append(arg)
            record.args = tuple(sanitized)

        extra = getattr(record, "__dict__", {})
        for key, value in list(extra.items()):
            if isinstance(value, str):
                extra[key] = self._redact(value)
        return True

    def _redact(self, value: str) -> str:
        masked = value
        for secret in self._secrets.values():
            if secret:
                masked = masked.replace(secret, self._mask)
        return masked


class JsonFormatter(logging.Formatter):
    """Structured JSON formatter adding standard metadata."""

    def format(self, record: logging.LogRecord) -> str:
        base: MutableMapping[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in ("request_id", "asin", "task", "decision_flag"):
            value = getattr(record, field, None)
            if value is not None:
                base[field] = value

        if record.exc_info:
            base["exception"] = self.formatException(record.exc_info)

        return json.dumps(base, ensure_ascii=False)


def configure_logging(
    level: str = "INFO",
    json_logs: bool = True,
    secrets: Optional[Mapping[str, str]] = None,
    stream: Optional[object] = None,
) -> SecretRedactor:
    """Configure root logging with optional JSON formatter and redaction."""

    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter() if json_logs else logging.Formatter("%(asctime)s %(name)s [%(levelname)s] %(message)s"))

    redactor = SecretRedactor(secrets or {})
    handler.addFilter(redactor)

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level.upper())
    root.addHandler(handler)

    logging.captureWarnings(True)
    logging.getLogger("urllib3").setLevel(max(logging.WARNING, root.level))

    return redactor


def extra_logger(name: str, **extra: object) -> logging.LoggerAdapter:
    """Return a logger adapter that injects contextual fields (e.g. request_id)."""

    return logging.LoggerAdapter(logging.getLogger(name), extra)
