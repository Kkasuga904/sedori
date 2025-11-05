from __future__ import annotations

import json
import logging
from typing import Dict, Iterable, Optional

import requests


logger = logging.getLogger(__name__)


class NotificationError(RuntimeError):
    """Raised when a notification channel fails."""


class NotificationService:
    def __init__(self, config: Dict[str, Dict[str, object]]) -> None:
        self._slack_webhooks = tuple(config.get("slack", {}).get("webhook_urls", []))
        line_config = config.get("line", {})
        self._line_access_token: Optional[str] = line_config.get("channel_access_token")
        self._line_user_ids: Iterable[str] = line_config.get("user_ids", [])

    def notify(self, title: str, message: str, payload: Optional[dict] = None) -> None:
        errors = []
        if self._slack_webhooks:
            try:
                self._notify_slack(title, message, payload)
            except NotificationError as exc:
                errors.append(str(exc))
        if self._line_access_token and self._line_user_ids:
            try:
                self._notify_line(message)
            except NotificationError as exc:
                errors.append(str(exc))
        if errors:
            raise NotificationError("; ".join(errors))

    def _notify_slack(self, title: str, message: str, payload: Optional[dict]) -> None:
        body = {
            "text": title,
            "attachments": [
                {
                    "color": "#36a64f",
                    "title": title,
                    "text": message,
                    "fields": [{"title": "Payload", "value": json.dumps(payload, ensure_ascii=False, indent=2)}]
                    if payload
                    else [],
                }
            ],
        }
        for webhook in self._slack_webhooks:
            try:
                response = requests.post(webhook, json=body, timeout=10)
                response.raise_for_status()
            except requests.RequestException as exc:
                logger.error("Slack notification failed for %s: %s", webhook, exc)
                raise NotificationError(f"Slack notification failed: {exc}") from exc

    def _notify_line(self, message: str) -> None:
        headers = {"Authorization": f"Bearer {self._line_access_token}"}
        body = {"messages": [{"type": "text", "text": message}]}
        for user_id in self._line_user_ids:
            payload = {**body, "to": user_id}
            try:
                response = requests.post(
                    "https://api.line.me/v2/bot/message/push",
                    headers=headers,
                    json=payload,
                    timeout=10,
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                logger.error("LINE notification failed for %s: %s", user_id, exc)
                raise NotificationError(f"LINE notification failed: {exc}") from exc

