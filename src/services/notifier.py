from __future__ import annotations

import logging
import os
from typing import Optional

import httpx
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError, SlackClientError
from tenacity import RetryError, Retrying, retry_if_exception_type, stop_after_attempt, wait_random_exponential

from src.common.config import LineSettings, RetrySettings, SlackSettings


logger = logging.getLogger(__name__)

LINE_NOTIFY_URL = "https://notify-api.line.me/api/notify"


class NotificationError(RuntimeError):
    """Raised when a notification channel fails."""


class _RetryableNotificationError(NotificationError):
    """Internal error used to trigger retry logic."""


class Notifier:
    """Dispatch notifications to Slack and LINE with retry/backoff."""

    def __init__(
        self,
        slack: SlackSettings,
        line: LineSettings,
        retry: RetrySettings,
        timeout_seconds: float = 5.0,
    ) -> None:
        self._slack_settings = slack
        self._line_settings = line
        self._retry_settings = retry
        self._timeout = timeout_seconds

        self._slack_token: Optional[str] = (
            slack.token.get_secret_value() if slack.token and slack.token.get_secret_value() else None
        )
        self._slack_channel = slack.channel
        self._slack_webhook = slack.webhook
        self._slack_client: Optional[WebClient] = WebClient(token=self._slack_token) if self._slack_token else None

    def post_slack(self, summary_text: str) -> None:
        """Send a plain text summary to Slack."""

        if not self._slack_settings.enabled:
            logger.debug("Slack notifications disabled by configuration.")
            return

        if self._slack_client and self._slack_channel:
            self._send_slack_message(summary_text)
            return

        if self._slack_webhook:
            self._send_slack_webhook(summary_text)
            return

        logger.debug("Slack notification skipped due to missing token/webhook or channel configuration.")

    def post_line(self, summary_text: str) -> None:
        """Send a message via LINE Notify."""

        if not self._line_settings.enabled:
            logger.debug("LINE notifications disabled by configuration.")
            return

        token = self._resolve_line_token()
        if not token:
            logger.debug("LINE notification skipped due to missing access token.")
            return

        headers = {"Authorization": f"Bearer {token}"}
        data = {"message": summary_text}

        retryer = self._build_retryer()
        try:
            for attempt in retryer:
                with attempt:
                    try:
                        response = httpx.post(
                            LINE_NOTIFY_URL,
                            headers=headers,
                            data=data,
                            timeout=self._timeout,
                        )
                    except httpx.RequestError as exc:
                        logger.warning("LINE Notify transport error: %s", exc)
                        raise _RetryableNotificationError("LINE Notify transport error.") from exc

                    if response.status_code in (429,) or response.status_code >= 500:
                        logger.warning(
                            "LINE Notify transient failure status=%s; retrying.",
                            response.status_code,
                        )
                        response.close()
                        raise _RetryableNotificationError(f"LINE Notify transient failure: {response.status_code}")
                    if response.status_code >= 400:
                        logger.error(
                            "LINE Notify request failed permanently status=%s.",
                            response.status_code,
                        )
                        response.close()
                        raise NotificationError(f"LINE Notify request failed: HTTP {response.status_code}")
                    response.close()
        except RetryError as exc:
            raise NotificationError("LINE Notify exhausted retry attempts.") from exc
        else:
            logger.info("LINE notification delivered.")

    def _send_slack_message(self, summary_text: str) -> None:
        assert self._slack_client is not None  # for mypy
        assert self._slack_channel is not None

        retryer = self._build_retryer()
        try:
            for attempt in retryer:
                with attempt:
                    self._slack_client.chat_postMessage(channel=self._slack_channel, text=summary_text)
        except SlackApiError as exc:
            status = getattr(exc.response, "status_code", None)
            if status in (429,) or (isinstance(status, int) and status >= 500):
                logger.warning("Slack API transient failure status=%s; retrying.", status)
                raise _RetryableNotificationError(f"Slack API transient failure: {status}") from exc
            logger.error("Slack API error: %s", exc)
            raise NotificationError("Slack API error.") from exc
        except SlackClientError as exc:
            logger.warning("Slack client transport error: %s", exc)
            raise _RetryableNotificationError("Slack client transport error.") from exc
        except RetryError as exc:
            raise NotificationError("Slack notification exhausted retry attempts.") from exc
        else:
            logger.info("Slack notification delivered to channel %s.", self._slack_channel)

    def _send_slack_webhook(self, summary_text: str) -> None:
        assert self._slack_webhook is not None

        payload = {"text": summary_text}
        retryer = self._build_retryer()
        try:
            for attempt in retryer:
                with attempt:
                    response = httpx.post(self._slack_webhook, json=payload, timeout=self._timeout)
                    if response.status_code in (429,) or response.status_code >= 500:
                        logger.warning(
                            "Slack webhook transient failure status=%s; retrying.",
                            response.status_code,
                        )
                        response.close()
                        raise _RetryableNotificationError(f"Slack webhook transient failure: {response.status_code}")
                    if response.status_code >= 400:
                        logger.error("Slack webhook request failed status=%s.", response.status_code)
                        response.close()
                        raise NotificationError(f"Slack webhook request failed: HTTP {response.status_code}")
                    response.close()
        except httpx.RequestError as exc:
            logger.warning("Slack webhook transport error: %s", exc)
            raise _RetryableNotificationError("Slack webhook transport error.") from exc
        except RetryError as exc:
            raise NotificationError("Slack webhook exhausted retry attempts.") from exc
        else:
            logger.info("Slack webhook notification delivered.")

    def _resolve_line_token(self) -> Optional[str]:
        if self._line_settings.token and self._line_settings.token.get_secret_value():
            return self._line_settings.token.get_secret_value()
        env_token = os.getenv("LINE_NOTIFY_TOKEN", "").strip()
        return env_token or None

    def _build_retryer(self) -> Retrying:
        wait_multiplier = max(self._retry_settings.base, 0.1)
        wait_max = max(self._retry_settings.max_sleep, wait_multiplier)
        return Retrying(
            stop=stop_after_attempt(max(self._retry_settings.max_attempts, 1)),
            wait=wait_random_exponential(multiplier=wait_multiplier, max=wait_max),
            retry=retry_if_exception_type(_RetryableNotificationError),
            reraise=True,
        )
