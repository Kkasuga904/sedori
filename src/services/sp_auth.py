from __future__ import annotations

import logging
import time
from typing import Optional

import requests
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from src.common.config import RetrySettings, SPAPISettings

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = (2.0, 5.0)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class TokenAcquisitionError(RuntimeError):
    """Raised when the LWA token cannot be obtained."""


class RetryableTokenError(RuntimeError):
    """Internal marker for retry-eligible token failures."""


class SellingPartnerAuthenticator:
    TOKEN_ENDPOINT = "https://api.amazon.com/auth/o2/token"

    def __init__(
        self,
        config: SPAPISettings,
        retry: RetrySettings,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._config = config
        self._retry = retry
        self._session = session or requests.Session()
        self._cached_token: Optional[str] = None
        self._expires_at: float = 0.0

    def get_access_token(self) -> str:
        now = time.time()
        if self._cached_token and now < self._expires_at - 60:
            return self._cached_token

        retryer = Retrying(
            reraise=True,
            stop=stop_after_attempt(self._retry.max_attempts),
            wait=wait_exponential_jitter(initial=self._retry.base, max=self._retry.max_sleep),
            retry=retry_if_exception_type(RetryableTokenError),
        )

        try:
            token_data = retryer.call(self._refresh_token)
        except RetryableTokenError as exc:
            logger.error("Failed to refresh LWA token after retries: %s", exc)
            raise TokenAcquisitionError("Unable to refresh Amazon LWA token") from exc
        except TokenAcquisitionError:
            raise
        except Exception as exc:  # pragma: no cover - defensive safeguard
            raise TokenAcquisitionError(f"Unexpected error obtaining token: {exc}") from exc

        self._cached_token = token_data["access_token"]
        self._expires_at = now + float(token_data.get("expires_in", 3600))
        return self._cached_token

    def _refresh_token(self) -> dict:
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self._config.refresh_token.get_secret_value(),
            "client_id": self._config.lwa_client_id.get_secret_value(),
            "client_secret": self._config.lwa_client_secret.get_secret_value(),
        }
        try:
            response = self._session.post(self.TOKEN_ENDPOINT, data=payload, timeout=REQUEST_TIMEOUT)
        except requests.Timeout as exc:
            raise RetryableTokenError(f"timeout: {exc}") from exc
        except requests.ConnectionError as exc:
            raise RetryableTokenError(f"connection error: {exc}") from exc
        except requests.RequestException as exc:
            raise TokenAcquisitionError(f"Failed to obtain Amazon access token: {exc}") from exc

        if response.status_code in RETRYABLE_STATUS_CODES:
            raise RetryableTokenError(f"retryable status {response.status_code}")
        if response.status_code >= 400:
            raise TokenAcquisitionError(
                f"Amazon token endpoint error (status={response.status_code}, detail={response.text})"
            )

        try:
            return response.json()
        except ValueError as exc:
            raise TokenAcquisitionError(f"Invalid token JSON payload: {exc}") from exc
