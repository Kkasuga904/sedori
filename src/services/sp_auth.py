from __future__ import annotations

import logging
import time
from typing import Dict, Optional

import requests


logger = logging.getLogger(__name__)


class SellingPartnerAuthenticator:
    TOKEN_ENDPOINT = "https://api.amazon.com/auth/o2/token"

    def __init__(self, config: Dict[str, str], session: Optional[requests.Session] = None) -> None:
        self._config = config
        self._session = session or requests.Session()
        self._cached_token: Optional[str] = None
        self._expires_at: float = 0.0

    def get_access_token(self) -> str:
        now = time.time()
        if self._cached_token and now < self._expires_at - 60:
            return self._cached_token

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self._config["refresh_token"],
            "client_id": self._config["lwa_client_id"],
            "client_secret": self._config["lwa_client_secret"],
        }
        try:
            response = self._session.post(self.TOKEN_ENDPOINT, data=payload, timeout=10)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(f"Failed to obtain Amazon access token: {exc}") from exc

        data = response.json()
        self._cached_token = data["access_token"]
        self._expires_at = now + data.get("expires_in", 3600)
        return self._cached_token

