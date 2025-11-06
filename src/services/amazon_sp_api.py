from __future__ import annotations

import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote, urlparse

import requests
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from src.common.config import RetrySettings, SPAPISettings
from src.common.models import CompetitivePrice, FeeBreakdown, ProductQuery, ServiceFlags, ServiceResult
from src.common.rate_limit import BudgetExceeded, CircuitBreaker, CircuitOpen, KeySemaphore, RequestBudget

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
REQUEST_TIMEOUT = (2.0, 5.0)
SERVICE_NAME = "execute-api"
DEFAULT_HOST_FE = "sellingpartnerapi-fe.amazon.com"


def _secret_value(value: object) -> str:
    if hasattr(value, "get_secret_value"):
        return value.get_secret_value()  # type: ignore[attr-defined]
    return str(value)


class AmazonSPAPIError(RuntimeError):
    """Raised when the Amazon SP-API returns an unrecoverable error."""


class RetryableRequestError(RuntimeError):
    """Internal marker for retry-eligible failures."""


class AmazonSPAPIClient:
    def __init__(
        self,
        settings: SPAPISettings,
        retry: RetrySettings,
        access_token_provider,
        budget: RequestBudget,
        budget_limit: int,
        semaphore: KeySemaphore,
        circuit_breaker: CircuitBreaker,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._settings = settings
        self._retry = retry
        self._session = session or requests.Session()
        self._access_token_provider = access_token_provider
        self._budget = budget
        self._budget_limit = budget_limit
        self._semaphore = semaphore
        self._circuit_breaker = circuit_breaker
        self._host = DEFAULT_HOST_FE
        self._endpoint = f"https://{self._host}"
        self._signer = SigV4Signer(
            access_key=_secret_value(self._settings.aws_access_key),
            secret_key=_secret_value(self._settings.aws_secret_key),
            region=self._settings.region,
            service=SERVICE_NAME,
        )

    def get_competitive_pricing(self, query: ProductQuery) -> ServiceResult[List[CompetitivePrice]]:
        params = self._build_pricing_params(query)
        outcome = self._request(
            "GET",
            "/products/pricing/v0/competitivePrice",
            request_key=self._budget_key(),
            params=params,
        )
        if outcome.data is None:
            return ServiceResult(data=[], flags=outcome.flags)

        payload = outcome.data.json()
        logger.debug("Competitive pricing payload: %s", payload)
        offers: List[CompetitivePrice] = []
        for product in payload.get("payload", []):
            for offer in product.get("competitivePricing", {}).get("competitivePrices", []):
                price = offer.get("price", {})
                amount = Decimal(str(price.get("LandedPrice", {}).get("Amount", "0")))
                shipping = Decimal(str(price.get("Shipping", {}).get("Amount", "0")))
                offers.append(
                    CompetitivePrice(
                        condition=offer.get("condition", "Unknown"),
                        seller_id=offer.get("sellerId", "Unknown"),
                        landed_price=amount,
                        shipping=shipping,
                        last_updated=datetime.now(timezone.utc),
                    )
                )
        return ServiceResult(data=offers, flags=outcome.flags)

    def get_fees_estimate(self, asin: str, price: Decimal) -> ServiceResult[FeeBreakdown]:
        currency = self._settings.default_currency
        body = {
            "FeesEstimateRequest": {
                "MarketplaceId": self._settings.marketplace_id,
                "Identifier": asin,
                "PriceToEstimateFees": {
                    "ListingPrice": {"CurrencyCode": currency, "Amount": str(price)}
                },
                "IdentifierValue": asin,
                "OptionalFulfillmentPrograms": ["FBA"],
            }
        }
        outcome = self._request(
            "POST",
            "/products/fees/v0/listings/fees",
            request_key=self._budget_key(),
            json=body,
        )
        if outcome.data is None:
            return ServiceResult(data=None, flags=outcome.flags)

        payload = outcome.data.json()
        logger.debug("Fees estimate payload: %s", payload)

        fees_section = (
            payload.get("payload", {})
            .get("FeesEstimatorResult", {})
            .get("FeesEstimate", {})
        )
        total_fees = fees_section.get("TotalFees", []) or []
        breakdown: Dict[str, Decimal] = {}
        for fee in total_fees:
            try:
                fee_type = fee["FeeType"]
                amount = Decimal(str(fee["FeeAmount"]["Amount"]))
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning("Skipping malformed fee entry %s (%s)", fee, exc)
                continue
            breakdown[fee_type] = amount

        return ServiceResult(
            data=FeeBreakdown(
                referral_fee=breakdown.get("ReferralFee", Decimal("0")),
                closing_fee=breakdown.get("VariableClosingFee", Decimal("0")),
                fba_fee=breakdown.get("FBAPerUnitFulfillmentFee", Decimal("0")),
                inbound_shipping=breakdown.get("FBAShipmentFee", Decimal("0")),
                packaging_materials=Decimal("0"),
                storage_fee=Decimal("0"),
                taxes=breakdown.get("Tax", Decimal("0")),
                fx_spread=Decimal("0"),
                returns_cost=Decimal("0"),
                other_costs=Decimal("0"),
            ),
            flags=outcome.flags,
        )

    def _build_pricing_params(self, query: ProductQuery) -> Dict[str, str]:
        params = {"MarketplaceId": self._settings.marketplace_id}
        if query.asin:
            params["Asins"] = query.asin
        if query.barcode:
            params["Skus"] = query.barcode
        return params

    def _budget_key(self) -> str:
        return f"spapi:{self._settings.marketplace_id}"

    def _request(self, method: str, path: str, request_key: str, **kwargs) -> ServiceResult[requests.Response]:
        try:
            self._circuit_breaker.allow()
        except CircuitOpen as exc:
            logger.warning("Circuit breaker open for %s: %s", request_key, exc)
            return ServiceResult(data=None, flags=ServiceFlags(degraded=True, circuit_open=True, reason="circuit_open"))

        headers = kwargs.pop("headers", {})
        headers["Accept"] = "application/json"
        headers["x-amz-access-token"] = self._access_token_provider()

        retryer = Retrying(
            reraise=True,
            stop=stop_after_attempt(self._retry.max_attempts),
            wait=wait_exponential_jitter(initial=self._retry.base, max=self._retry.max_sleep),
            retry=retry_if_exception_type(RetryableRequestError),
        )

        try:
            response: requests.Response = retryer(
                self._send_once,
                method,
                path,
                request_key,
                headers,
                dict(kwargs),
            )
        except BudgetExceeded:
            logger.warning("SP-API budget exhausted for %s", request_key)
            return ServiceResult(data=None, flags=ServiceFlags(degraded=True, reason="budget_exceeded"))
        except RetryableRequestError as exc:
            logger.error("SP-API retries exhausted for %s: %s", request_key, exc)
            self._circuit_breaker.record_failure()
            return ServiceResult(data=None, flags=ServiceFlags(degraded=True, reason="retry_exhausted"))
        except AmazonSPAPIError:
            self._circuit_breaker.record_failure()
            raise
        except Exception as exc:  # pragma: no cover - defensive safeguard
            self._circuit_breaker.record_failure()
            raise AmazonSPAPIError(f"Unexpected SP-API failure: {exc}") from exc

        self._circuit_breaker.record_success()
        return ServiceResult(data=response, flags=ServiceFlags())

    def _send_once(
        self,
        method: str,
        path: str,
        request_key: str,
        headers: Dict[str, str],
        kwargs: Dict[str, object],
    ) -> requests.Response:
        self._budget.consume(request_key, self._budget_limit)
        with self._semaphore.acquire(request_key):
            signed_headers, prepared_kwargs = self._prepare_and_sign(method, path, headers, kwargs)
            try:
                response = self._session.request(
                    method=method,
                    url=f"{self._endpoint}{path}",
                    headers=signed_headers,
                    timeout=REQUEST_TIMEOUT,
                    **prepared_kwargs,
                )
            except requests.Timeout as exc:
                raise RetryableRequestError(f"timeout: {exc}") from exc
            except requests.ConnectionError as exc:
                raise RetryableRequestError(f"connection error: {exc}") from exc
            except requests.RequestException as exc:
                raise AmazonSPAPIError(f"Amazon SP-API request failed: {exc}") from exc

        if response.status_code in RETRYABLE_STATUS_CODES:
            raise RetryableRequestError(f"retryable status {response.status_code} for path {path}")
        if response.status_code >= 400:
            raise AmazonSPAPIError(
                f"Amazon SP-API request failed (status={response.status_code}, detail={response.text})"
            )
        return response

    def _prepare_and_sign(
        self,
        method: str,
        path: str,
        headers: Dict[str, str],
        kwargs: Dict[str, object],
    ) -> tuple[Dict[str, str], Dict[str, object]]:
        prepared_headers = dict(headers)
        prepared_headers["host"] = self._host
        prepared_kwargs: Dict[str, object] = dict(kwargs)

        body_bytes: Optional[bytes] = None
        if "json" in prepared_kwargs:
            body_obj = prepared_kwargs.pop("json")
            body_str = json.dumps(body_obj, separators=(",", ":"), default=str)
            body_bytes = body_str.encode("utf-8")
            prepared_kwargs["data"] = body_bytes
            prepared_headers.setdefault("Content-Type", "application/json")
        elif "data" in prepared_kwargs:
            data_value = prepared_kwargs["data"]
            if isinstance(data_value, bytes):
                body_bytes = data_value
            elif isinstance(data_value, str):
                body_bytes = data_value.encode("utf-8")
            elif data_value is not None:
                body_bytes = json.dumps(data_value, default=str).encode("utf-8")
                prepared_kwargs["data"] = body_bytes
        params = prepared_kwargs.get("params")
        if params is None:
            prepared_kwargs.pop("params", None)

        signed_headers = self._signer.sign(
            method=method,
            url=f"{self._endpoint}{path}",
            headers=prepared_headers,
            params=params if isinstance(params, dict) else None,
            body=body_bytes or b"",
        )
        if "content-type" in signed_headers and "Content-Type" not in signed_headers:
            signed_headers["Content-Type"] = signed_headers["content-type"]

        if body_bytes is not None:
            prepared_kwargs["data"] = body_bytes
        else:
            prepared_kwargs.pop("data", None)
        if params is not None:
            prepared_kwargs["params"] = params

        return signed_headers, prepared_kwargs


class SigV4Signer:
    def __init__(self, access_key: str, secret_key: str, region: str, service: str) -> None:
        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region
        self._service = service

    def sign(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        params: Optional[Dict[str, object]],
        body: bytes,
    ) -> Dict[str, str]:
        timestamp = datetime.now(timezone.utc)
        amz_date = timestamp.strftime("%Y%m%dT%H%M%SZ")
        datestamp = timestamp.strftime("%Y%m%d")

        normalized_headers = {key: " ".join(str(value).strip().split()) for key, value in headers.items()}
        canonical_headers = {key.lower(): value for key, value in normalized_headers.items()}
        payload_hash = hashlib.sha256(body).hexdigest()
        canonical_headers["x-amz-date"] = amz_date
        canonical_headers["x-amz-content-sha256"] = payload_hash

        canonical_request = self._canonical_request(method, url, canonical_headers, params, payload_hash)
        string_to_sign = self._string_to_sign(canonical_request, amz_date, datestamp)
        signing_key = self._derive_signing_key(datestamp)
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

        authorization = (
            "AWS4-HMAC-SHA256 "
            f"Credential={self._access_key}/{datestamp}/{self._region}/{self._service}/aws4_request, "
            f"SignedHeaders={self._signed_headers(canonical_headers)}, Signature={signature}"
        )

        canonical_headers["authorization"] = authorization

        final_headers = dict(canonical_headers)
        final_headers["Authorization"] = final_headers.pop("authorization")
        final_headers["X-Amz-Date"] = amz_date
        final_headers.setdefault("x-amz-date", amz_date)
        final_headers["X-Amz-Content-Sha256"] = payload_hash
        final_headers.setdefault("x-amz-content-sha256", payload_hash)
        return final_headers

    def _canonical_request(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        params: Optional[Dict[str, object]],
        payload_hash: str,
    ) -> str:
        parsed = urlparse(url)
        canonical_uri = parsed.path or "/"
        canonical_query = self._canonical_query(params)
        canonical_headers = self._canonical_headers(headers)
        signed_headers = self._signed_headers(headers)
        return "\n".join(
            [
                method.upper(),
                canonical_uri,
                canonical_query,
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )

    def _string_to_sign(self, canonical_request: str, amz_date: str, datestamp: str) -> str:
        scope = f"{datestamp}/{self._region}/{self._service}/aws4_request"
        hashed_request = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
        return "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashed_request])

    def _derive_signing_key(self, datestamp: str) -> bytes:
        key_date = self._hmac(("AWS4" + self._secret_key).encode("utf-8"), datestamp)
        key_region = self._hmac(key_date, self._region)
        key_service = self._hmac(key_region, self._service)
        return self._hmac(key_service, "aws4_request")

    def _canonical_query(self, params: Optional[Dict[str, object]]) -> str:
        if not params:
            return ""
        items: List[Tuple[str, str]] = []
        for key, value in params.items():
            if value is None:
                continue
            values = value if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) else [value]
            for entry in values:
                items.append((self._encode(str(key)), self._encode(str(entry))))
        items.sort()
        return "&".join(f"{key}={value}" for key, value in items)

    @staticmethod
    def _canonical_headers(headers: Dict[str, str]) -> str:
        pairs = sorted(headers.items())
        return "".join(f"{key}:{value}\n" for key, value in pairs)

    @staticmethod
    def _signed_headers(headers: Dict[str, str]) -> str:
        return ";".join(sorted(headers.keys()))

    @staticmethod
    def _encode(value: str) -> str:
        return quote(value, safe="-_.~")

    @staticmethod
    def _hmac(key: bytes, data: str) -> bytes:
        return hmac.new(key, data.encode("utf-8"), hashlib.sha256).digest()
