from __future__ import annotations

from decimal import Decimal
from typing import Dict, Optional

from pydantic import BaseModel, Field, SecretStr, field_validator


class RetrySettings(BaseModel):
    max_attempts: int = Field(default=5, ge=1)
    base: float = Field(default=0.5, gt=0)
    max_sleep: float = Field(default=10.0, gt=0)


class CacheSettings(BaseModel):
    ttl_seconds: int = Field(default=1800, ge=0)
    cleanup_interval: int = Field(default=300, ge=0)


class BudgetSettings(BaseModel):
    spapi: int = Field(default=120, ge=1)
    keepa: int = Field(default=150, ge=1)


class MoneySettings(BaseModel):
    rounding: Decimal = Field(default=Decimal("0.01"))
    fx_spread_bp: int = Field(default=0, ge=0)
    return_rate: Decimal = Field(default=Decimal("0.0"))
    storage_fee_monthly: Decimal = Field(default=Decimal("0"))
    inbound_shipping: Decimal = Field(default=Decimal("0"))
    packaging_materials: Decimal = Field(default=Decimal("0"))

    @field_validator("rounding", "return_rate", "storage_fee_monthly", "inbound_shipping", "packaging_materials", mode="before")
    @classmethod
    def _ensure_decimal(cls, value: object) -> Decimal:  # noqa: D401
        """Cast numeric inputs to ``Decimal`` for consistent handling."""

        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))


class ThresholdSettings(BaseModel):
    min_profit: Decimal = Field(default=Decimal("0"))
    min_roi: Decimal = Field(default=Decimal("0"))
    max_rank: Optional[int] = Field(default=None, ge=0)

    @field_validator("min_profit", "min_roi", mode="before")
    @classmethod
    def _ensure_decimal(cls, value: object) -> Decimal:  # noqa: D401
        """Cast numeric inputs to ``Decimal`` for consistent handling."""

        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))


class SlackSettings(BaseModel):
    enabled: bool = False
    channel: Optional[str] = None
    webhook: Optional[str] = None
    token: Optional[SecretStr] = None


class LineSettings(BaseModel):
    enabled: bool = False
    token: Optional[SecretStr] = None


class NotifySettings(BaseModel):
    slack: SlackSettings = Field(default_factory=SlackSettings)
    line: LineSettings = Field(default_factory=LineSettings)


class GoogleSheetsSettings(BaseModel):
    credentials_file: str
    spreadsheet_id: str
    worksheet_name: str


class KeepaSettings(BaseModel):
    api_key: SecretStr
    domain: int = Field(default=5, ge=1)


class SPAPISettings(BaseModel):
    marketplace_id: str
    region: str
    lwa_client_id: SecretStr
    lwa_client_secret: SecretStr
    refresh_token: SecretStr
    aws_access_key: SecretStr
    aws_secret_key: SecretStr
    role_arn: str
    default_currency: str = Field(default="JPY")


class APISettings(BaseModel):
    spapi: SPAPISettings
    keepa: KeepaSettings


class ObservabilitySettings(BaseModel):
    json_logs: bool = True
    log_level: str = Field(default="INFO")


class CLISettings(BaseModel):
    stagger_jitter_seconds: float = Field(default=0.4, ge=0)
    spapi_max_inflight: int = Field(default=1, ge=1)
    keepa_max_inflight: int = Field(default=1, ge=1)


class Settings(BaseModel):
    api: APISettings
    notify: NotifySettings = Field(default_factory=NotifySettings)
    thresholds: ThresholdSettings = Field(default_factory=ThresholdSettings)
    retry: RetrySettings = Field(default_factory=RetrySettings)
    cache: CacheSettings = Field(default_factory=CacheSettings)
    money: MoneySettings = Field(default_factory=MoneySettings)
    budget: BudgetSettings = Field(default_factory=BudgetSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    cli: CLISettings = Field(default_factory=CLISettings)
    google_sheets: Optional[GoogleSheetsSettings] = None

    def secrets_for_redaction(self) -> Dict[str, str]:
        """Return a mapping of secret labels to their redacted values."""

        spapi = self.api.spapi
        secrets = {
            "lwa_client_id": spapi.lwa_client_id.get_secret_value(),
            "lwa_client_secret": spapi.lwa_client_secret.get_secret_value(),
            "refresh_token": spapi.refresh_token.get_secret_value(),
            "aws_access_key": spapi.aws_access_key.get_secret_value(),
            "aws_secret_key": spapi.aws_secret_key.get_secret_value(),
            "keepa_api_key": self.api.keepa.api_key.get_secret_value(),
        }
        slack = self.notify.slack
        slack_token = slack.token.get_secret_value() if slack.token else None
        if slack_token:
            secrets["slack_token"] = slack_token
        if slack.webhook:
            secrets["slack_webhook"] = slack.webhook
        line = self.notify.line
        line_token = line.token.get_secret_value() if line.token else None
        if line_token:
            secrets["line_token"] = line_token
        return {key: value for key, value in secrets.items() if value}








