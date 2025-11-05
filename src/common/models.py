from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import List, Optional


@dataclass(frozen=True)
class ProductQuery:
    """User-supplied product identifier."""

    asin: Optional[str] = None
    barcode: Optional[str] = None

    def __post_init__(self) -> None:
        if not (self.asin or self.barcode):
            raise ValueError("Either asin or barcode must be provided")


@dataclass(frozen=True)
class CompetitivePrice:
    condition: str
    seller_id: str
    landed_price: Decimal
    shipping: Decimal
    last_updated: datetime


@dataclass(frozen=True)
class KeepaPriceSnapshot:
    current_price: Decimal
    average_price_30d: Decimal
    lowest_price_30d: Decimal
    highest_price_30d: Decimal
    sales_rank: Optional[int] = None
    currency: str = "JPY"
    title: Optional[str] = None
    image_urls: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class FeeBreakdown:
    referral_fee: Decimal
    closing_fee: Decimal
    fba_fee: Decimal
    shipping_fee: Decimal
    taxes: Decimal

    @property
    def total(self) -> Decimal:
        return (
            self.referral_fee
            + self.closing_fee
            + self.fba_fee
            + self.shipping_fee
            + self.taxes
        )


@dataclass(frozen=True)
class ProfitAnalysis:
    selling_price: Decimal
    purchase_cost: Decimal
    fees: FeeBreakdown
    profit: Decimal
    roi: Decimal
    margin: Decimal


@dataclass(frozen=True)
class PurchaseDecision:
    is_profitable: bool
    meets_thresholds: bool
    reasons: List[str] = field(default_factory=list)


@dataclass
class ProductListing:
    asin: str
    title: str
    price: Decimal
    description: str
    image_urls: List[str]
    currency: str = "JPY"
