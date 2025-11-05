from __future__ import annotations

import logging
from dataclasses import asdict
from decimal import Decimal
from typing import Dict, Iterable, List, Optional

from src.common.config_loader import ConfigError, load_settings
from src.common.models import (
    CompetitivePrice,
    FeeBreakdown,
    KeepaPriceSnapshot,
    ProductListing,
    ProductQuery,
    ProfitAnalysis,
    PurchaseDecision,
)
from src.services.amazon_sp_api import AmazonSPAPIClient, AmazonSPAPIError
from src.services.google_sheets import GoogleSheetsError, GoogleSheetsService
from src.services.keepa_api import KeepaAPIClient, KeepaAPIError
from src.services.notification import NotificationError, NotificationService
from src.services.profit_calculator import ProfitComputationError, calculate_profit
from src.services.selenium_uploader import AmazonListingAutomation, ListingAutomationError
from src.services.sp_auth import SellingPartnerAuthenticator


logger = logging.getLogger(__name__)


class ScrapeAgent:
    def __init__(
        self,
        settings: Optional[Dict[str, Dict[str, object]]] = None,
        env: Optional[str] = None,
    ) -> None:
        try:
            self._settings = settings or load_settings(env=env)
        except ConfigError as exc:
            logger.error("Configuration loading failed: %s", exc)
            raise

        amazon_settings = self._settings["amazon"]
        runtime = self._settings.get("runtime", {})
        self._thresholds = self._settings.get("thresholds", {})

        self._authenticator = SellingPartnerAuthenticator(amazon_settings)
        self._amazon_client = AmazonSPAPIClient(
            {**amazon_settings, "default_currency": runtime.get("default_currency", "JPY")},
            self._authenticator.get_access_token,
        )
        self._keepa_client = KeepaAPIClient(self._settings["keepa"]["api_key"])
        self._notification_service = NotificationService(self._settings.get("notifications", {}))

        try:
            self._sheets_service: Optional[GoogleSheetsService] = GoogleSheetsService(self._settings["google_sheets"])
        except (KeyError, GoogleSheetsError) as exc:
            logger.warning("Google Sheets integration disabled: %s", exc)
            self._sheets_service = None

        try:
            self._automation: Optional[AmazonListingAutomation] = AmazonListingAutomation(self._settings["listing"])
        except (KeyError, ListingAutomationError) as exc:
            logger.warning("Listing automation disabled: %s", exc)
            self._automation = None

    def run(
        self,
        asin: Optional[str],
        barcode: Optional[str],
        purchase_cost: Decimal,
        shipping_fees: Decimal,
        taxes: Decimal,
        target_price: Optional[Decimal] = None,
    ) -> Dict[str, object]:
        query = ProductQuery(asin=asin, barcode=barcode)
        try:
            keepa_snapshot = self._keepa_client.get_price_snapshot(query)
            competitive_prices = self._amazon_client.get_competitive_pricing(query)
        except (KeepaAPIError, AmazonSPAPIError) as exc:
            logger.error("Data collection failed: %s", exc)
            raise

        selling_price = target_price or self._determine_selling_price(competitive_prices, keepa_snapshot)

        try:
            fee_breakdown = self._amazon_client.get_fees_estimate(asin or barcode, selling_price)
        except AmazonSPAPIError as exc:
            logger.warning("Falling back to zero Amazon fees due to error: %s", exc)
            fee_breakdown = FeeBreakdown(
                referral_fee=Decimal("0"),
                closing_fee=Decimal("0"),
                fba_fee=Decimal("0"),
                shipping_fee=Decimal("0"),
                taxes=Decimal("0"),
            )

        fee_breakdown = FeeBreakdown(
            referral_fee=fee_breakdown.referral_fee,
            closing_fee=fee_breakdown.closing_fee,
            fba_fee=fee_breakdown.fba_fee,
            shipping_fee=shipping_fees,
            taxes=taxes,
        )

        try:
            profit_analysis = calculate_profit(selling_price, purchase_cost, fee_breakdown)
        except ProfitComputationError as exc:
            logger.error("Profit calculation failed: %s", exc)
            raise

        decision = self._make_decision(profit_analysis, keepa_snapshot, competitive_prices)

        listing = self._build_listing(asin or (barcode or ""), selling_price, keepa_snapshot)

        result = {
            "query": asdict(query),
            "keepa": _serialize_keepa(keepa_snapshot),
            "competitive_prices": [_serialize_competitive_price(price) for price in competitive_prices],
            "profit": _serialize_profit(profit_analysis),
            "decision": {
                "is_profitable": decision.is_profitable,
                "meets_thresholds": decision.meets_thresholds,
                "reasons": decision.reasons,
            },
            "listing": _serialize_listing(listing),
        }

        if decision.meets_thresholds:
            self._handle_positive_decision(result, listing, profit_analysis)

        return result

    def _determine_selling_price(
        self,
        competitive_prices: List[CompetitivePrice],
        keepa_snapshot: KeepaPriceSnapshot,
    ) -> Decimal:
        if competitive_prices:
            best_offer = min(competitive_prices, key=lambda offer: offer.landed_price)
            return best_offer.landed_price

        logger.info("No competitive price data available; falling back to Keepa current price")
        return keepa_snapshot.current_price or Decimal("0")

    def _make_decision(
        self,
        profit_analysis: ProfitAnalysis,
        keepa_snapshot: KeepaPriceSnapshot,
        prices: Iterable[CompetitivePrice],
    ) -> PurchaseDecision:
        reasons: List[str] = []
        min_profit = Decimal(str(self._thresholds.get("minimum_profit", "0")))
        min_roi = Decimal(str(self._thresholds.get("minimum_roi", "0")))
        max_rank = self._thresholds.get("max_rank")
        max_rank_value = int(max_rank) if max_rank else None

        is_profitable = profit_analysis.profit > 0
        meets_profit = profit_analysis.profit >= min_profit
        meets_roi = profit_analysis.roi >= min_roi
        meets_rank = (
            True
            if max_rank_value is None
            else keepa_snapshot.sales_rank is None or keepa_snapshot.sales_rank <= max_rank_value
        )

        if not meets_profit:
            reasons.append(f"Profit {profit_analysis.profit} below threshold {min_profit}")
        if not meets_roi:
            reasons.append(f"ROI {profit_analysis.roi} below threshold {min_roi}")
        if not meets_rank:
            reasons.append(f"Sales rank {keepa_snapshot.sales_rank} exceeds {max_rank}")
        if not any(True for _ in prices):
            reasons.append("No competitive offers available")

        meets_thresholds = is_profitable and meets_profit and meets_roi and meets_rank

        return PurchaseDecision(
            is_profitable=is_profitable,
            meets_thresholds=meets_thresholds,
            reasons=reasons,
        )

    def _build_listing(
        self,
        asin: str,
        selling_price: Decimal,
        keepa_snapshot: KeepaPriceSnapshot,
    ) -> ProductListing:
        title = keepa_snapshot.title or f"ASIN {asin}"
        description = (
            f"{title}\n"
            f"Current 30-day average price: {keepa_snapshot.average_price_30d}\n"
            f"Lowest 30-day price: {keepa_snapshot.lowest_price_30d}\n"
            f"Highest 30-day price: {keepa_snapshot.highest_price_30d}"
        )
        return ProductListing(
            asin=asin,
            title=title,
            price=selling_price,
            description=description,
            image_urls=keepa_snapshot.image_urls,
            currency=keepa_snapshot.currency,
        )

    def _handle_positive_decision(
        self,
        result: Dict[str, object],
        listing: ProductListing,
        profit: ProfitAnalysis,
    ) -> None:
        message = f"仕入れOK: {listing.title} (ASIN: {listing.asin}) 利益: {profit.profit} ROI: {profit.roi}"
        try:
            self._notification_service.notify("仕入れ判定結果", message, payload=result)
        except NotificationError as exc:
            logger.error("Notification failed: %s", exc)

        if self._sheets_service:
            try:
                self._sheets_service.append_listing(listing, profit)
            except GoogleSheetsError as exc:
                logger.error("Google Sheets update failed: %s", exc)

        if self._automation:
            try:
                self._automation.publish_listing(listing)
            except ListingAutomationError as exc:
                logger.error("Amazon listing automation failed: %s", exc)


def _serialize_keepa(snapshot: KeepaPriceSnapshot) -> Dict[str, object]:
    return {
        "current_price": str(snapshot.current_price),
        "average_price_30d": str(snapshot.average_price_30d),
        "lowest_price_30d": str(snapshot.lowest_price_30d),
        "highest_price_30d": str(snapshot.highest_price_30d),
        "sales_rank": snapshot.sales_rank,
        "currency": snapshot.currency,
        "title": snapshot.title,
        "image_urls": snapshot.image_urls,
    }


def _serialize_competitive_price(price: CompetitivePrice) -> Dict[str, object]:
    return {
        "condition": price.condition,
        "seller_id": price.seller_id,
        "landed_price": str(price.landed_price),
        "shipping": str(price.shipping),
        "last_updated": price.last_updated.isoformat(),
    }


def _serialize_profit(profit: ProfitAnalysis) -> Dict[str, object]:
    return {
        "selling_price": str(profit.selling_price),
        "purchase_cost": str(profit.purchase_cost),
        "fees": {
            "referral_fee": str(profit.fees.referral_fee),
            "closing_fee": str(profit.fees.closing_fee),
            "fba_fee": str(profit.fees.fba_fee),
            "shipping_fee": str(profit.fees.shipping_fee),
            "taxes": str(profit.fees.taxes),
            "total": str(profit.fees.total),
        },
        "profit": str(profit.profit),
        "roi": str(profit.roi),
        "margin": str(profit.margin),
    }


def _serialize_listing(listing: ProductListing) -> Dict[str, object]:
    return {
        "asin": listing.asin,
        "title": listing.title,
        "price": str(listing.price),
        "description": listing.description,
        "image_urls": listing.image_urls,
        "currency": listing.currency,
    }
