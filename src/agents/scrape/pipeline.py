from __future__ import annotations

import json
import logging
import random
import time
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

from src.common.config import Settings
from src.common.config_loader import ConfigError, load_settings
from src.common.logging import extra_logger
from src.common.models import (
    CompetitivePrice,
    FeeBreakdown,
    KeepaPriceSnapshot,
    ProductListing,
    ProductQuery,
    ProfitAnalysis,
    PurchaseDecision,
    ServiceFlags,
)
from src.common.profit import basis_points
from src.common.rate_limit import CircuitBreaker, KeySemaphore, RequestBudget
from src.services.amazon_sp_api import AmazonSPAPIClient, AmazonSPAPIError
from src.services.google_sheets import GoogleSheetsError, GoogleSheetsService
from src.services.keepa_api import KeepaAPIClient, KeepaAPIError
from src.services.notifier import NotificationError, Notifier
from src.services.profit_calculator import ProfitComputationError, calculate_profit
from src.services.sp_auth import SellingPartnerAuthenticator, TokenAcquisitionError

logger = logging.getLogger(__name__)


def _default_keepa_snapshot() -> KeepaPriceSnapshot:
    return KeepaPriceSnapshot(
        current_price=Decimal("0"),
        average_price_30d=Decimal("0"),
        lowest_price_30d=Decimal("0"),
        highest_price_30d=Decimal("0"),
        sales_rank=None,
        currency="JPY",
        title=None,
        image_urls=[],
    )


class ScrapeAgent:
    def __init__(
        self,
        settings: Optional[Settings] = None,
        env: Optional[str] = None,
    ) -> None:
        try:
            self._settings = settings or load_settings(env=env)
        except ConfigError as exc:
            logger.error("Configuration loading failed: %s", exc)
            raise

        self._sp_budget = RequestBudget()
        self._keepa_budget = RequestBudget()
        self._sp_circuit = CircuitBreaker()
        self._keepa_circuit = CircuitBreaker()
        self._sp_semaphore = KeySemaphore(self._settings.cli.spapi_max_inflight)
        self._keepa_semaphore = KeySemaphore(self._settings.cli.keepa_max_inflight)

        self._authenticator = SellingPartnerAuthenticator(self._settings.api.spapi, self._settings.retry)
        self._amazon_client = AmazonSPAPIClient(
            self._settings.api.spapi,
            self._settings.retry,
            self._authenticator.get_access_token,
            budget=self._sp_budget,
            budget_limit=self._settings.budget.spapi,
            semaphore=self._sp_semaphore,
            circuit_breaker=self._sp_circuit,
        )
        self._keepa_client = KeepaAPIClient(
            self._settings.api.keepa,
            self._settings.retry,
            self._settings.cache,
            budget=self._keepa_budget,
            budget_limit=self._settings.budget.keepa,
            semaphore=self._keepa_semaphore,
            circuit_breaker=self._keepa_circuit,
        )
        self._notifier = Notifier(self._settings.notify.slack, self._settings.notify.line, self._settings.retry)

        if self._settings.google_sheets:
            try:
                self._sheets_service: Optional[GoogleSheetsService] = GoogleSheetsService(
                    self._settings.google_sheets.model_dump()
                )
            except GoogleSheetsError as exc:
                logger.warning("Google Sheets integration disabled: %s", exc)
                self._sheets_service = None
        else:
            self._sheets_service = None

    def run(
        self,
        asin: Optional[str],
        barcode: Optional[str],
        purchase_cost: Decimal,
        inbound_shipping_override: Optional[Decimal] = None,
        packaging_override: Optional[Decimal] = None,
        storage_fee_override: Optional[Decimal] = None,
        taxes: Optional[Decimal] = None,
        target_price: Optional[Decimal] = None,
        fx_spread_override: Optional[int] = None,
        return_rate_override: Optional[Decimal] = None,
        notify_slack: bool = False,
        notify_line: bool = False,
        dry_run: bool = False,
        decision_path: Optional[Path] = None,
        request_id: Optional[str] = None,
    ) -> Dict[str, object]:
        if not (asin or barcode):
            raise ValueError("Either asin or barcode must be provided")

        request_id = request_id or str(uuid.uuid4())
        context_logger = extra_logger(__name__, request_id=request_id, asin=asin or barcode or "unknown")

        query = ProductQuery(asin=asin, barcode=barcode)
        flags_state: Dict[str, object] = {"degraded": False, "cached": False, "circuit_open": False, "reasons": []}

        try:
            keepa_result = self._keepa_client.get_price_snapshot(query)
            keepa_flags = keepa_result.flags
            keepa_snapshot = keepa_result.data or _default_keepa_snapshot()
        except KeepaAPIError as exc:
            context_logger.error("Keepa data collection failed: %s", exc)
            keepa_flags = ServiceFlags(degraded=True, reason="keepa_error")
            keepa_snapshot = _default_keepa_snapshot()
        _merge_flags(flags_state, keepa_flags)

        self._stagger()

        try:
            competitive_result = self._amazon_client.get_competitive_pricing(query)
            competitive_flags = competitive_result.flags
            offers = competitive_result.data or []
        except (AmazonSPAPIError, TokenAcquisitionError) as exc:
            context_logger.error("SP-API competitive pricing failed: %s", exc)
            competitive_flags = ServiceFlags(degraded=True, reason="spapi_pricing_error")
            offers = []
        _merge_flags(flags_state, competitive_flags)

        selling_price = self._determine_selling_price(target_price, offers, keepa_snapshot)

        self._stagger()

        try:
            fees_result = self._amazon_client.get_fees_estimate(asin or barcode, selling_price)
            fees_flags = fees_result.flags
            base_fees = fees_result.data or FeeBreakdown()
        except (AmazonSPAPIError, TokenAcquisitionError) as exc:
            context_logger.warning("SP-API fees estimate unavailable: %s", exc)
            fees_flags = ServiceFlags(degraded=True, reason="spapi_fee_error")
            base_fees = FeeBreakdown()
        _merge_flags(flags_state, fees_flags)

        rounding = self._settings.money.rounding
        fx_spread_bp = fx_spread_override if fx_spread_override is not None else self._settings.money.fx_spread_bp
        return_rate = return_rate_override if return_rate_override is not None else self._settings.money.return_rate

        fees = FeeBreakdown(
            referral_fee=base_fees.referral_fee,
            closing_fee=base_fees.closing_fee,
            fba_fee=base_fees.fba_fee,
            inbound_shipping=inbound_shipping_override if inbound_shipping_override is not None else self._settings.money.inbound_shipping,
            packaging_materials=packaging_override if packaging_override is not None else self._settings.money.packaging_materials,
            storage_fee=storage_fee_override if storage_fee_override is not None else self._settings.money.storage_fee_monthly,
            taxes=(taxes or Decimal("0")) + base_fees.taxes,
            fx_spread=basis_points(selling_price, fx_spread_bp),
            returns_cost=selling_price * return_rate,
            other_costs=base_fees.other_costs,
        )

        try:
            profit_analysis = calculate_profit(selling_price, purchase_cost, fees, rounding)
        except ProfitComputationError as exc:
            context_logger.error("Profit calculation failed: %s", exc)
            raise

        decision = self._make_decision(profit_analysis, keepa_snapshot, offers, flags_state)

        listing = self._build_listing(asin or (barcode or ""), selling_price, keepa_snapshot)

        result = self._build_result(
            request_id=request_id,
            query=query,
            purchase_cost=purchase_cost,
            selling_price=selling_price,
            keepa_snapshot=keepa_snapshot,
            offers=offers,
            profit=profit_analysis,
            thresholds=self._settings.thresholds,
            flags=flags_state,
            decision=decision,
            keepa_flags=keepa_flags,
            competitive_flags=competitive_flags,
            fees_flags=fees_flags,
        )

        if decision_path:
            decision_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

        reasons = result["flags"].get("reasons", [])
        if decision.meets_thresholds and not dry_run and (notify_slack or notify_line):
            summary = self._build_summary(listing, profit_analysis, reasons)
            self._dispatch_notifications(
                summary,
                notify_slack=notify_slack,
                notify_line=notify_line,
                request_id=request_id,
                asin=listing.asin,
            )

        if decision.meets_thresholds and not dry_run and self._sheets_service:
            try:
                self._sheets_service.append_listing(listing, profit_analysis)
            except GoogleSheetsError as exc:
                context_logger.error("Google Sheets update failed: %s", exc)

        return result

    def _stagger(self) -> None:
        delay = random.uniform(0, self._settings.cli.stagger_jitter_seconds)
        if delay > 0:
            time.sleep(delay)

    def _determine_selling_price(
        self,
        target_price: Optional[Decimal],
        offers: Sequence[CompetitivePrice],
        keepa_snapshot: KeepaPriceSnapshot,
    ) -> Decimal:
        if target_price and target_price > 0:
            return target_price
        if offers:
            return min(offers, key=lambda offer: offer.landed_price).landed_price
        if keepa_snapshot.current_price and keepa_snapshot.current_price > 0:
            return keepa_snapshot.current_price
        logger.warning("Falling back to zero selling price due to missing market data")
        return Decimal("0")

    def _make_decision(
        self,
        profit_analysis: ProfitAnalysis,
        keepa_snapshot: KeepaPriceSnapshot,
        offers: Iterable[CompetitivePrice],
        flags_state: Dict[str, object],
    ) -> PurchaseDecision:
        reasons: List[str] = []
        thresholds = self._settings.thresholds
        min_profit = thresholds.min_profit
        min_roi = thresholds.min_roi
        max_rank = thresholds.max_rank

        is_profitable = profit_analysis.profit > 0
        meets_profit = profit_analysis.profit >= min_profit
        meets_roi = profit_analysis.roi >= min_roi
        meets_rank = (
            True
            if max_rank is None
            else keepa_snapshot.sales_rank is None or keepa_snapshot.sales_rank <= max_rank
        )
        has_offers = any(True for _ in offers)

        if not meets_profit:
            reasons.append("profit_below_threshold")
        if not meets_roi:
            reasons.append("roi_below_threshold")
        if not meets_rank:
            reasons.append("rank_above_threshold")
        if not has_offers:
            reasons.append("no_competitive_offers")
        if flags_state.get("degraded"):
            reasons.append("degraded_inputs")

        meets_thresholds = (
            is_profitable
            and meets_profit
            and meets_roi
            and meets_rank
            and has_offers
            and not flags_state.get("degraded")
        )

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
            f"30d avg price: {keepa_snapshot.average_price_30d}\n"
            f"30d lowest price: {keepa_snapshot.lowest_price_30d}\n"
            f"30d highest price: {keepa_snapshot.highest_price_30d}"
        )
        return ProductListing(
            asin=asin,
            title=title,
            price=selling_price,
            description=description,
            image_urls=keepa_snapshot.image_urls,
            currency=keepa_snapshot.currency,
        )

    def _dispatch_notifications(
        self,
        summary: str,
        notify_slack: bool,
        notify_line: bool,
        request_id: str,
        asin: str,
    ) -> None:
        errors = []
        if notify_slack:
            try:
                self._notifier.post_slack(summary)
            except NotificationError as exc:
                errors.append(f"slack: {exc}")
        if notify_line:
            try:
                self._notifier.post_line(summary)
            except NotificationError as exc:
                errors.append(f"line: {exc}")
        if errors:
            extra_logger(__name__, request_id=request_id, asin=asin).error("Notification failed: %s", "; ".join(errors))

    def _build_summary(
        self,
        listing: ProductListing,
        profit: ProfitAnalysis,
        reasons: Sequence[str],
    ) -> str:
        selling_price = self._format_currency(profit.selling_price)
        profit_value = self._format_currency(profit.profit)
        roi_value = self._format_roi(profit.roi)
        primary_reason = reasons[0] if reasons else "thresholds_met"
        return f"ASIN: {listing.asin} | {selling_price} | profit {profit_value} | ROI {roi_value} | reason: {primary_reason}"

    def _format_currency(self, amount: Decimal) -> str:
        quantum = self._settings.money.rounding
        try:
            quantized = amount.quantize(quantum)
        except (InvalidOperation, ValueError):
            quantized = amount
        return f"¥{format(quantized, 'f')}"

    @staticmethod
    def _format_roi(roi: Decimal) -> str:
        try:
            return f"{float(roi):.1%}"
        except (TypeError, ValueError, OverflowError):
            return "0.0%"

    def _build_result(
        self,
        request_id: str,
        query: ProductQuery,
        purchase_cost: Decimal,
        selling_price: Decimal,
        keepa_snapshot: KeepaPriceSnapshot,
        offers: Sequence[CompetitivePrice],
        profit: ProfitAnalysis,
        thresholds,
        flags: Dict[str, object],
        decision: PurchaseDecision,
        keepa_flags: ServiceFlags,
        competitive_flags: ServiceFlags,
        fees_flags: ServiceFlags,
    ) -> Dict[str, object]:
        aggregated_reasons = sorted({*decision.reasons, *(flags.get("reasons", []))})
        flags["reasons"] = aggregated_reasons
        calc_payload = _serialize_profit(profit)
        return {
            "request_id": request_id,
            "inputs": {
                "asin": query.asin,
                "barcode": query.barcode,
                "purchase_cost": str(purchase_cost),
                "selling_price": str(selling_price),
            },
            "sources": {
                "keepa": {
                    "flags": _flags_to_dict(keepa_flags),
                    "snapshot": _serialize_keepa(keepa_snapshot),
                },
                "competitive": {
                    "flags": _flags_to_dict(competitive_flags),
                    "offers": [_serialize_competitive_price(price) for price in offers],
                },
                "fees": {
                    "flags": _flags_to_dict(fees_flags),
                    "breakdown": calc_payload["fees"],
                },
            },
            "calc": calc_payload,
            "thresholds": {
                "min_profit": str(thresholds.min_profit),
                "min_roi": str(thresholds.min_roi),
                "max_rank": thresholds.max_rank,
            },
            "flags": flags,
            "decision": {
                "buy": decision.meets_thresholds,
                "profitable": decision.is_profitable,
                "reasons": aggregated_reasons,
            },
        }


def _flags_to_dict(flags: ServiceFlags) -> Dict[str, object]:
    return {
        "degraded": flags.degraded,
        "cached": flags.cached,
        "circuit_open": flags.circuit_open,
        "reason": flags.reason,
    }


def _merge_flags(state: Dict[str, object], flags: ServiceFlags) -> None:
    if flags.degraded:
        state["degraded"] = True
    if flags.cached:
        state["cached"] = True
    if flags.circuit_open:
        state["circuit_open"] = True
    if flags.reason:
        reasons = state.setdefault("reasons", [])
        reasons.append(flags.reason)


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
        "total_cost": str(profit.total_cost),
        "fees": {
            "referral_fee": str(profit.fees.referral_fee),
            "closing_fee": str(profit.fees.closing_fee),
            "fba_fee": str(profit.fees.fba_fee),
            "inbound_shipping": str(profit.fees.inbound_shipping),
            "packaging_materials": str(profit.fees.packaging_materials),
            "storage_fee": str(profit.fees.storage_fee),
            "taxes": str(profit.fees.taxes),
            "fx_spread": str(profit.fees.fx_spread),
            "returns_cost": str(profit.fees.returns_cost),
            "other_costs": str(profit.fees.other_costs),
            "total": str(profit.fees.total),
        },
        "profit": str(profit.profit),
        "roi": str(profit.roi),
        "margin": str(profit.margin),
    }



