from __future__ import annotations

from decimal import Decimal, InvalidOperation

from src.common.models import FeeBreakdown, ProfitAnalysis
from src.common.profit import quantize_money, quantize_ratio


class ProfitComputationError(RuntimeError):
    """Raised when profit calculation inputs are invalid."""


def calculate_profit(
    selling_price: Decimal,
    purchase_cost: Decimal,
    fees: FeeBreakdown,
    rounding: Decimal,
) -> ProfitAnalysis:
    """Calculate profit metrics given a detailed fee breakdown."""

    try:
        total_cost = purchase_cost + fees.total
        profit = selling_price - total_cost
        roi = _safe_divide(profit, purchase_cost)
        margin = _safe_divide(profit, selling_price)
    except InvalidOperation as exc:
        raise ProfitComputationError(f"Invalid decimal input: {exc}") from exc

    quantized_fees = FeeBreakdown(
        referral_fee=quantize_money(fees.referral_fee, rounding),
        closing_fee=quantize_money(fees.closing_fee, rounding),
        fba_fee=quantize_money(fees.fba_fee, rounding),
        inbound_shipping=quantize_money(fees.inbound_shipping, rounding),
        packaging_materials=quantize_money(fees.packaging_materials, rounding),
        storage_fee=quantize_money(fees.storage_fee, rounding),
        taxes=quantize_money(fees.taxes, rounding),
        fx_spread=quantize_money(fees.fx_spread, rounding),
        returns_cost=quantize_money(fees.returns_cost, rounding),
        other_costs=quantize_money(fees.other_costs, rounding),
    )

    return ProfitAnalysis(
        selling_price=quantize_money(selling_price, rounding),
        purchase_cost=quantize_money(purchase_cost, rounding),
        fees=quantized_fees,
        total_cost=quantize_money(total_cost, rounding),
        profit=quantize_money(profit, rounding),
        roi=quantize_ratio(roi),
        margin=quantize_ratio(margin),
    )


def _safe_divide(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return numerator / denominator
