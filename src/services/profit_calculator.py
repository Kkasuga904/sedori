from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Dict

from src.common.models import FeeBreakdown, ProfitAnalysis


class ProfitComputationError(RuntimeError):
    """Raised when profit calculation inputs are invalid."""


def calculate_profit(
    selling_price: Decimal,
    purchase_cost: Decimal,
    fees: FeeBreakdown,
) -> ProfitAnalysis:
    """
    Calculate profit, ROI, and margin for a product.

    Args:
        selling_price: Final sale price charged to the customer.
        purchase_cost: Total acquisition cost.
        fees: Detailed breakdown of Amazon and logistics fees.

    Returns:
        Structured ``ProfitAnalysis`` containing profit metrics.
    """

    try:
        profit = selling_price - purchase_cost - fees.total
        roi = _safe_divide(profit, purchase_cost)
        margin = _safe_divide(profit, selling_price)
    except InvalidOperation as exc:
        raise ProfitComputationError(f"Invalid decimal input: {exc}") from exc

    return ProfitAnalysis(
        selling_price=_quantize(selling_price),
        purchase_cost=_quantize(purchase_cost),
        fees=fees,
        profit=_quantize(profit),
        roi=_quantize_ratio(roi),
        margin=_quantize_ratio(margin),
    )


def _safe_divide(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return numerator / denominator


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(Decimal(".01"), rounding=ROUND_HALF_UP)


def _quantize_ratio(value: Decimal) -> Decimal:
    return value.quantize(Decimal(".0001"), rounding=ROUND_HALF_UP)
