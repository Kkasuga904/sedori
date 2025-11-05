from decimal import Decimal

from src.common.models import FeeBreakdown
from src.services.profit_calculator import calculate_profit


def test_calculate_profit_returns_expected_metrics() -> None:
    fees = FeeBreakdown(
        referral_fee=Decimal("150"),
        closing_fee=Decimal("80"),
        fba_fee=Decimal("300"),
        shipping_fee=Decimal("200"),
        taxes=Decimal("50"),
    )
    analysis = calculate_profit(
        selling_price=Decimal("2500"),
        purchase_cost=Decimal("1200"),
        fees=fees,
    )

    assert analysis.profit == Decimal("520.00")
    assert analysis.roi == Decimal("0.4333")
    assert analysis.margin == Decimal("0.2080")
