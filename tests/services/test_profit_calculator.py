from __future__ import annotations

from decimal import Decimal

from src.common.models import FeeBreakdown
from src.services.profit_calculator import calculate_profit


def test_profit_rounding_quantizes_values() -> None:
    fees = FeeBreakdown(
        referral_fee=Decimal("123.456"),
        closing_fee=Decimal("10.555"),
        fba_fee=Decimal("200.499"),
        inbound_shipping=Decimal("35.333"),
        packaging_materials=Decimal("12.111"),
        storage_fee=Decimal("8.666"),
        taxes=Decimal("5.555"),
        fx_spread=Decimal("4.444"),
        returns_cost=Decimal("3.333"),
        other_costs=Decimal("2.222"),
    )
    result = calculate_profit(
        selling_price=Decimal("1234.567"),
        purchase_cost=Decimal("450.789"),
        fees=fees,
        rounding=Decimal("0.01"),
    )
    assert result.selling_price == Decimal("1234.57")
    assert result.purchase_cost == Decimal("450.79")
    assert result.fees.referral_fee == Decimal("123.46")
    assert result.total_cost == Decimal("856.96")
    assert result.profit == Decimal("377.60")
    assert result.roi == Decimal("0.8377")
    assert result.margin == Decimal("0.3059")


def test_profit_negative_when_costs_exceed_revenue() -> None:
    fees = FeeBreakdown(
        referral_fee=Decimal("300"),
        closing_fee=Decimal("100"),
        fba_fee=Decimal("200"),
        inbound_shipping=Decimal("150"),
        packaging_materials=Decimal("50"),
        storage_fee=Decimal("30"),
        taxes=Decimal("20"),
        fx_spread=Decimal("10"),
        returns_cost=Decimal("40"),
        other_costs=Decimal("25"),
    )
    result = calculate_profit(
        selling_price=Decimal("500"),
        purchase_cost=Decimal("250"),
        fees=fees,
        rounding=Decimal("0.01"),
    )
    assert result.profit == Decimal("-675.00")
    assert not result.profit > 0


def test_profit_handles_zero_purchase_cost() -> None:
    fees = FeeBreakdown(
        referral_fee=Decimal("10"),
        closing_fee=Decimal("5"),
        fba_fee=Decimal("5"),
        inbound_shipping=Decimal("0"),
        packaging_materials=Decimal("0"),
        storage_fee=Decimal("0"),
        taxes=Decimal("0"),
        fx_spread=Decimal("0"),
        returns_cost=Decimal("0"),
        other_costs=Decimal("0"),
    )
    result = calculate_profit(
        selling_price=Decimal("100"),
        purchase_cost=Decimal("0"),
        fees=fees,
        rounding=Decimal("0.01"),
    )
    assert result.roi == Decimal("0.0000")
    assert result.margin == Decimal("0.8000")


def test_profit_supports_integer_rounding() -> None:
    fees = FeeBreakdown(referral_fee=Decimal("1.25"))
    result = calculate_profit(
        selling_price=Decimal("10.4"),
        purchase_cost=Decimal("2.2"),
        fees=fees,
        rounding=Decimal("1"),
    )
    assert result.selling_price == Decimal("10")
    assert result.purchase_cost == Decimal("2")
    assert result.fees.referral_fee == Decimal("1")
    assert result.profit == Decimal("7")
