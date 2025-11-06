from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

MONEY_QUANTUM_DEFAULT = Decimal("0.01")
RATIO_QUANTUM = Decimal("0.0001")


@dataclass(frozen=True)
class ProfitInputs:
    selling_price: Decimal
    purchase_cost: Decimal
    referral_fee: Decimal
    fba_fee: Decimal
    storage_fee: Decimal
    inbound_shipping: Decimal
    packaging_materials: Decimal
    fx_spread_bp: int
    return_rate: Decimal
    other_costs: Decimal = Decimal("0")


def quantize_money(value: Decimal, quantum: Decimal = MONEY_QUANTUM_DEFAULT) -> Decimal:
    return value.quantize(quantum, rounding=ROUND_HALF_UP)


def quantize_ratio(value: Decimal) -> Decimal:
    return value.quantize(RATIO_QUANTUM, rounding=ROUND_HALF_UP)


def basis_points(amount: Decimal, bps: int) -> Decimal:
    return (amount * Decimal(bps)) / Decimal("10000")


def expected_return_cost(revenue: Decimal, return_rate: Decimal) -> Decimal:
    return revenue * return_rate
