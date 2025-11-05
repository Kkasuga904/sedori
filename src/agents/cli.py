from __future__ import annotations

import argparse
import json
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Dict

from src.agents.scrape.pipeline import ScrapeAgent


def _decimal(value: str) -> Decimal:
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError(f"Invalid decimal value '{value}': {exc}") from exc


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="amazon-sedori",
        description="Amazon arbitrage assistant leveraging SP-API and Keepa data.",
    )
    parser.add_argument("--asin", help="Amazon ASIN identifier.")
    parser.add_argument("--barcode", help="Product barcode (JAN/EAN).")
    parser.add_argument("--purchase-cost", type=_decimal, required=True, help="Acquisition cost in JPY.")
    parser.add_argument("--shipping-fees", type=_decimal, default=Decimal("0"), help="Additional shipping fees.")
    parser.add_argument("--taxes", type=_decimal, default=Decimal("0"), help="Applicable taxes.")
    parser.add_argument("--target-price", type=_decimal, help="Override selling price.")
    parser.add_argument("--env", help="Environment override (matches config/env/<env>.yml).")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(name)s [%(levelname)s] %(message)s",
    )

    if not (args.asin or args.barcode):
        raise SystemExit("Either --asin or --barcode must be provided.")

    agent = ScrapeAgent(env=args.env)
    result = agent.run(
        asin=args.asin,
        barcode=args.barcode,
        purchase_cost=args.purchase_cost,
        shipping_fees=args.shipping_fees,
        taxes=args.taxes,
        target_price=args.target_price,
    )

    if args.pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
