from __future__ import annotations

import argparse
import json
import sys
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path

from src.agents.scrape.pipeline import ScrapeAgent
from src.common.config_loader import ConfigError, load_settings
from src.common.logging import configure_logging


def _decimal(value: str) -> Decimal:
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError(f"Invalid decimal value '{value}': {exc}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="amazon-sedori",
        description="Amazon arbitrage assistant leveraging SP-API and Keepa data.",
    )
    parser.add_argument("--asin", help="Amazon ASIN identifier.")
    parser.add_argument("--barcode", help="Product barcode (JAN/EAN).")
    parser.add_argument("--purchase-cost", type=_decimal, required=True, help="Acquisition cost in JPY.")
    parser.add_argument(
        "--inbound-shipping",
        "--shipping-fees",
        dest="inbound_shipping",
        type=_decimal,
        help="Override inbound shipping cost per unit.",
    )
    parser.add_argument("--packaging", type=_decimal, help="Override packaging material cost per unit.")
    parser.add_argument("--storage-fee", type=_decimal, help="Override monthly storage fee per unit.")
    parser.add_argument("--taxes", type=_decimal, help="Additional taxes per unit.")
    parser.add_argument("--target-price", type=_decimal, help="Override selling price.")
    parser.add_argument("--fx-spread-bp", type=int, help="Override FX spread in basis points.")
    parser.add_argument("--return-rate", type=_decimal, help="Override expected return rate (e.g. 0.05).")
    parser.add_argument("--env", help="Environment override (matches config/env/<env>.yml).")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    parser.add_argument("--notify-slack", action="store_true", help="Send Slack notification when thresholds pass.")
    parser.add_argument("--notify-line", action="store_true", help="Send LINE notification when thresholds pass.")
    parser.add_argument("--dry-run", action="store_true", help="Resolve data without triggering notifications or side-effects.")
    parser.add_argument("--decision-path", type=Path, help="Optional path to write the decision JSON artifact.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def parse_arguments() -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args()
    if not (args.asin or args.barcode):
        parser.error("Either --asin or --barcode must be provided.")
    return args


def main() -> None:
    args = parse_arguments()

    try:
        settings = load_settings(env=args.env)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        sys.exit(1)

    configure_logging(
        level=args.log_level,
        json_logs=settings.observability.json_logs,
        secrets=settings.secrets_for_redaction(),
    )

    agent = ScrapeAgent(settings=settings)
    request_id = str(uuid.uuid4())
    decision_path = args.decision_path.expanduser() if args.decision_path else None

    result = agent.run(
        asin=args.asin,
        barcode=args.barcode,
        purchase_cost=args.purchase_cost,
        inbound_shipping_override=args.inbound_shipping,
        packaging_override=args.packaging,
        storage_fee_override=args.storage_fee,
        taxes=args.taxes,
        target_price=args.target_price,
        fx_spread_override=args.fx_spread_bp,
        return_rate_override=args.return_rate,
        notify_slack=args.notify_slack,
        notify_line=args.notify_line,
        dry_run=args.dry_run,
        decision_path=decision_path,
        request_id=request_id,
    )

    if args.pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
