from __future__ import annotations

import logging
from typing import Dict, Iterable

from gspread import Spreadsheet, Worksheet, service_account
from gspread.exceptions import APIError, WorksheetNotFound

from src.common.models import ProfitAnalysis, ProductListing


logger = logging.getLogger(__name__)


class GoogleSheetsError(RuntimeError):
    """Raised when Google Sheets interaction fails."""


class GoogleSheetsService:
    def __init__(self, config: Dict[str, str]) -> None:
        try:
            self._credentials_file = config["credentials_file"]
            self._spreadsheet_id = config["spreadsheet_id"]
            self._worksheet_name = config["worksheet_name"]
        except KeyError as exc:
            raise GoogleSheetsError(f"Missing Google Sheets configuration: {exc}") from exc

        try:
            self._client = service_account(filename=self._credentials_file)
            self._spreadsheet: Spreadsheet = self._client.open_by_key(self._spreadsheet_id)
        except (OSError, APIError) as exc:
            raise GoogleSheetsError(f"Failed to initialise Google Sheets client: {exc}") from exc

    def append_listing(self, listing: ProductListing, profit: ProfitAnalysis) -> None:
        try:
            worksheet = self._get_worksheet()
            worksheet.append_row(self._build_row(listing, profit), value_input_option="USER_ENTERED")
        except APIError as exc:
            logger.error("Appending row to Google Sheets failed: %s", exc)
            raise GoogleSheetsError(f"Google Sheets append failed: {exc}") from exc

    def _get_worksheet(self) -> Worksheet:
        try:
            return self._spreadsheet.worksheet(self._worksheet_name)
        except WorksheetNotFound:
            logger.info("Worksheet %s not found; creating new one", self._worksheet_name)
            worksheet = self._spreadsheet.add_worksheet(title=self._worksheet_name, rows=100, cols=20)
            worksheet.append_row(
                [
                    "ASIN",
                    "Title",
                    "Price",
                    "Currency",
                    "Description",
                    "Image URLs",
                    "Profit",
                    "ROI",
                    "Margin",
                ],
                value_input_option="USER_ENTERED",
            )
            return worksheet

    def _build_row(self, listing: ProductListing, profit: ProfitAnalysis) -> Iterable:
        return [
            listing.asin,
            listing.title,
            float(listing.price),
            listing.currency,
            listing.description,
            ", ".join(listing.image_urls),
            float(profit.profit),
            float(profit.roi),
            float(profit.margin),
        ]
