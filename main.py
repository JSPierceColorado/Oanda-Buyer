import json
import logging
import os
import sys
from typing import Optional

import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------------------------------------------------------------------
# Config & constants
# ---------------------------------------------------------------------

ICON_MULTIPLIERS = {
    "💎": 2.0,
    "💥": 1.0,
    "🚀": 2.0,
    "✨": 1.0,
    "📊": 2.0,
}

# Column indices (0-based) for the Oanda-Screener sheet
COL_PAIR = 0        # A – Pair
COL_PRICE = 1       # B – Price
COL_PCT_DOWN = 2    # C – % down from ATH
COL_LONG_MA = 10    # K – Long MA
COL_ICON = 18       # S – Icon
COL_SENTIMENT = 20  # U – Sentiment

SENTIMENT_BUY = "🟢"

# Defaults for Google Sheets
DEFAULT_SHEET_NAME = "Active-Investing"
DEFAULT_WORKSHEET_NAME = "Oanda-Screener"

# ---------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------


def get_bracket_pct(pct_down: float) -> Optional[float]:
    """
    Bracket order size by % down from ATH.

    0–6% down → 5% of buying power
    7–12%     → 10% of buying power
    13–18%    → 15% of buying power
    19%+      → 20% of buying power

    Anything negative is treated as invalid and returns None.
    """
    if pct_down < 0:
        return None

    if 0 <= pct_down <= 6:
        return 0.05
    if 6 < pct_down <= 12:
        return 0.10
    if 12 < pct_down <= 18:
        return 0.15
    if pct_down > 18:
        return 0.20

    # In case of weird boundaries
    return None


def parse_float(value: str) -> Optional[float]:
    """Parse a float from a string that might have a '%' sign or be blank."""
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        return None
    if value.endswith("%"):
        value = value[:-1].strip()
    try:
        return float(value)
    except ValueError:
        return None


# ---------------------------------------------------------------------
# Oanda API client
# ---------------------------------------------------------------------


class OandaClient:
    def __init__(self):
        self.api_key = os.getenv("OANDA_API_KEY")
        self.account_id = os.getenv("OANDA_ACCOUNT_ID")
        env = os.getenv("OANDA_ENV", "practice").lower()

        if not self.api_key or not self.account_id:
            logging.error("OANDA_API_KEY and OANDA_ACCOUNT_ID must be set.")
            sys.exit(1)

        if env == "live":
            self.base_url = "https://api-fxtrade.oanda.com"
        else:
            self.base_url = "https://api-fxpractice.oanda.com"

    def _request(self, method: str, path: str, **kwargs):
        url = f"{self.base_url}{path}"
        headers = kwargs.pop("headers", {})
        headers.setdefault("Authorization", f"Bearer {self.api_key}")
        headers.setdefault("Content-Type", "application/json")

        logging.debug("Oanda request %s %s", method, url)
        resp = requests.request(method, url, headers=headers, **kwargs)

        if not resp.ok:
            logging.error("Oanda API error %s %s: %s",
                          resp.status_code, resp.reason, resp.text)
            resp.raise_for_status()

        return resp.json()

    def get_account_summary(self) -> dict:
        return self._request("GET", f"/v3/accounts/{self.account_id}/summary")

    def get_open_positions(self) -> list:
        data = self._request("GET", f"/v3/accounts/{self.account_id}/openPositions")
        return data.get("positions", [])

    def create_market_buy(self, instrument: str, units: int) -> dict:
        if units <= 0:
            raise ValueError("Units must be positive for a buy order.")

        body = {
            "order": {
                "instrument": instrument,
                "units": str(units),  # positive = buy, negative = sell
                "timeInForce": "FOK",
                "type": "MARKET",
                "positionFill": "DEFAULT",
            }
        }

        logging.info("Submitting market buy: instrument=%s units=%s",
                     instrument, units)
        return self._request(
            "POST",
            f"/v3/accounts/{self.account_id}/orders",
            json=body,
        )


# ---------------------------------------------------------------------
# Google Sheets helpers
# ---------------------------------------------------------------------


def get_gspread_client() -> gspread.Client:
    creds_json = os.getenv("GOOGLE_CREDS_JSON")
    if not creds_json:
        logging.error("GOOGLE_CREDS_JSON env var must be set with service account JSON.")
        sys.exit(1)

    try:
        info = json.loads(creds_json)
    except json.JSONDecodeError:
        logging.error("GOOGLE_CREDS_JSON is not valid JSON.")
        sys.exit(1)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(credentials)


def fetch_screener_rows():
    sheet_name = os.getenv("GOOGLE_SHEET_NAME", DEFAULT_SHEET_NAME)
    worksheet_name = os.getenv("GOOGLE_WORKSHEET_NAME", DEFAULT_WORKSHEET_NAME)

    client = get_gspread_client()
    logging.info("Opening Google Sheet: %s / %s", sheet_name, worksheet_name)
    sheet = client.open(sheet_name)
    ws = sheet.worksheet(worksheet_name)

    # get_all_values returns a list of rows, each row is a list of cell strings
    rows = ws.get_all_values()
    if not rows:
        logging.warning("No data found in sheet.")
        return []

    # Assume first row is header, skip it
    return rows[1:]


# ---------------------------------------------------------------------
# Core trading logic
# ---------------------------------------------------------------------


def get_buying_power_from_summary(summary: dict) -> float:
    """
    Try to extract a reasonable "buying power" value from Oanda account summary.
    Prefer marginAvailable; fall back to NAV or balance if needed.
    """
    account = summary.get("account", {})
    for key in ("marginAvailable", "NAV", "balance"):
        if key in account:
            try:
                bp = float(account[key])
                logging.info("Using %s as buying power: %s", key, bp)
                return bp
            except (ValueError, TypeError):
                continue

    logging.error("Could not determine buying power from account summary.")
    sys.exit(1)


def any_open_positions(positions: list) -> bool:
    """
    Return True if there is any open position in the account.
    This enforces the rule:
      - it should make only one active buy,
      - send no more buys if an asset is active in the account.
    """
    if not positions:
        return False

    # A position is "active" if any long or short units != 0
    for pos in positions:
        long_units = pos.get("long", {}).get("units", "0")
        short_units = pos.get("short", {}).get("units", "0")
        try:
            if float(long_units) != 0 or float(short_units) != 0:
                return True
        except ValueError:
            continue
    return False


def choose_order_from_rows(rows, buying_power: float):
    """
    Scan rows and compute notional for valid candidates.

    Rules:

    - Only consider rows where:
        * icon in ICON_MULTIPLIERS
        * sentiment == 🟢
    - % down from ATH (C) controls base allocation bracket.
    - Icon multiplier scales inside the bracket.
    - Long MA vs price factor = long_ma / price
    - Anything with pct_down < 0 is skipped.
    - If notional < 1.0, skip.
    - Returns first valid (pair, price, notional) encountered.
    """
    for idx, row in enumerate(rows, start=2):  # start=2 because of header row
        # Guard against short rows
        if len(row) <= max(COL_PAIR, COL_PRICE, COL_PCT_DOWN,
                           COL_LONG_MA, COL_ICON, COL_SENTIMENT):
            logging.debug("Row %s too short, skipping: %s", idx, row)
            continue

        pair = row[COL_PAIR].strip()
        price_str = row[COL_PRICE]
        pct_down_str = row[COL_PCT_DOWN]
        long_ma_str = row[COL_LONG_MA]
        icon = row[COL_ICON].strip()
        sentiment = row[COL_SENTIMENT].strip()

        # Skip empty or header-ish rows
        if not pair or pair.lower() == "pair":
            continue

        if sentiment != SENTIMENT_BUY:
            logging.debug("Row %s %s: sentiment not 🟢 (%s), skipping.",
                          idx, pair, sentiment)
            continue

        if icon not in ICON_MULTIPLIERS:
            logging.debug("Row %s %s: icon %s not in ICON_MULTIPLIERS, skipping.",
                          idx, pair, icon)
            continue

        price = parse_float(price_str)
        pct_down = parse_float(pct_down_str)
        long_ma = parse_float(long_ma_str)

        if price is None or price <= 0:
            logging.debug("Row %s %s: invalid price '%s', skipping.",
                          idx, pair, price_str)
            continue

        if long_ma is None or long_ma <= 0:
            logging.debug("Row %s %s: invalid long MA '%s', skipping.",
                          idx, pair, long_ma_str)
            continue

        if pct_down is None:
            logging.debug("Row %s %s: invalid pct_down '%s', skipping.",
                          idx, pair, pct_down_str)
            continue

        bracket_pct = get_bracket_pct(pct_down)
        if bracket_pct is None:
            logging.debug("Row %s %s: pct_down %s outside valid brackets, skipping.",
                          idx, pair, pct_down)
            continue

        icon_mult = ICON_MULTIPLIERS[icon]
        ma_price_factor = long_ma / price

        sentiment_mult = 1.0  # we only reach here on 🟢

        base_alloc = buying_power * bracket_pct
        notional = base_alloc * icon_mult * ma_price_factor * sentiment_mult

        logging.info(
            "Row %s %s: price=%.5f pct_down=%.2f bracket_pct=%.3f "
            "icon=%s icon_mult=%.2f long_ma=%.5f ma_price_factor=%.3f "
            "base_alloc=%.2f notional_raw=%.2f",
            idx, pair, price, pct_down, bracket_pct,
            icon, icon_mult, long_ma, ma_price_factor,
            base_alloc, notional
        )

        if notional < 1.0:
            logging.info("Row %s %s: notional < 1.0 (%.2f), skipping.", idx, pair, notional)
            continue

        # Don't exceed current buying power
        if notional > buying_power:
            logging.info(
                "Row %s %s: notional %.2f exceeds buying power %.2f, clamping.",
                idx, pair, notional, buying_power
            )
            notional = buying_power

        notional = round(notional, 2)

        if notional < 1.0:
            logging.info("Row %s %s: notional fell below 1.0 after clamping, skipping.", idx, pair)
            continue

        logging.info(
            "Selected candidate: row %s %s, price=%.5f, notional=%.2f",
            idx, pair, price, notional
        )

        return pair, price, notional

    logging.info("No valid candidates found in sheet.")
    return None


def main():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logging.info("Starting Oanda buying bot run (single pass).")

    # -----------------------------------------------------------------
    # Oanda: summary & open positions
    # -----------------------------------------------------------------
    oanda = OandaClient()

    summary = oanda.get_account_summary()
    positions = oanda.get_open_positions()

    if any_open_positions(positions):
        logging.info(
            "Account already has active positions; per rules, "
            "no new buys will be placed this run."
        )
        return

    buying_power = get_buying_power_from_summary(summary)
    if buying_power <= 0:
        logging.info("Buying power is <= 0 (%.2f); no trades will be placed.", buying_power)
        return

    # -----------------------------------------------------------------
    # Google Sheets: read screener rows
    # -----------------------------------------------------------------
    rows = fetch_screener_rows()
    if not rows:
        logging.info("No screener rows to process.")
        return

    candidate = choose_order_from_rows(rows, buying_power)
    if not candidate:
        logging.info("No candidate met all criteria; ending run.")
        return

    pair, price, notional = candidate

    # -----------------------------------------------------------------
    # Convert notional to units and place order
    # -----------------------------------------------------------------
    # NOTE:
    #   This assumes the sheet price is in account currency per 1 unit of the
    #   base instrument, similar to how Oanda quotes. If your sheet format
    #   differs, adjust this conversion.
    units = int(notional / price)

    if units <= 0:
        logging.info(
            "Calculated units <= 0 for pair %s (price=%.5f, notional=%.2f), "
            "no order will be placed.",
            pair, price, notional,
        )
        return

    try:
        logging.info(
            "Placing market buy on %s: notional=%.2f, price=%.5f, units=%s",
            pair, notional, price, units
        )
        resp = oanda.create_market_buy(pair, units)
        logging.info("Order placed successfully: %s", json.dumps(resp, indent=2))
    except Exception as e:
        logging.exception("Failed to place order: %s", e)
        # Let process exit non-zero so you can see it in Railway logs
        sys.exit(1)

    logging.info("Run complete. Exiting.")


if __name__ == "__main__":
    main()
