import os
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from tvDatafeed import TvDatafeed, Interval
import requests

# === CONFIG ===
INTERVAL = Interval.in_30_minute  # 30m timeframe
LOOKBACK = 96  # last 96 candles
SLEEP_TIME = 180  # 3 minutes between scans

# === LOGGER SETUP ===
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

# === TELEGRAM SETUP ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_message(message: str):
    """Send formatted message to Telegram."""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
        response = requests.post(url, data=data)
        if response.status_code == 200:
            log.info("📩 Telegram message sent")
        else:
            log.error(f"⚠️ Telegram error {response.text}")
    except Exception as e:
        log.error(f"⚠️ Telegram send failed: {e}")

# === LOAD SYMBOLS ===
try:
    symbols_df = pd.read_csv("ALL_WATCHLIST_SYMBOLS.csv")
    symbols = symbols_df["SYMBOL"].tolist()
    log.info(f"✅ Loaded {len(symbols)} symbols from CSV")
except Exception as e:
    log.error(f"❌ Error loading CSV: {e}")
    symbols = []

# === TVDATAFEED LOGIN ===
try:
    tv = TvDatafeed()
except Exception as e:
    log.warning(f"⚠️ Cookie login failed, using nologin fallback: {e}")
    tv = TvDatafeed()

# === CORE SIGNAL SCAN FUNCTION ===
def run_scan():
    for symbol_exchange in symbols:
        try:
            # Example symbol format: NSE:SBIN or NASDAQ:AAPL
            exchange, symbol_clean = symbol_exchange.split(":")
            data = tv.get_hist(symbol_clean, exchange, interval=INTERVAL, n=LOOKBACK)
            if data is None or data.empty:
                continue

            # --- Calculate signals (replace with your PineScript logic) ---
            close = data["close"]
            high = data["high"]
            low = data["low"]

            # Example custom strategy logic:
            atr = (high - low).rolling(window=14).mean().iloc[-1]
            latest_close = close.iloc[-1]
            prev_close = close.iloc[-2]

            # Define dummy buy/sell signal conditions
            if latest_close > prev_close * 1.002:  # +0.2% move up
                latest_type = "BUY"
            elif latest_close < prev_close * 0.998:  # -0.2% move down
                latest_type = "SELL"
            else:
                continue

            # ---- Send Telegram message ----
            tp, sl = (latest_close + atr * 3.0, latest_close - atr * 1.5) if latest_type == "BUY" else (
                latest_close - atr * 3.0, latest_close + atr * 1.5)

            ist_time = (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%d-%b %H:%M")

            msg = (f"**PERFECT 5 SIGNAL - {latest_type}**\n"
                   f"Symbol: `{symbol_clean}`\n"
                   f"Exchange: `{exchange}`\n"
                   f"Price: `{latest_close:.2f}`\n"
                   f"TP: `{tp:.2f}`\n"
                   f"SL: `{sl:.2f}`\n"
                   f"Time: `{ist_time} IST`")

            log.info(f"{latest_type} → {exchange}:{symbol_clean}")
            send_telegram_message(msg)

        except Exception as e:
            log.error(f"Error scanning {symbol_exchange}: {e}")

# === MAIN LOOP ===
if __name__ == "__main__":
    log.info(f"🚀 Scanner started (3-min continuous mode, last {LOOKBACK} candles).")

    while True:
        try:
            start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log.info(f"🕒 Running scan at {start_time}")
            run_scan()
            log.info("✅ Scan complete. Sleeping 3 minutes...\n")
        except Exception as e:
            log.error(f"❌ Fatal scan error: {e}")
        time.sleep(SLEEP_TIME)
