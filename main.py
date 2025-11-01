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
SLEEP_TIME = 180  # 3 minutes between full scans
PER_SYMBOL_DELAY = 3  # 3 seconds between symbols

# === LOGGER SETUP ===
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

# === TELEGRAM SETUP ===
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_message(message: str):
    """Send formatted message to Telegram."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("⚠️ Telegram credentials missing.")
        return
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
    symbols = symbols_df["SYMBOL"].dropna().tolist()
    log.info(f"✅ Loaded {len(symbols)} symbols from CSV")
except Exception as e:
    log.error(f"❌ Error loading CSV: {e}")
    symbols = []

# === TVDATAFEED LOGIN ===
try:
    tv = TvDatafeed()
    log.info("✅ tvDatafeed initialized (nologin mode).")
except Exception as e:
    log.error(f"⚠️ tvDatafeed initialization failed: {e}")
    tv = None

# === MAIN SCAN FUNCTION ===
def scan_symbol(symbol_exchange):
    """Scan individual symbol for signals."""
    try:
        if ":" not in symbol_exchange:
            return
        exchange, symbol_clean = symbol_exchange.split(":")

        df = tv.get_hist(
            symbol=symbol_clean,
            exchange=exchange,
            interval=INTERVAL,
            n_bars=LOOKBACK
        )

        if df is None or df.empty:
            log.warning(f"⚠️ No data for {exchange}:{symbol_clean}")
            return

        close = df["close"]
        high = df["high"]
        low = df["low"]

        atr = (high - low).rolling(window=14).mean().iloc[-1]
        latest_close = close.iloc[-1]
        prev_close = close.iloc[-2]

        latest_type = None
        if latest_close > prev_close * 1.002:
            latest_type = "BUY"
        elif latest_close < prev_close * 0.998:
            latest_type = "SELL"

        if latest_type:
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
        else:
            log.info(f"➡️ No new signal for {exchange}:{symbol_clean}")

    except Exception as e:
        log.error(f"❌ Error scanning {symbol_exchange}: {e}")

# === LOOP FUNCTION ===
def scan_loop():
    log.info("🚀 Scanner started (3-min continuous mode, 3s per symbol, last 96 candles).")
    while True:
        start_time = datetime.now()
        log.info(f"🕒 Starting scan at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        for i, sym in enumerate(symbols, 1):
            scan_symbol(sym)
            log.info(f"⏳ Sleeping {PER_SYMBOL_DELAY}s... ({i}/{len(symbols)})")
            time.sleep(PER_SYMBOL_DELAY)

        log.info(f"✅ Full scan complete. Sleeping {SLEEP_TIME//60} minutes before next round...\n")
        time.sleep(SLEEP_TIME)

# === MAIN ENTRY ===
if __name__ == "__main__":
    if tv:
        scan_loop()
    else:
        log.error("🚫 tvDatafeed not initialized. Exiting...")
