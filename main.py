import os, base64, json, tempfile, logging, threading, time, re
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify
import pandas as pd
import requests
from tvDatafeed import TvDatafeed, Interval
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("Perfect5Bot")

# -----------------------------
# Env Variables
# -----------------------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
CSV_PATH = os.getenv("CSV_PATH", "ALL_WATCHLIST_SYMBOLS.csv")
PORT = int(os.getenv("PORT", 8000))
PAUSE_BETWEEN_SYMBOLS = float(os.getenv("PAUSE_BETWEEN_SYMBOLS", "3"))
SLEEP_BETWEEN_SCANS = float(os.getenv("SLEEP_BETWEEN_SCANS", "180"))  # 3 minutes
N_BARS = int(os.getenv("N_BARS", "96"))

# -----------------------------
# Telegram helper
# -----------------------------
def send_telegram_message(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("⚠️ Telegram credentials missing.")
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        resp = requests.post(url, data={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"})
        if resp.status_code != 200:
            log.error(f"Telegram error {resp.text}")
    except Exception as e:
        log.error(f"⚠️ Telegram send failed: {e}")

# -----------------------------
# Load symbols
# -----------------------------
if not os.path.exists(CSV_PATH):
    log.error("❌ CSV file not found: %s", CSV_PATH)
    raise SystemExit(1)

symbols_df = pd.read_csv(CSV_PATH)
if "SYMBOL" not in symbols_df.columns:
    log.error("❌ CSV must have a 'SYMBOL' column.")
    raise SystemExit(1)

symbols = symbols_df["SYMBOL"].dropna().tolist()
log.info(f"✅ Loaded {len(symbols)} symbols from CSV")

# -----------------------------
# tvDatafeed init
# -----------------------------
try:
    tv = TvDatafeed()
    log.info("✅ tvDatafeed initialized (nologin mode).")
except Exception as e:
    log.warning(f"⚠️ tvDatafeed init failed: {e}")
    tv = TvDatafeed()

# -----------------------------
# Strategy Signal Calculation
# -----------------------------
def calculate_signals(symbol_exchange: str):
    try:
        exchange, symbol = symbol_exchange.split(":")
        df = tv.get_hist(symbol, exchange, interval=Interval.in_30_minute, n_bars=N_BARS)
        if df is None or df.empty:
            log.warning(f"⚠️ No data for {symbol_exchange}")
            return

        df = df.dropna()
        close = df["close"]
        high = df["high"]
        low = df["low"]

        atr = AverageTrueRange(high=high, low=low, close=close, window=14).average_true_range().iloc[-1]
        close_now, close_prev = close.iloc[-1], close.iloc[-2]

        # Dummy buy/sell condition
        if close_now > close_prev * 1.002:
            signal = "BUY"
        elif close_now < close_prev * 0.998:
            signal = "SELL"
        else:
            log.info(f"➡️ No new signal for {symbol_exchange}")
            return

        tp = close_now + atr * 3.0 if signal == "BUY" else close_now - atr * 3.0
        sl = close_now - atr * 1.5 if signal == "BUY" else close_now + atr * 1.5

        ist_time = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime("%d-%b %H:%M")
        msg = (f"**PERFECT 5 SIGNAL - {signal}**\n"
               f"Symbol: `{symbol}`\nExchange: `{exchange}`\n"
               f"Price: `{close_now:.2f}`\nTP: `{tp:.2f}`\nSL: `{sl:.2f}`\nTime: `{ist_time} IST`")

        log.info(f"{signal} → {exchange}:{symbol}")
        send_telegram_message(msg)
    except Exception as e:
        log.error(f"❌ Error scanning {symbol_exchange}: {e}")

# -----------------------------
# Main scan loop
# -----------------------------
def scan_loop():
    log.info("🚀 Continuous scanner started (3s per symbol, 3min between rounds, last 96 candles).")
    while True:
        start_time = datetime.now()
        log.info(f"🕒 Starting scan at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        for idx, sym in enumerate(symbols, start=1):
            calculate_signals(sym)
            log.info(f"⏳ Sleeping 3s... ({idx}/{len(symbols)})")
            time.sleep(PAUSE_BETWEEN_SYMBOLS)
        log.info("✅ Full scan complete. Sleeping 3 minutes before next round...")
        time.sleep(SLEEP_BETWEEN_SCANS)

# -----------------------------
# Flask server
# -----------------------------
app = Flask(__name__)
@app.route("/")
def home():
    return jsonify({"status": "MyPerfect5Bot", "time": datetime.now(timezone.utc).isoformat()})
@app.route("/health")
def health():
    return "OK"
@app.route("/ping")
def ping():
    return "pong"

def start_flask():
    log.info(f"🌐 Flask running on port {PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# -----------------------------
# Launch
# -----------------------------
if __name__ == "__main__":
    threading.Thread(target=scan_loop, daemon=True).start()
    start_flask()
