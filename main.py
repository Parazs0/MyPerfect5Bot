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
PAUSE_BETWEEN_SYMBOLS = float(os.getenv("PAUSE_BETWEEN_SYMBOLS", "5"))
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

# === SYMBOL PARSER ===
def parse_symbol(raw: str):
    s = str(raw).strip()
    if not s:
        return ("NSE", "")
    if ":" in s:
        ex, sym = s.split(":", 1)
        return (ex.strip().upper(), sym.strip().upper())
    up = s.upper()
    if up.endswith(".NS") or up.endswith("-NS"):
        return ("NSE", s[:-3])
    if up.endswith(".BO") or up.endswith("-BO"):
        return ("BSE", s[:-3])
    return ("NSE", s)

# -----------------------------
# Strategy Signal Calculation
# -----------------------------
def calculate_signals(raw_symbol: str):
    global tv
    try:
        ex_token, sym_token = parse_symbol(raw_symbol)
        ex_token = (ex_token or "NSE")
        sym_token = str(sym_token).strip()
        if not sym_token:
            return

        # --- Get data ---
        df, used_ex = try_get_hist(tv, sym_token, ex_token, Interval.in_30_minute, N_BARS)
        if df is None or df.empty:
            return

        df = df.reset_index().rename(columns={df.columns[0]: "datetime"})
        for c in ["close", "high", "low"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df.dropna(inplace=True)
        if len(df) < 60:
            return

        # --- Indicator calculations ---
        ema20 = EMAIndicator(df["close"], window=20).ema_indicator()
        ema50 = EMAIndicator(df["close"], window=50).ema_indicator()
        super_series, _ = compute_supertrend(df, period=10, multiplier=3.0)
        atr_series = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=14).average_true_range()

        display = f"{used_ex or ex_token}:{sym_token}"

        # --- Scan last 96 bars for signals ---
        for i in range(1, len(df)):
            close_now = df["close"].iat[i]
            close_prev = df["close"].iat[i-1]
            ema20_now, ema20_prev = ema20.iat[i], ema20.iat[i-1]
            super_now, super_prev = super_series.iat[i], super_series.iat[i-1]
            atr_now = atr_series.iat[i]
            signal_time = df["datetime"].iat[i]
            signal_time_ist = (signal_time + timedelta(hours=5, minutes=30)).strftime("%d-%b %H:%M")

            buy = (close_now > ema20_now) and (close_now > super_now) and not ((close_prev > ema20_prev) and (close_prev > super_prev))
            sell = (close_now < ema20_now) and (close_now < super_now) and not ((close_prev < ema20_prev) and (close_prev < super_prev))

            # --- BUY Signal ---
            if buy:
                tp = close_now + atr_now * 3.0
                sl = close_now - atr_now * 1.5
                msg = (
                    f"**PERFECT 5 SIGNAL - BUY**\n"
                    f"Symbol: `{display}`\n"
                    f"Price: `{close_now:.2f}`\n"
                    f"TP: `{tp:.2f}`\n"
                    f"SL: `{sl:.2f}`\n"
                    f"Time: `{signal_time_ist} IST`\n"
                    f"TF: `30m`"
                )
                log.info(f"📈 BUY → {display} @ {signal_time_ist}")
                send_telegram_message(msg)

            # --- SELL Signal ---
            if sell:
                tp = close_now - atr_now * 3.0
                sl = close_now + atr_now * 1.5
                msg = (
                    f"**PERFECT 5 SIGNAL - SELL**\n"
                    f"Symbol: `{display}`\n"
                    f"Price: `{close_now:.2f}`\n"
                    f"TP: `{tp:.2f}`\n"
                    f"SL: `{sl:.2f}`\n"
                    f"Time: `{signal_time_ist} IST`\n"
                    f"TF: `30m`"
                )
                log.info(f"📉 SELL → {display} @ {signal_time_ist}")
                send_telegram_message(msg)

    except Exception as e:
        log.exception(f"Error processing {raw_symbol}: {e}")

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
