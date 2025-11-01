# main.py
import os
import base64
import json
import tempfile
import logging
import threading
import time
from datetime import datetime

from flask import Flask
import pandas as pd

# ta / tvdatafeed imports
from tvDatafeed import TvDatafeed, Interval
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("perfect5")

# -------------------------
# Env / Paths
# -------------------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TV_USERNAME = os.getenv("TV_USERNAME")
TV_PASSWORD = os.getenv("TV_PASSWORD")
TV_COOKIES_BASE64 = os.getenv("TV_COOKIES_BASE64")
CSV_PATH = os.getenv("CSV_PATH", "ALL_WATCHLIST_SYMBOLS.csv")
PORT = int(os.getenv("PORT", 8000))

# -------------------------
# Helper: Telegram send
# -------------------------
import requests
def send_telegram_message(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram credentials missing, skipping send.")
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        resp = requests.post(url, data={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"})
        if resp.status_code != 200:
            log.error(f"Telegram API error {resp.status_code}: {resp.text}")
    except Exception as e:
        log.exception("Telegram send error")

# -------------------------
# Decode cookies (if provided) -> write temp file
# -------------------------
cookies_path = None
if TV_COOKIES_BASE64:
    try:
        decoded = base64.b64decode(TV_COOKIES_BASE64)
        tf = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        tf.write(decoded)
        tf.close()
        cookies_path = tf.name
        log.info(f"Decoded cookies saved to {cookies_path}")
    except Exception as e:
        log.exception("Failed to decode TV_COOKIES_BASE64")

# -------------------------
# Load CSV symbols
# -------------------------
if not os.path.exists(CSV_PATH):
    log.error(f"CSV not found at {CSV_PATH}. Exiting.")
    raise SystemExit(1)

symbols_df = pd.read_csv(CSV_PATH)
if "SYMBOL" not in symbols_df.columns:
    log.error("CSV must contain a SYMBOL column. Exiting.")
    raise SystemExit(1)

symbols = symbols_df["SYMBOL"].dropna().astype(str).unique().tolist()
log.info(f"Loaded {len(symbols)} symbols from CSV")

# -------------------------
# Exchange detection
# -------------------------
def detect_exchange(symbol: str) -> str:
    # Priority: CSV EXCHANGE column
    if "EXCHANGE" in symbols_df.columns:
        row = symbols_df.loc[symbols_df["SYMBOL"] == symbol]
        if not row.empty:
            val = row["EXCHANGE"].iat[0]
            if isinstance(val, str) and val.strip():
                return val.strip().upper()

    s = symbol.upper()
    if s.endswith(".NS") or s.endswith(":NSE") or s.endswith("-NS"):
        return "NSE"
    if s.endswith(".BO") or s.endswith(":BSE") or s.endswith("-BO"):
        return "BSE"
    if any(x in s for x in ["CRUDE","OIL","BRENT","WTI","GOLD","SILVER","XAU","XAG"]):
        return "MCX"
    if any(x in s for x in ["BTC","ETH","BNB","COIN","CRYPTO"]):
        return "INDEX"
    if any(x in s for x in ["USD","EUR","GBP","JPY","FOREX","OANDA"]):
        return "OANDA"
    known = {"BSE","INDEX","CAPITALCOM","TVC","IG","MCX","OANDA","NSE",
             "NSEIX","SKILLING","SPREADEX","SZSE","VANTAGE"}
    for ex in known:
        if ex in s:
            return ex
    return "NSE"

# -------------------------
# Compute Supertrend
# -------------------------
def compute_supertrend(df, period=10, multiplier=3.0):
    atr = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=period).average_true_range()
    hl2 = (df['high'] + df['low']) / 2.0
    upperband = hl2 + (multiplier * atr)
    lowerband = hl2 - (multiplier * atr)
    final_upper = upperband.copy()
    final_lower = lowerband.copy()
    supertrend = pd.Series(index=df.index, dtype='float64')
    direction = pd.Series(index=df.index, dtype='int64')
    for i in range(len(df)):
        if i == 0:
            final_upper.iat[i] = upperband.iat[i]
            final_lower.iat[i] = lowerband.iat[i]
            supertrend.iat[i] = final_upper.iat[i]
            direction.iat[i] = 1
            continue
        fu_prev = final_upper.iat[i-1]
        fl_prev = final_lower.iat[i-1]
        close_prev = df['close'].iat[i-1]
        fu = upperband.iat[i] if (upperband.iat[i] < fu_prev or close_prev > fu_prev) else fu_prev
        fl = lowerband.iat[i] if (lowerband.iat[i] > fl_prev or close_prev < fl_prev) else fl_prev
        final_upper.iat[i] = fu
        final_lower.iat[i] = fl
        if df['close'].iat[i] > fu_prev:
            direction.iat[i] = 1
            supertrend.iat[i] = fl
        elif df['close'].iat[i] < fl_prev:
            direction.iat[i] = -1
            supertrend.iat[i] = fu
        else:
            direction.iat[i] = direction.iat[i-1]
            supertrend.iat[i] = supertrend.iat[i-1]
    return supertrend, direction

# -------------------------
# tvDatafeed session loader (cookies -> username/password -> nologin)
# -------------------------
def load_tv_session():
    # attempt cookies injection
    try:
        if cookies_path:
            try:
                with open(cookies_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # convert to dict name->value if list format
                if isinstance(data, list):
                    cookies_dict = {c['name']: c['value'] for c in data if 'name' in c and 'value' in c}
                elif isinstance(data, dict):
                    # maybe already dict name->value or full cookie
                    # if values are dicts convert to name->value
                    if all(isinstance(v, dict) for v in data.values()):
                        cookies_dict = {k: v.get('value','') for k,v in data.items()}
                    else:
                        cookies_dict = data
                else:
                    cookies_dict = {}
                # create nologin client and inject cookies
                tvc = TvDatafeed(nologin=True)
                try:
                    session = getattr(tvc, "session", None)
                    if session:
                        for k,v in cookies_dict.items():
                            session.cookies.set(k, v)
                    log.info("Injected cookies into tvDatafeed session.")
                except Exception as e:
                    log.warning("Could not inject cookies into session: %s", e)
                # quick test
                try:
                    test = tvc.get_hist(symbol="RELIANCE", exchange="NSE", interval=Interval.in_daily, n_bars=1)
                    if test is not None:
                        log.info("Cookies-based session working.")
                        return tvc
                    else:
                        log.warning("Cookies-based session didn't return data.")
                except Exception as e:
                    log.warning("Cookies session test failed: %s", e)
            except Exception as e:
                log.exception("Failed to load cookies file")
        # fallback to username/password
        if TV_USERNAME and TV_PASSWORD:
            tvc = TvDatafeed(username=TV_USERNAME, password=TV_PASSWORD)
            log.info("Logged in via username/password.")
            return tvc
        # final fallback
        tvc = TvDatafeed(nologin=True)
        log.warning("Using nologin tvDatafeed (limited).")
        return tvc
    except Exception as e:
        log.exception("Failed to initialize tvDatafeed")
        return TvDatafeed(nologin=True)

tv = load_tv_session()

# -------------------------
# Signal calculation per symbol
# -------------------------
def calculate_signals(symbol: str):
    global tv
    try:
        exchange = detect_exchange(symbol)
        df = tv.get_hist(symbol=symbol, exchange=exchange, interval=Interval.in_30_minute, n_bars=200)
        if df is None or df.empty:
            log.debug(f"No data for {symbol} ({exchange})")
            return
        df = df.reset_index().rename(columns={df.columns[0]: "datetime"})
        for col in ["close","high","low"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.dropna(inplace=True)
        if len(df) < 50:
            log.debug(f"Insufficient bars for {symbol}")
            return

        ema20 = EMAIndicator(df["close"], window=20).ema_indicator()
        supertrend_series, _ = compute_supertrend(df, period=10, multiplier=3.0)

        close_now, close_prev = df["close"].iat[-1], df["close"].iat[-2]
        ema_now, ema_prev = ema20.iat[-1], ema20.iat[-2]
        super_now, super_prev = supertrend_series.iat[-1], supertrend_series.iat[-2]

        buy = (close_now > ema_now) and (close_now > super_now) and not ((close_prev > ema_prev) and (close_prev > super_prev))
        sell = (close_now < ema_now) and (close_now < super_now) and not ((close_prev < ema_prev) and (close_prev < super_prev))

        atr = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=14).average_true_range().iat[-1]

        if buy:
            tp, sl = close_now + atr * 2.0, close_now - atr * 1.0
            msg = (f"**PERFECT 5 SIGNAL - BUY**\n"
                   f"**Symbol:** `{symbol}`\n**Exchange:** `{exchange}`\n"
                   f"**Price:** `{close_now:.2f}`\n**TP:** `{tp:.2f}`\n**SL:** `{sl:.2f}`\n**TF:** `30m`")
            log.info(f"BUY → {symbol}")
            send_telegram_message(msg)

        if sell:
            tp, sl = close_now - atr * 2.0, close_now + atr * 1.0
            msg = (f"**PERFECT 5 SIGNAL - SELL**\n"
                   f"**Symbol:** `{symbol}`\n**Exchange:** `{exchange}`\n"
                   f"**Price:** `{close_now:.2f}`\n**TP:** `{tp:.2f}`\n**SL:** `{sl:.2f}`\n**TF:** `30m`")
            log.info(f"SELL → {symbol}")
            send_telegram_message(msg)

    except Exception as e:
        s = str(e).lower()
        log.exception(f"Error processing {symbol}: {e}")
        if "session" in s or "401" in s or "expired" in s:
            log.warning("Session problem detected — reloading tv session.")
            tv = load_tv_session()

# -------------------------
# Scanning loop (background)
# -------------------------
SCAN_INTERVAL_SECONDS = 120  # full-scan interval
def scan_loop():
    log.info("Starting scan loop.")
    while True:
        start = datetime.now()
        log.info(f"--- Scan started at {start.strftime('%Y-%m-%d %H:%M:%S')} ---")
        for symbol in symbols:
            try:
                calculate_signals(symbol)
            except Exception as e:
                log.exception(f"Exception during symbol {symbol}: {e}")
            time.sleep(0.4)  # gentle
        end = datetime.now()
        log.info(f"--- Scan finished at {end.strftime('%Y-%m-%d %H:%M:%S')} (duration {(end-start).total_seconds():.1f}s) ---")
        time.sleep(SCAN_INTERVAL_SECONDS)

# -------------------------
# Flask health server
# -------------------------
app = Flask(__name__)

@app.route("/")
def index():
    return "MyPerfect5Bot running"

@app.route("/health")
def health():
    return "OK"

# -------------------------
# Start background thread + Flask
# -------------------------
if __name__ == "__main__":
    # start scanner in background
    t = threading.Thread(target=scan_loop, daemon=True)
    t.start()
    log.info("Scanner thread started, launching Flask server.")
    app.run(host="0.0.0.0", port=PORT)
