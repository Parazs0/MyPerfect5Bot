# main.py — Perfect5Bot: Market Hours Filter Added (Full Exchanges)
import os, base64, json, logging, threading, time
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify
import pandas as pd
import requests
from tvDatafeed import TvDatafeed, Interval
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange
import pickle

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
SLEEP_BETWEEN_SCANS = float(os.getenv("SLEEP_BETWEEN_SCANS", "300"))
N_BARS = int(os.getenv("N_BARS", "96"))

FALLBACK_EXCHANGES = ["NSE","BSE","MCX","TVC","INDEX","OANDA","SKILLING","CAPITALCOM","VANTAGE","IG","SPREADEX","SZSE","NSEIX"]

# Global: Prevent duplicate signals within 25 minutes
last_signal_sent = {}

# -----------------------------
# Market Timings (IST, Mon-Fri unless specified) - Updated for All Exchanges
# -----------------------------
MARKET_TIMINGS = {
    "NSE": {"start": "09:15", "end": "15:30", "days": [0,1,2,3,4]},  # Mon-Fri
    "BSE": {"start": "09:00", "end": "15:30", "days": [0,1,2,3,4]},  # Mon-Fri
    "MCX": {"start": "09:00", "end": "23:55", "days": [0,1,2,3,4]},  # Mon-Sat
    "IG": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # 24/5 Forex/CFDs
    "CAPITALCOM": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # 24/5 CFDs
    "SPREADEX": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # Approx Mon-Fri (12:30PM-2:30AM IST)
    "TVC": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # 24/7 Composites (Crypto/Indices vary)
    "INDEX": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4,5,6]},  # Default NSE/BSE
    "OANDA": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # 24/5 Forex
    "NSEIX": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # NSE Indices (same as NSE)
    "SKILLING": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # 24/5 CFD/Forex
    "SZSE": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # Mon-Fri (CST to IST: 9:30-11:30 + 13:00-14:57 CST)
    "VANTAGE": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # 24/5 Forex/CFD
    "DEFAULT": {"start": "09:00", "end": "17:00", "days": [0,1,2,3,4]}  # Fallback
}

def is_market_open(exchange: str) -> bool:
    """Check if current IST time is within market hours for the exchange."""
    now_utc = datetime.now(timezone.utc)
    ist_offset = timedelta(hours=5, minutes=30)
    now_ist = now_utc + ist_offset
    current_time = now_ist.time()
    current_day = now_ist.weekday()  # 0=Mon, 6=Sun

    timings = MARKET_TIMINGS.get(exchange.upper(), MARKET_TIMINGS["DEFAULT"])
    if current_day not in timings["days"]:
        log.debug("Market closed: Weekend/Holiday for %s (day %d)", exchange, current_day)
        return False

    start_time = datetime.strptime(timings["start"], "%H:%M").time()
    end_time = datetime.strptime(timings["end"], "%H:%M").time()

    # Handle overnight/24h sessions
    if start_time < end_time:
        return start_time <= current_time <= end_time
    else:  # Overnight (end next day)
        return current_time >= start_time or current_time <= end_time

# -----------------------------
# Telegram helper
# -----------------------------
def send_telegram_message(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram credentials missing — message not sent.")
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        resp = requests.post(url, data={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"})
        if resp.status_code != 200:
            log.error("Telegram error %s: %s", resp.status_code, resp.text)
    except Exception as e:
        log.exception("Telegram send failed: %s", e)

# -----------------------------
# Load symbols
# -----------------------------
if not os.path.exists(CSV_PATH):
    log.error("CSV file not found: %s", CSV_PATH)
    raise SystemExit(1)

symbols_df = pd.read_csv(CSV_PATH)
if "SYMBOL" not in symbols_df.columns:
    log.error("CSV must have a 'SYMBOL' column.")
    raise SystemExit(1)

symbols = symbols_df["SYMBOL"].dropna().astype(str).tolist()
log.info("Loaded %d symbols from CSV", len(symbols))

# -----------------------------
# tvDatafeed init with cookies.b64.txt (Render Ready)
# -----------------------------
COOKIES_B64_FILE = "cookies.b64.txt"

def init_tv():
    global tv

    # 1. Try loading from cookies.b64.txt
    if os.path.exists(COOKIES_B64_FILE):
        try:
            with open(COOKIES_B64_FILE, 'r') as f:
                b64_data = f.read().strip()
            cookies_data = base64.b64decode(b64_data)
            cookies = pickle.loads(cookies_data)
            tv = TvDatafeed(cookies=cookies)
            log.info("tvDatafeed loaded from cookies.b64.txt (authenticated)")
            return
        except Exception as e:
            log.warning("Failed to load cookies.b64.txt: %s", e)

    # 2. Fallback: Try login with env vars
    username = os.getenv("TV_USERNAME")
    password = os.getenv("TV_PASSWORD")
    if username and password:
        try:
            tv = TvDatafeed(username=username, password=password)
            log.info("Logged in successfully")
            
            # Safe cookies save: Check if cookies attribute exists
            if hasattr(tv, 'cookies') and tv.cookies:
                try:
                    cookies_data = pickle.dumps(tv.cookies)
                    b64_data = base64.b64encode(cookies_data).decode('utf-8')
                    with open(COOKIES_B64_FILE, 'w') as f:
                        f.write(b64_data)
                    log.info("Logged in & cookies saved to cookies.b64.txt")
                except Exception as save_e:
                    log.warning("Could not save cookies.b64.txt: %s", save_e)
            else:
                log.warning("Login succeeded but no cookies attribute found")
            return
        except Exception as e:
            log.warning("Login failed: %s", e)

    # 3. Final fallback: nologin
    log.warning("No cookies/login → using nologin mode (may timeout)")
    tv = TvDatafeed()

# === CALL INIT (ONLY ONCE) ===
init_tv()

# === SYMBOL PARSER ===
def parse_symbol(raw: str):
    s = str(raw).strip()
    if not s: return ("NSE", "")
    if ":" in s:
        ex, sym = s.split(":", 1)
        return (ex.strip().upper(), sym.strip())
    up = s.upper()
    if up.endswith(".NS") or up.endswith("-NS"): return ("NSE", s[:-3])
    if up.endswith(".BO") or up.endswith("-BO"): return ("BSE", s[:-3])
    return ("NSE", s)

# === TRY GET HIST WITH FALLBACK ===
def try_get_hist(tvc, symbol, exchange, interval, n_bars):
    tried = []
    if exchange: tried.append(exchange)
    tried.extend([e for e in FALLBACK_EXCHANGES if e not in tried])
    tried.append(None)

    last_exc = None
    for ex in tried:
        try:
            try:
                df = tvc.get_hist(symbol=symbol, exchange=ex, interval=interval, n_bars=n_bars)
            except TypeError:
                df = tvc.get_hist(symbol=symbol, exchange=ex, interval=interval, n=n_bars)
            if df is not None and not df.empty:
                return df, ex
        except Exception as e:
            last_exc = e
            log.debug("get_hist failed for %s @ %s: %s", symbol, ex or "None", e)
            continue
    if last_exc: raise last_exc
    return None, None

# -----------------------------
# Supertrend function
# -----------------------------
def compute_supertrend(df, period=10, multiplier=3.0):
    df_local = df.copy().reset_index(drop=True)
    hl2 = (df_local['high'] + df_local['low']) / 2.0
    atr = AverageTrueRange(high=df_local['high'], low=df_local['low'], close=df_local['close'], window=period).average_true_range()
    upper = hl2 + multiplier * atr
    lower = hl2 - multiplier * atr

    final_upper = upper.copy()
    final_lower = lower.copy()
    sup = pd.Series(index=df_local.index, dtype='float64')
    dirn = pd.Series(index=df_local.index, dtype='int64')

    for i in range(len(df_local)):
        if i == 0:
            final_upper.iat[i] = upper.iat[i]
            final_lower.iat[i] = lower.iat[i]
            sup.iat[i] = final_upper.iat[i]
            dirn.iat[i] = 1
            continue

        fu_prev = final_upper.iat[i-1]
        fl_prev = final_lower.iat[i-1]
        close_prev = df_local['close'].iat[i-1]

        fu = upper.iat[i] if (upper.iat[i] < fu_prev or close_prev > fu_prev) else fu_prev
        fl = lower.iat[i] if (lower.iat[i] > fl_prev or close_prev < fl_prev) else fl_prev
        final_upper.iat[i] = fu
        final_lower.iat[i] = fl

        if df_local['close'].iat[i] > fu_prev:
            dirn.iat[i] = 1
            sup.iat[i] = fl
        elif df_local['close'].iat[i] < fl_prev:
            dirn.iat[i] = -1
            sup.iat[i] = fu
        else:
            dirn.iat[i] = dirn.iat[i-1]
            sup.iat[i] = sup.iat[i-1]

    return sup, dirn

# -----------------------------
# Strategy: Find LATEST crossover in last N_BARS (with Market Hours Check)
# -----------------------------
def calculate_signals(raw_symbol: str):
    global tv
    try:
        ex_token, sym_token = parse_symbol(raw_symbol)
        ex_token = ex_token or "NSE"
        sym_token = str(sym_token).strip()
        if not sym_token:
            return

        # === MARKET HOURS CHECK ===
        if not is_market_open(ex_token):
            log.debug("Market closed for %s (%s) — skipping", raw_symbol, ex_token)
            return

        # === Download 5-min & 30-min data ===
        df_5, used_ex = try_get_hist(tv, sym_token, ex_token, Interval.in_5_minute, N_BARS * 6)
        df_30, _ = try_get_hist(tv, sym_token, ex_token, Interval.in_30_minute, N_BARS)

        if df_5 is None or df_5.empty or df_30 is None or df_30.empty:
            return

        # Normalize datetime
        df_5 = df_5.copy().reset_index()
        df_30 = df_30.copy().reset_index()
        df_5.rename(columns={df_5.columns[0]: "datetime"}, inplace=True)
        df_30.rename(columns={df_30.columns[0]: "datetime"}, inplace=True)
        df_5["datetime"] = pd.to_datetime(df_5["datetime"])
        df_30["datetime"] = pd.to_datetime(df_30["datetime"])

        # === Calculate 30-min indicators ===
        ema20_30 = EMAIndicator(df_30["close"], window=20).ema_indicator()
        super_30, _ = compute_supertrend(df_30, period=10, multiplier=3.0)
        atr_30 = AverageTrueRange(df_30["high"], df_30["low"], df_30["close"], window=14).average_true_range()

        df_30["ema20"] = ema20_30
        df_30["supertrend"] = super_30
        df_30["atr"] = atr_30

        # === Merge 30-min data into 5-min ===
        df_5 = pd.merge_asof(df_5.sort_values("datetime"),
                             df_30[["datetime", "ema20", "supertrend", "atr"]].sort_values("datetime"),
                             on="datetime")

        if len(df_5) < 10:
            return

        display = f"{used_ex or ex_token}:{sym_token}"
        key = f"{used_ex or ex_token}:{sym_token}"

        # === Latest 5-min candle ===
        close_now = df_5["close"].iloc[-1]
        close_prev = df_5["close"].iloc[-2]
        ema_now = df_5["ema20"].iloc[-1]
        ema_prev = df_5["ema20"].iloc[-2]
        st_now = df_5["supertrend"].iloc[-1]
        st_prev = df_5["supertrend"].iloc[-2]
        atr_now = df_5["atr"].iloc[-1]
        signal_time = df_5["datetime"].iloc[-1]

        # === Conditions (same as TradingView) ===
        buy = (close_now > ema_now) and (close_now > st_now) and not ((close_prev > ema_prev) and (close_prev > st_prev))
        sell = (close_now < ema_now) and (close_now < st_now) and not ((close_prev < ema_prev) and (close_prev < st_prev))

        if not (buy or sell):
            return

        # === Time & Duplicate Filter ===
        signal_time_ist = (signal_time + timedelta(hours=5, minutes=30)).strftime("%d-%b %H:%M")
        now = datetime.now()
        if key in last_signal_sent and (now - last_signal_sent[key]).total_seconds() < 25 * 60:
            log.debug("Duplicate skipped for %s", display)
            return
        last_signal_sent[key] = now

        # === Send Telegram Message ===
        if buy:
            tp = close_now + atr_now * 4.5
            sl = close_now - atr_now * 1.5
            msg = (
                f"**PERFECT 5 SIGNAL - BUY [5m Scan | 30m Logic]**\n"
                f"Symbol: `{display}`\n"
                f"Price: `{close_now:.2f}`\n"
                f"TP: `{tp:.2f}`\n"
                f"SL: `{sl:.2f}`\n"
                f"Time: `{signal_time_ist} IST`"
            )
            log.info("BUY → %s @ %s", display, signal_time_ist)
            send_telegram_message(msg)

        elif sell:
            tp = close_now - atr_now * 4.5
            sl = close_now + atr_now * 1.5
            msg = (
                f"**PERFECT 5 SIGNAL - SELL [5m Scan | 30m Logic]**\n"
                f"Symbol: `{display}`\n"
                f"Price: `{close_now:.2f}`\n"
                f"TP: `{tp:.2f}`\n"
                f"SL: `{sl:.2f}`\n"
                f"Time: `{signal_time_ist} IST`"
            )
            log.info("SELL → %s @ %s", display, signal_time_ist)
            send_telegram_message(msg)

    except Exception as e:
        log.exception("Error processing %s: %s", raw_symbol, e)

# -----------------------------
# Main scan loop
# -----------------------------
def scan_loop():
    log.info("Scanner started (%.1fs/symbol, %.1fs/round, last %d bars).",
             PAUSE_BETWEEN_SYMBOLS, SLEEP_BETWEEN_SCANS, N_BARS)
    while True:
        start_time = datetime.now()
        log.info("Starting scan at %s", start_time.strftime("%Y-%m-%d %H:%M:%S"))
        for idx, sym in enumerate(symbols, 1):
            try:
                calculate_signals(sym)
            except Exception:
                log.exception("Exception scanning %s", sym)
            log.info("Sleeping %.1fs... (%d/%d)", PAUSE_BETWEEN_SYMBOLS, idx, len(symbols))
            time.sleep(PAUSE_BETWEEN_SYMBOLS)
        log.info("Full scan complete. Sleeping %.1f seconds...", SLEEP_BETWEEN_SCANS)
        time.sleep(SLEEP_BETWEEN_SCANS)

# -----------------------------
# Flask server
# -----------------------------
app = Flask(__name__)
@app.route("/")
def home():
    return jsonify({"status": "Perfect5Bot", "time": datetime.now(timezone.utc).isoformat()})
@app.route("/health")
def health():
    return "OK"
@app.route("/ping")
def ping():
    return "pong"

def start_flask():
    log.info("Flask running on port %d", PORT)
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# -----------------------------
# Launch
# -----------------------------
if __name__ == "__main__":
    threading.Thread(target=scan_loop, daemon=True).start()
    start_flask()
