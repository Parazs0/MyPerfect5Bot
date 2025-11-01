import os, base64, json, tempfile, logging, threading, time
from datetime import datetime
from flask import Flask, jsonify
import pandas as pd
import requests
from tvDatafeed import TvDatafeed, Interval
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange

# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("Perfect5Bot")

# -----------------------------
# Environment variables
# -----------------------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TV_USERNAME = os.getenv("TV_USERNAME")
TV_PASSWORD = os.getenv("TV_PASSWORD")
TV_COOKIES_BASE64 = os.getenv("TV_COOKIES_BASE64")
CSV_PATH = os.getenv("CSV_PATH", "ALL_WATCHLIST_SYMBOLS.csv")
PORT = int(os.getenv("PORT", 8000))
SCAN_INTERVAL_SECONDS = 1800  # scan every 30 minutes

# -----------------------------
# Telegram sender
# -----------------------------
def send_telegram_message(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("⚠️ Telegram credentials missing — skipping message.")
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        data = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
        resp = requests.post(url, data=data)
        if resp.status_code != 200:
            log.error(f"Telegram API error {resp.status_code}: {resp.text}")
    except Exception as e:
        log.error(f"Telegram send failed: {e}")

# -----------------------------
# Decode Base64 cookies to temp file
# -----------------------------
cookies_path = None
if TV_COOKIES_BASE64:
    try:
        decoded = base64.b64decode(TV_COOKIES_BASE64)
        tf = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        tf.write(decoded)
        tf.close()
        cookies_path = tf.name
        log.info(f"✅ TradingView cookies decoded to: {cookies_path}")
    except Exception as e:
        log.error(f"Failed to decode cookies: {e}")

# -----------------------------
# Load CSV
# -----------------------------
if not os.path.exists(CSV_PATH):
    log.error(f"CSV not found: {CSV_PATH}")
    raise SystemExit(1)
symbols_df = pd.read_csv(CSV_PATH)
if "SYMBOL" not in symbols_df.columns:
    log.error("CSV must contain a 'SYMBOL' column.")
    raise SystemExit(1)
symbols = symbols_df["SYMBOL"].dropna().astype(str).unique().tolist()
log.info(f"✅ Loaded {len(symbols)} symbols from CSV")

# -----------------------------
# Exchange detection helper
# -----------------------------
def parse_symbol(symbol: str):
    """Splits exchange and symbol if present like NSE:RELIANCE"""
    if ":" in symbol:
        ex, sym = symbol.split(":", 1)
        return ex.strip().upper(), sym.strip().upper()
    ex = detect_exchange(symbol)
    return ex, symbol.strip().upper()

def detect_exchange(symbol: str) -> str:
    if "EXCHANGE" in symbols_df.columns:
        row = symbols_df.loc[symbols_df["SYMBOL"] == symbol]
        if not row.empty:
            val = str(row["EXCHANGE"].iat[0]).strip().upper()
            if val:
                return val
    s = symbol.upper()
    known = {"BSE", "INDEX", "CAPITALCOM", "TVC", "IG", "MCX", "OANDA", "NSE", "NSEIX",
             "SKILLING", "SPREADEX", "SZSE", "VANTAGE"}
    for ex in known:
        if ex in s:
            return ex
    if s.endswith(".NS"):
        return "NSE"
    if s.endswith(".BO"):
        return "BSE"
    return "NSE"

# -----------------------------
# Supertrend
# -----------------------------
def compute_supertrend(df, period=10, multiplier=3.0):
    atr = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=period).average_true_range()
    hl2 = (df['high'] + df['low']) / 2.0
    upperband, lowerband = hl2 + multiplier * atr, hl2 - multiplier * atr
    final_upper, final_lower = upperband.copy(), lowerband.copy()
    supertrend = pd.Series(index=df.index, dtype='float64')
    direction = pd.Series(index=df.index, dtype='int64')

    for i in range(len(df)):
        if i == 0:
            supertrend.iat[i], direction.iat[i] = final_upper.iat[i], 1
            continue
        fu_prev, fl_prev, close_prev = final_upper.iat[i-1], final_lower.iat[i-1], df['close'].iat[i-1]
        fu = upperband.iat[i] if (upperband.iat[i] < fu_prev or close_prev > fu_prev) else fu_prev
        fl = lowerband.iat[i] if (lowerband.iat[i] > fl_prev or close_prev < fl_prev) else fl_prev
        final_upper.iat[i], final_lower.iat[i] = fu, fl
        if df['close'].iat[i] > fu_prev:
            direction.iat[i], supertrend.iat[i] = 1, fl
        elif df['close'].iat[i] < fl_prev:
            direction.iat[i], supertrend.iat[i] = -1, fu
        else:
            direction.iat[i], supertrend.iat[i] = direction.iat[i-1], supertrend.iat[i-1]
    return supertrend, direction

# -----------------------------
# Load tvDatafeed session
# -----------------------------
def load_tv_session():
    try:
        if cookies_path:
            with open(cookies_path, "r", encoding="utf-8") as f:
                cookies_data = json.load(f)
            tvc = TvDatafeed()
            import requests
            session = requests.Session()
            for c in cookies_data:
                if isinstance(c, dict) and "name" in c and "value" in c:
                    session.cookies.set(c["name"], c["value"], domain=c.get("domain", ".tradingview.com"))
            tvc.session = session
            log.info("✅ Cookies successfully injected into tvDatafeed session.")
            test = tvc.get_hist("RELIANCE", "NSE", Interval.in_daily, 1)
            if test is not None and not test.empty:
                log.info("✅ Cookies login verified successfully.")
                return tvc

        if TV_USERNAME and TV_PASSWORD:
            tvc = TvDatafeed(username=TV_USERNAME, password=TV_PASSWORD)
            log.info("✅ TradingView login via username/password successful.")
            return tvc

        tvc = TvDatafeed()
        log.warning("⚠️ Using nologin mode (limited data).")
        return tvc

    except Exception as e:
        log.error(f"Failed to initialize tvDatafeed: {e}")
        return TvDatafeed()

tv = load_tv_session()

# -----------------------------
# Signal logic
# -----------------------------
def calculate_signals(symbol: str):
    global tv
    try:
        exchange, symbol_clean = parse_symbol(symbol)
        df = tv.get_hist(symbol=symbol_clean, exchange=exchange, interval=Interval.in_30_minute, n_bars=200)
        if df is None or df.empty:
            return
        df = df.reset_index().rename(columns={df.columns[0]: "datetime"})
        for c in ["close", "high", "low"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df.dropna(inplace=True)
        if len(df) < 50:
            return

        ema20 = EMAIndicator(df["close"], 20).ema_indicator()
        supertrend, _ = compute_supertrend(df)
        close_now, close_prev = df["close"].iat[-1], df["close"].iat[-2]
        ema_now, ema_prev = ema20.iat[-1], ema20.iat[-2]
        super_now, super_prev = supertrend.iat[-1], supertrend.iat[-2]
        atr = AverageTrueRange(df['high'], df['low'], df['close'], 14).average_true_range().iat[-1]

        buy = (close_now > ema_now) and (close_now > super_now) and not ((close_prev > ema_prev) and (close_prev > super_prev))
        sell = (close_now < ema_now) and (close_now < super_now) and not ((close_prev < ema_prev) and (close_prev < super_prev))

        if buy:
            tp, sl = close_now + atr * 2, close_now - atr
            msg = (f"**PERFECT 5 SIGNAL - BUY**\n"
                   f"Symbol: `{symbol_clean}`\nExchange: `{exchange}`\n"
                   f"Price: `{close_now:.2f}`\nTP: `{tp:.2f}`\nSL: `{sl:.2f}`\nTF: `30m`")
            log.info(f"BUY → {exchange}:{symbol_clean}")
            send_telegram_message(msg)

        elif sell:
            tp, sl = close_now - atr * 2, close_now + atr
            msg = (f"**PERFECT 5 SIGNAL - SELL**\n"
                   f"Symbol: `{symbol_clean}`\nExchange: `{exchange}`\n"
                   f"Price: `{close_now:.2f}`\nTP: `{tp:.2f}`\nSL: `{sl:.2f}`\nTF: `30m`")
            log.info(f"SELL → {exchange}:{symbol_clean}")
            send_telegram_message(msg)

    except Exception as e:
        log.warning(f"⚠️ Error in {symbol}: {e}")
        if "session" in str(e).lower() or "expired" in str(e).lower():
            log.info("🔄 Reloading TradingView session...")
            tv = load_tv_session()

# -----------------------------
# Background scanner
# -----------------------------
def scan_loop():
    log.info("🚀 Scan loop started.")
    while True:
        start = datetime.now()
        log.info(f"🔍 Starting scan at {start.strftime('%H:%M:%S')}")
        for sym in symbols:
            calculate_signals(sym)
            time.sleep(3)
        log.info(f"✅ Scan completed ({len(symbols)} symbols). Waiting {SCAN_INTERVAL_SECONDS}s...")
        time.sleep(SCAN_INTERVAL_SECONDS)

# -----------------------------
# Flask + Keepalive
# -----------------------------
app = Flask(__name__)

@app.route("/")
def home():
    return jsonify({
        "status": "✅ MyPerfect5Bot is live on Render!",
        "scanner": "running",
        "uptime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

@app.route("/health")
def health():
    return "OK", 200

@app.route("/ping")
def ping():
    return "pong", 200

def start_flask():
    log.info(f"🌐 Starting Flask server on port {PORT} ...")
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

def start_bot():
    try:
        scan_loop()
    except Exception as e:
        log.error(f"Scanner crashed: {e}")
        threading.Timer(60, start_bot).start()

if __name__ == "__main__":
    threading.Thread(target=start_bot, daemon=True).start()
    start_flask()
