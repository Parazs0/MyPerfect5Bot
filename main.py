# main.py — Final stable version (cookie-injection, robust parsing, 30m UTC sync)
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
# Env
# -----------------------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TV_USERNAME = os.getenv("TV_USERNAME")
TV_PASSWORD = os.getenv("TV_PASSWORD")
TV_COOKIES_BASE64 = os.getenv("TV_COOKIES_BASE64")
CSV_PATH = os.getenv("CSV_PATH", "ALL_WATCHLIST_SYMBOLS.csv")
PORT = int(os.getenv("PORT", 8000))
PAUSE_BETWEEN_SYMBOLS = float(os.getenv("PAUSE_BETWEEN_SYMBOLS", "3"))
N_BARS = int(os.getenv("N_BARS", "300"))

# Fallback exchange order to try if primary doesn't return data
FALLBACK_EXCHANGES = ["NSE","BSE","MCX","TVC","INDEX","OANDA","SKILLING","CAPITALCOM","VANTAGE","IG","SPREADEX","SZSE"]

# -----------------------------
# Telegram helper
# -----------------------------
def send_telegram_message(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        log.debug("Telegram credentials missing — skipping send.")
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        resp = requests.post(url, data={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"})
        if resp.status_code != 200:
            log.error("Telegram API error %s: %s", resp.status_code, resp.text)
    except Exception:
        log.exception("Telegram send error")

# -----------------------------
# CSV load
# -----------------------------
if not os.path.exists(CSV_PATH):
    log.error("CSV not found at %s", CSV_PATH)
    raise SystemExit(1)

symbols_df = pd.read_csv(CSV_PATH)
if "SYMBOL" not in symbols_df.columns:
    log.error("CSV must contain SYMBOL column")
    raise SystemExit(1)

symbols = symbols_df["SYMBOL"].dropna().astype(str).tolist()
log.info("Loaded %d symbols from CSV", len(symbols))

# -----------------------------
# Symbol parsing & normalization
# -----------------------------
def normalize_token(tok: str):
    if not tok:
        return None
    t = re.sub(r'[^A-Za-z0-9]', '', tok).upper()
    return t

def parse_symbol(raw: str):
    s = str(raw).strip()
    if not s:
        return ("NSE", "")
    if ":" in s:
        ex, sym = s.split(":", 1)
        return (normalize_token(ex) or "NSE", sym.strip())
    up = s.upper()
    if up.endswith(".NS") or up.endswith("-NS"):
        return ("NSE", s[:-3])
    if up.endswith(".BO") or up.endswith("-BO"):
        return ("BSE", s[:-3])
    # try to detect known exchange in string
    for ex in ["NSE","BSE","MCX","TVC","INDEX","OANDA","SKILLING","CAPITALCOM","VANTAGE","IG","SPREADEX","SZSE","NSEIX"]:
        if ex in up:
            # strip the ex part if present like "NSE:SYM" already handled; for safety return found ex
            return (ex, s)
    return ("NSE", s)

# -----------------------------
# Supertrend (Pine-like)
# -----------------------------
def compute_supertrend(df, period=10, multiplier=3.0):
    atr = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=period).average_true_range()
    hl2 = (df['high'] + df['low']) / 2.0
    upper = hl2 + multiplier * atr
    lower = hl2 - multiplier * atr
    final_upper = upper.copy()
    final_lower = lower.copy()
    sup = pd.Series(index=df.index, dtype='float64')
    dirn = pd.Series(index=df.index, dtype='int64')
    for i in range(len(df)):
        if i == 0:
            final_upper.iat[i] = upper.iat[i]
            final_lower.iat[i] = lower.iat[i]
            sup.iat[i] = final_upper.iat[i]
            dirn.iat[i] = 1
            continue
        fu_prev = final_upper.iat[i-1]
        fl_prev = final_lower.iat[i-1]
        close_prev = df['close'].iat[i-1]
        fu = upper.iat[i] if (upper.iat[i] < fu_prev or close_prev > fu_prev) else fu_prev
        fl = lower.iat[i] if (lower.iat[i] > fl_prev or close_prev < fl_prev) else fl_prev
        final_upper.iat[i] = fu
        final_lower.iat[i] = fl
        if df['close'].iat[i] > fu_prev:
            dirn.iat[i] = 1
            sup.iat[i] = fl
        elif df['close'].iat[i] < fl_prev:
            dirn.iat[i] = -1
            sup.iat[i] = fu
        else:
            dirn.iat[i] = dirn.iat[i-1]
            sup.iat[i] = sup.iat[i-1]
    return sup, dirn

# -----------------------------
# Cookies decode helper (writes temp json file)
# -----------------------------
cookies_path = None
if TV_COOKIES_BASE64:
    try:
        decoded = base64.b64decode(TV_COOKIES_BASE64)
        tf = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        tf.write(decoded); tf.close()
        cookies_path = tf.name
        log.info("✅ TradingView cookies decoded to: %s", cookies_path)
    except Exception as e:
        log.warning("Failed to decode TV_COOKIES_BASE64: %s", e)
        cookies_path = None

# -----------------------------
# tvDatafeed loader with stable cookie injection
# -----------------------------
def load_tv_session():
    try:
        cookies_data = None
        if cookies_path and os.path.exists(cookies_path):
            try:
                with open(cookies_path, "r", encoding="utf-8") as f:
                    cookies_data = json.load(f)
            except Exception as e:
                log.warning("Could not read cookies json: %s", e)
                cookies_data = None

        # create client
        try:
            tvc = TvDatafeed()
        except Exception as e:
            log.warning("TvDatafeed init without cookies failed: %s", e)
            tvc = TvDatafeed()

        # if cookies, build requests.Session and attach safely
        if cookies_data:
            try:
                sess = requests.Session()
                if isinstance(cookies_data, list):
                    for c in cookies_data:
                        if isinstance(c, dict) and "name" in c and "value" in c:
                            domain = c.get("domain", ".tradingview.com")
                            if not domain.startswith("."):
                                domain = "." + domain
                            try:
                                sess.cookies.set(c["name"], c["value"], domain=domain)
                            except Exception:
                                sess.cookies.set(c["name"], c["value"])
                elif isinstance(cookies_data, dict):
                    for k, v in cookies_data.items():
                        try:
                            sess.cookies.set(k, v)
                        except Exception:
                            pass

                # try to attach to tvc.session — but avoid causing serialization errors elsewhere
                try:
                    setattr(tvc, "session", sess)
                    log.info("Attached requests.Session to tvc.session")
                except Exception:
                    try:
                        tvc._injected_session = sess
                        log.info("Attached requests.Session to tvc._injected_session (fallback)")
                    except Exception:
                        log.warning("Could not attach session to tvc object (ignored)")

                # quick test
                try:
                    test = tvc.get_hist(symbol="RELIANCE", exchange="NSE", interval=Interval.in_daily, n_bars=1)
                    if test is not None and not test.empty:
                        log.info("✅ Cookies-based session working.")
                        return tvc
                    else:
                        log.warning("Cookies session test returned no data (may be expired).")
                except Exception as e:
                    log.warning("Cookies-based quick test failed: %s", e)

            except Exception as e:
                log.warning("Failed to build requests.Session from cookies: %s", e)

        # Try username/password login
        if TV_USERNAME and TV_PASSWORD:
            try:
                tvc2 = TvDatafeed(username=TV_USERNAME, password=TV_PASSWORD)
                log.info("✅ Logged in via username/password.")
                return tvc2
            except Exception as e:
                log.warning("Username/password login failed: %s", e)

        # Final fallback: no-login
        log.warning("Using nologin tvDatafeed (limited access).")
        return tvc

    except Exception as e:
        log.exception("tvDatafeed initialization error: %s", e)
        try:
            return TvDatafeed()
        except Exception as e2:
            log.critical("Final tvDatafeed fallback failed: %s", e2)
            raise

tv = load_tv_session()

# -----------------------------
# Helper to try get_hist with exchange fallbacks
# -----------------------------
def try_get_hist(tvc, symbol, exchange, interval, n_bars):
    # build try list
    tried = []
    if exchange:
        tried.append(exchange)
    tried.extend([e for e in FALLBACK_EXCHANGES if e not in tried])
    tried.append(None)  # try without exchange
    last_exc = None
    for ex in tried:
        try:
            if ex:
                df = tvc.get_hist(symbol=symbol, exchange=ex, interval=interval, n_bars=n_bars)
            else:
                # attempt without exchange parameter
                try:
                    df = tvc.get_hist(symbol=symbol, interval=interval, n_bars=n_bars)
                except TypeError:
                    df = None
            if df is not None and not df.empty:
                return df, ex
        except Exception as e:
            last_exc = e
            log.debug("get_hist failed for %s @ %s: %s", symbol, ex, e)
            continue
    # nothing worked
    if last_exc:
        raise last_exc
    return None, None

# -----------------------------
# Signal calc (Pine-alike)
# -----------------------------
def calculate_signals(raw_symbol: str):
    global tv
    try:
        ex_token, sym_token = parse_symbol(raw_symbol)
        ex_token = (ex_token or "NSE")
        sym_token = str(sym_token).strip()
        if not sym_token:
            return

        try:
            df, used_ex = try_get_hist(tv, sym_token, ex_token, Interval.in_30_minute, N_BARS)
        except Exception as e:
            log.warning("Failed fetching bars for %s (tried %s): %s", raw_symbol, ex_token, e)
            return

        if df is None or df.empty:
            log.debug("No data for %s", raw_symbol)
            return

        df = df.reset_index().rename(columns={df.columns[0]: "datetime"})
        for c in ["close","high","low"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df.dropna(inplace=True)
        if len(df) < 60:
            log.debug("Insufficient bars for %s (len=%d)", raw_symbol, len(df))
            return

        ema20 = EMAIndicator(df["close"], window=20).ema_indicator()
        ema50 = EMAIndicator(df["close"], window=50).ema_indicator()
        ema20_now, ema20_prev = float(ema20.iat[-1]), float(ema20.iat[-2])
        # supertrend (period=10, factor=3)
        super_series, _ = compute_supertrend(df, period=10, multiplier=3.0)
        super_now, super_prev = float(super_series.iat[-1]), float(super_series.iat[-2])
        atr_series = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=14).average_true_range()
        atr_now = float(atr_series.iat[-1])
        close_now = float(df["close"].iat[-1])
        close_prev = float(df["close"].iat[-2])

        buy = (close_now > ema20_now) and (close_now > super_now) and not ((close_prev > ema20_prev) and (close_prev > super_prev))
        sell = (close_now < ema20_now) and (close_now < super_now) and not ((close_prev < ema20_prev) and (close_prev < super_prev))

        display = f"{used_ex or ex_token}:{sym_token}"
        if buy:
            tp = close_now + atr_now * 3.0
            sl = close_now - atr_now * 1.5
            msg = (f"**PERFECT 5 SIGNAL - BUY**\nSymbol: `{display}`\nPrice: `{close_now:.2f}`\nTP: `{tp:.2f}`\nSL: `{sl:.2f}`\nTF: `30m`")
            log.info("BUY → %s", display)
            send_telegram_message(msg)
        if sell:
            tp = close_now - atr_now * 3.0
            sl = close_now + atr_now * 1.5
            msg = (f"**PERFECT 5 SIGNAL - SELL**\nSymbol: `{display}`\nPrice: `{close_now:.2f}`\nTP: `{tp:.2f}`\nSL: `{sl:.2f}`\nTF: `30m`")
            log.info("SELL → %s", display)
            send_telegram_message(msg)

    except Exception as e:
        log.exception("Error processing %s: %s", raw_symbol, e)
        s = str(e).lower()
        if "session" in s or "expired" in s or "401" in s:
            log.info("Session problem detected — reloading tv session.")
            try:
                tv = load_tv_session()
            except Exception:
                log.exception("Reload failed")

# -----------------------------
# 30-minute UTC synchronization
# -----------------------------
def wait_until_next_30m_close():
    now = datetime.now(timezone.utc)
    if now.minute < 30:
        next_close = now.replace(minute=30, second=0, microsecond=0)
    else:
        next_close = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    wait_s = (next_close - now).total_seconds()
    if wait_s > 0:
        log.info("Waiting %.0f seconds until next 30m candle close (UTC %s -> %s)", wait_s, now.strftime("%H:%M:%S"), next_close.strftime("%H:%M:%S"))
        time.sleep(wait_s + 1)

# -----------------------------
# Scanner loop
# -----------------------------
def scan_loop():
    log.info("Scanner started (30m UTC synced).")
    while True:
        wait_until_next_30m_close()
        start = datetime.now()
        log.info("Starting scan at %s local", start.strftime("%Y-%m-%d %H:%M:%S"))
        for sym in symbols:
            try:
                calculate_signals(sym)
            except Exception:
                log.exception("Exception scanning %s", sym)
            time.sleep(PAUSE_BETWEEN_SYMBOLS)
        log.info("Scan complete (duration %.1f s)", (datetime.now() - start).total_seconds())
        # loop repeats and waits for next 30m close

# -----------------------------
# Flask health endpoints
# -----------------------------
app = Flask(__name__)
@app.route("/")
def home():
    return jsonify({"status":"MyPerfect5Bot","time":datetime.utcnow().isoformat()}), 200
@app.route("/health")
def health():
    return "OK", 200
@app.route("/ping")
def ping():
    return "pong", 200

def start_flask():
    log.info("Starting Flask on port %d", PORT)
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# -----------------------------
# Launch
# -----------------------------
if __name__ == "__main__":
    t = threading.Thread(target=scan_loop, daemon=True)
    t.start()
    start_flask()
