# main.py — fixed stable version
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
PAUSE_BETWEEN_SYMBOLS = float(os.getenv("PAUSE_BETWEEN_SYMBOLS", "4"))  # seconds per symbol
SLEEP_BETWEEN_SCANS = float(os.getenv("SLEEP_BETWEEN_SCANS", "180"))  # seconds between full rounds
N_BARS = int(os.getenv("N_BARS", "96"))  # last N bars to scan

FALLBACK_EXCHANGES = ["NSE","BSE","MCX","TVC","INDEX","OANDA","SKILLING","CAPITALCOM","VANTAGE","IG","SPREADEX","SZSE","NSEIX"]

# -----------------------------
# Telegram helper
# -----------------------------
def send_telegram_message(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("⚠️ Telegram credentials missing — message not sent.")
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        resp = requests.post(url, data={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"})
        if resp.status_code != 200:
            log.error("Telegram error %s: %s", resp.status_code, resp.text)
    except Exception as e:
        log.exception("⚠️ Telegram send failed: %s", e)

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

symbols = symbols_df["SYMBOL"].dropna().astype(str).tolist()
log.info("✅ Loaded %d symbols from CSV", len(symbols))

# -----------------------------
# tvDatafeed init
# -----------------------------
try:
    tv = TvDatafeed()
    log.info("✅ tvDatafeed initialized (nologin mode).")
except Exception as e:
    log.warning("⚠️ tvDatafeed init failed: %s", e)
    tv = TvDatafeed()

# === SYMBOL PARSER ===
def parse_symbol(raw: str):
    s = str(raw).strip()
    if not s:
        return ("NSE", "")
    if ":" in s:
        ex, sym = s.split(":", 1)
        return (ex.strip().upper(), sym.strip())
    up = s.upper()
    if up.endswith(".NS") or up.endswith("-NS"):
        return ("NSE", s[:-3])
    if up.endswith(".BO") or up.endswith("-BO"):
        return ("BSE", s[:-3])
    # fallback: return NSE by default and raw symbol
    return ("NSE", s)

# === TRY GET HIST WITH FALLBACK EXCHANGES ===
def try_get_hist(tvc, symbol, exchange, interval, n_bars):
    """
    Try fetching data from multiple exchanges in fallback order
    until one returns valid bars.
    """
    tried = []
    if exchange:
        tried.append(exchange)
    tried.extend([e for e in FALLBACK_EXCHANGES if e not in tried])
    tried.append(None)  # final try without exchange

    last_exc = None
    for ex in tried:
        try:
            # handle different signatures: some tvDatafeed versions expect n_bars, some expect n
            try:
                if ex:
                    df = tvc.get_hist(symbol=symbol, exchange=ex, interval=interval, n_bars=n_bars)
                else:
                    df = tvc.get_hist(symbol=symbol, interval=interval, n_bars=n_bars)
            except TypeError:
                # fallback to older param name 'n'
                if ex:
                    df = tvc.get_hist(symbol=symbol, exchange=ex, interval=interval, n=n_bars)
                else:
                    df = tvc.get_hist(symbol=symbol, interval=interval, n=n_bars)
            if df is not None and not df.empty:
                return df, ex
        except Exception as e:
            last_exc = e
            log.debug("get_hist failed for %s @ %s: %s", symbol, ex, e)
            continue

    if last_exc:
        raise last_exc
    return None, None

# -----------------------------
# Supertrend function (Pine-like)
# -----------------------------
def compute_supertrend(df, period=10, multiplier=3.0):
    """
    Returns (supertrend_series, direction_series)
    direction: 1 for up, -1 for down
    """
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
        try:
            df, used_ex = try_get_hist(tv, sym_token, ex_token, Interval.in_30_minute, N_BARS)
        except Exception as e:
            log.warning("Failed fetching bars for %s (tried %s): %s", raw_symbol, ex_token, e)
            return

        if df is None or df.empty:
            return

        # normalize dataframe
        if 'datetime' not in df.columns:
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.copy()
                df['datetime'] = df.index
            else:
                df = df.reset_index().rename(columns={df.columns[0]: "datetime"})
        else:
            df = df.loc[:, ~df.columns.duplicated()].copy()

        df.dropna(subset=['datetime','close','high','low'], inplace=True)
        
        # 🩹 Fix ambiguous 'datetime' (if both index and column)
        if 'datetime' in df.index.names:
                df = df.reset_index()  # remove datetime from index
                if 'datetime' in df.columns.duplicated(keep=False):
                    df = df.loc[:, ~df.columns.duplicated()]  # remove duplicates again
                elif 'datetime' not in df.columns:
                    df = df.reset_index()  # in case datetime is only in index

                if df.columns.duplicated().any():
                    df = df.loc[:, ~df.columns.duplicated()]  # safety again
    
                df = df.sort_values(by='datetime').reset_index(drop=True)
                if len(df) < 10:
                    return

        # compute indicators
        ema20 = EMAIndicator(df["close"], window=20).ema_indicator()
        super_series, _ = compute_supertrend(df, period=10, multiplier=3.0)
        atr_series = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=14).average_true_range()
        display = f"{used_ex or ex_token}:{sym_token}"

        # --- Detect latest signal only ---
        signal_found = None

        for i in range(len(df) - 1, 0, -1):  # go backwards (latest first)
            try:
                close_now = float(df["close"].iat[i])
                close_prev = float(df["close"].iat[i-1])
                ema20_now = float(ema20.iat[i]) if not pd.isna(ema20.iat[i]) else None
                ema20_prev = float(ema20.iat[i-1]) if not pd.isna(ema20.iat[i-1]) else None
                super_now = float(super_series.iat[i]) if not pd.isna(super_series.iat[i]) else None
                super_prev = float(super_series.iat[i-1]) if not pd.isna(super_series.iat[i-1]) else None
                atr_now = float(atr_series.iat[i]) if not pd.isna(atr_series.iat[i]) else 0.0
                signal_time = df["datetime"].iat[i]

                if ema20_now is None or ema20_prev is None or super_now is None or super_prev is None:
                    continue

                buy = (close_now > ema20_now) and (close_now > super_now) and not ((close_prev > ema20_prev) and (close_prev > super_prev))
                sell = (close_now < ema20_now) and (close_now < super_now) and not ((close_prev < ema20_prev) and (close_prev < super_prev))

                if buy or sell:
                    signal_found = ("BUY" if buy else "SELL", signal_time, close_now, atr_now)
                    break  # 🟢 break immediately after finding latest signal

            except Exception as inner_e:
                log.debug("Error evaluating bar %d for %s: %s", i, display, inner_e)
                continue

        # --- Send latest signal ---
        if signal_found:
            sig, signal_time, price, atr_now = signal_found
            signal_time_ist = (
                (signal_time.tz_localize(None) if signal_time.tzinfo is None else signal_time.tz_convert(None))
                + timedelta(hours=5, minutes=30)
            ).strftime("%d-%b %H:%M")

            tp = price + atr_now * (3.0 if sig == "BUY" else -3.0)
            sl = price - atr_now * (1.5 if sig == "BUY" else -1.5)

            msg = (
                f"**PERFECT 5 SIGNAL - {sig}**\n"
                f"Symbol: `{display}`\n"
                f"Price: `{price:.2f}`\n"
                f"TP: `{tp:.2f}`\n"
                f"SL: `{sl:.2f}`\n"
                f"Time: `{signal_time_ist} IST`"
            )
            log.info("📊 %s → %s @ %s", sig, display, signal_time_ist)
            send_telegram_message(msg)

    except Exception as e:
        log.exception("Error processing %s: %s", raw_symbol, e)

# -----------------------------
# Main scan loop
# -----------------------------
def scan_loop():
    log.info("🚀 Continuous scanner started (%ss per symbol, %ss between rounds, last %d bars).",
             PAUSE_BETWEEN_SYMBOLS, SLEEP_BETWEEN_SCANS, N_BARS)
    while True:
        start_time = datetime.now()
        log.info("🕒 Starting scan at %s", start_time.strftime("%Y-%m-%d %H:%M:%S"))
        for idx, sym in enumerate(symbols, start=1):
            try:
                calculate_signals(sym)
            except Exception:
                log.exception("Exception scanning %s", sym)
            log.info("⏳ Sleeping %.1fs... (%d/%d)", PAUSE_BETWEEN_SYMBOLS, idx, len(symbols))
            time.sleep(PAUSE_BETWEEN_SYMBOLS)
        log.info("✅ Full scan complete. Sleeping %.1f seconds before next round...", SLEEP_BETWEEN_SCANS)
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
    log.info("🌐 Flask running on port %d", PORT)
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# -----------------------------
# Launch
# -----------------------------
if __name__ == "__main__":
    threading.Thread(target=scan_loop, daemon=True).start()
    start_flask()
