# =========================================================
# PERFECT5 SIGNAL BOT (24x7 Cookie + Multi-Exchange Version)
# =========================================================
import sys
sys.path.append('./tvDatafeed')
import os
import time
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from tvDatafeed import TvDatafeed, Interval
from ta.trend import EMAIndicator
import requests
import threading
import http.server
import socketserver
import shutil
import json

# ===========================
# ENVIRONMENT SETUP
# ===========================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TV_USERNAME = os.getenv("TV_USERNAME")
TV_PASSWORD = os.getenv("TV_PASSWORD")

COOKIES_PATH = r"C:\Users\Gandhi\Downloads\tradingview-telegram-bot\tradingview_cookies.json"

# ===========================
# TradingView Login (via cookies or fallback)
# ===========================
import base64

COOKIES_PATH = "tradingview_cookies.json"

try:
    b64 = os.getenv("TV_COOKIES_BASE64")
    if b64:
        # Decode Base64 → JSON
        decoded = base64.b64decode(b64).decode("utf-8")
        with open(COOKIES_PATH, "w", encoding="utf-8") as f:
            f.write(decoded)
        print("✅ TradingView cookies loaded from environment variable.")

        # Login using cookies
        tv = TvDatafeed(
            username=None,
            password=None,
            chromedriver_path=None,
            auto_login=False,
            tradingview_cookie_file=COOKIES_PATH
        )
        print("✅ TradingView login successful (via cookies).")

    else:
        print("⚠️ No cookies found in environment. Trying username/password...")
        if not (TV_USERNAME and TV_PASSWORD):
            raise Exception("TV_USERNAME or TV_PASSWORD missing.")
        tv = TvDatafeed(username=TV_USERNAME, password=TV_PASSWORD)
        print("✅ TradingView login successful (via username/password).")

except Exception as e:
    print(f"⚠️ TradingView login error: {e}")
    try:
        tv = TvDatafeed(nologin=True)
        print("⚠️ Proceeding with nologin mode (limited access).")
    except Exception as ex:
        print(f"❌ Unable to initialize tvDatafeed client: {ex}")
        raise

# ===========================
# LOAD CSV SYMBOLS
# ===========================
CSV_PATH = r"ALL_WATCHLIST_SYMBOLS.csv"
if not os.path.exists(CSV_PATH):
    raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

symbols_df = pd.read_csv(CSV_PATH)
if "SYMBOL" not in symbols_df.columns:
    raise KeyError("CSV must contain a 'SYMBOL' column")

symbols = symbols_df["SYMBOL"].dropna().astype(str).unique().tolist()
print(f"📊 Loaded {len(symbols)} symbols from CSV")

# ===========================
# TELEGRAM MESSAGE FUNCTION
# ===========================
def send_telegram_message(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("⚠️ Telegram config missing — cannot send message")
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        resp = requests.post(url, data={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"})
        if resp.status_code != 200:
            print(f"Telegram API returned {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"Telegram error: {e}")

# ===========================
# EXCHANGE DETECTION (UPDATED)
# ===========================
def detect_exchange(symbol: str) -> str:
    """Detect exchange name based on CSV or symbol pattern."""
    if "EXCHANGE" in symbols_df.columns:
        try:
            row = symbols_df.loc[symbols_df["SYMBOL"] == symbol]
            if not row.empty:
                val = row["EXCHANGE"].iat[0]
                if isinstance(val, str) and val.strip():
                    return val.strip().upper()
        except Exception:
            pass

    s = symbol.upper()

    if s.endswith(".NS") or s.endswith(":NSE") or s.endswith("-NS"):
        return "NSE"
    if s.endswith(".BO") or s.endswith(":BSE") or s.endswith("-BO"):
        return "BSE"

    if any(x in s for x in ["CRUDE","OIL","BRENT","WTI"]):
        return "MCX"
    if any(x in s for x in ["GOLD","SILVER","XAU","XAG"]):
        return "MCX"
    if any(x in s for x in ["BTC","ETH","BNB","COIN","INDEX","NIFTY","SENSEX"]):
        return "INDEX"
    if any(x in s for x in ["USD","EUR","GBP","JPY","FOREX","OANDA"]):
        return "OANDA"

    # Check known exchange codes inside symbol
    known = {"BSE","INDEX","CAPITALCOM","TVC","IG","MCX","OANDA","NSE",
             "NSEIX","SKILLING","SPREADEX","SZSE","VANTAGE"}
    for ex in known:
        if ex in s:
            return ex

    return "NSE"

# ===========================
# SUPERTREND CALCULATION
# ===========================
def compute_supertrend(df: pd.DataFrame, period: int = 10, multiplier: float = 3.0):
    from ta.volatility import AverageTrueRange
    atr = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=period).average_true_range()
    hl2 = (df['high'] + df['low']) / 2.0
    upperband = hl2 + (multiplier * atr)
    lowerband = hl2 - (multiplier * atr)
    final_upper, final_lower = upperband.copy(), lowerband.copy()
    supertrend = pd.Series(index=df.index, dtype='float64')
    direction = pd.Series(index=df.index, dtype='int64')

    for i in range(len(df)):
        if i == 0:
            final_upper.iat[i], final_lower.iat[i] = upperband.iat[i], lowerband.iat[i]
            supertrend.iat[i], direction.iat[i] = final_upper.iat[i], 1
            continue
        fu_prev, fl_prev = final_upper.iat[i-1], final_lower.iat[i-1]
        close_prev = df['close'].iat[i-1]
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

# ===========================
# SIGNAL LOGIC
# ===========================
def calculate_signals(symbol: str):
    global tv
    try:
        exchange = detect_exchange(symbol)
        df = tv.get_hist(symbol=symbol, exchange=exchange, interval=Interval.in_30_minute, n_bars=200)
        if df is None or df.empty:
            print(f"No data for {symbol} ({exchange})")
            return
        df = df.reset_index().rename(columns={df.columns[0]: "datetime"})
        for col in ["close","high","low"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.dropna(inplace=True)
        if len(df) < 50:
            print(f"Insufficient bars for {symbol}")
            return

        ema20 = EMAIndicator(df["close"], window=20).ema_indicator()
        supertrend_series, _ = compute_supertrend(df, period=10, multiplier=3.0)

        close_now, close_prev = df["close"].iat[-1], df["close"].iat[-2]
        ema_now, ema_prev = ema20.iat[-1], ema20.iat[-2]
        super_now, super_prev = supertrend_series.iat[-1], supertrend_series.iat[-2]

        buy = (close_now > ema_now) and (close_now > super_now) and not ((close_prev > ema_prev) and (close_prev > super_prev))
        sell = (close_now < ema_now) and (close_now < super_now) and not ((close_prev < ema_prev) and (close_prev < super_prev))

        from ta.volatility import AverageTrueRange
        atr = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=14).average_true_range().iat[-1]

        if buy:
            tp, sl = close_now + atr * 2.0, close_now - atr * 1.0
            msg = (f"**PERFECT 5 SIGNAL - BUY**\n"
                   f"**Symbol:** `{symbol}`\n**Exchange:** `{exchange}`\n"
                   f"**Price:** `{close_now:.2f}`\n**TP:** `{tp:.2f}`\n**SL:** `{sl:.2f}`\n**TF:** `30m`")
            print(f"BUY → {symbol}")
            send_telegram_message(msg)

        if sell:
            tp, sl = close_now - atr * 2.0, close_now + atr * 1.0
            msg = (f"**PERFECT 5 SIGNAL - SELL**\n"
                   f"**Symbol:** `{symbol}`\n**Exchange:** `{exchange}`\n"
                   f"**Price:** `{close_now:.2f}`\n**TP:** `{tp:.2f}`\n**SL:** `{sl:.2f}`\n**TF:** `30m`")
            print(f"SELL → {symbol}")
            send_telegram_message(msg)

    except Exception as e:
        if "Session expired" in str(e) or "401" in str(e):
            print("⚠️ Session expired, reloading cookies...")
            tv = load_tv_session()
        else:
            print(f"Error processing {symbol}: {e}")

# ===========================
# MAIN LOOP
# ===========================
def main_loop():
    print(f"\n🕒 Starting scan at {datetime.now().strftime('%H:%M:%S')}")
    for sym in symbols:
        calculate_signals(sym)
        time.sleep(0.5)
    print(f"✅ Scan finished at {datetime.now().strftime('%H:%M:%S')}\n")

# ===========================
# KEEP-ALIVE SERVER (Render)
# ===========================
PORT = 8000
class HealthCheckHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is alive!")

def run_server():
    with socketserver.TCPServer(("", PORT), HealthCheckHandler) as httpd:
        print(f"🌐 Health-check server running on port {PORT}")
        httpd.serve_forever()

# ===========================
# RUN BOT FOREVER
# ===========================
if __name__ == "__main__":
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    while True:
        main_loop()
        time.sleep(120)
