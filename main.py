# save as perfect5_signal_bot.py
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

# ===========================
# Load environment
# ===========================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TV_USERNAME = os.getenv("TV_USERNAME")
TV_PASSWORD = os.getenv("TV_PASSWORD")

# ===========================
# Auto clear old tvDatafeed login cookies
# ===========================
cache_path = os.path.expanduser("~/.tvdatafeed")
if os.path.exists(cache_path):
    try:
        shutil.rmtree(cache_path)
        print("Old tvDatafeed login cache cleared.")
    except Exception as e:
        print("Could not clear cache:", e)
else:
    print("No old tvDatafeed cache found.")

# ===========================
# TradingView Login (username/password only)
# ===========================
try:
    if not (TV_USERNAME and TV_PASSWORD):
        raise Exception("TV_USERNAME or TV_PASSWORD missing in .env")
    tv = TvDatafeed(username=TV_USERNAME, password=TV_PASSWORD)
    print("TradingView login successful (via username/password).")
except Exception as e:
    print(f"TradingView login error: {e}")
    try:
        tv = TvDatafeed(nologin=True)
        print("Proceeding with nologin client (limited access).")
    except Exception as ex:
        print(f"Unable to initialize tvDatafeed client: {ex}")
        raise

# ===========================
# Load CSV
# ===========================
CSV_PATH = r"ALL_WATCHLIST_SYMBOLS.csv"
if not os.path.exists(CSV_PATH):
    raise FileNotFoundError(f"CSV file not found at {CSV_PATH}")

symbols_df = pd.read_csv(CSV_PATH)
if "SYMBOL" not in symbols_df.columns:
    raise KeyError("CSV must contain a 'SYMBOL' column")

symbols = symbols_df["SYMBOL"].dropna().astype(str).unique().tolist()
print(f"Loaded {len(symbols)} symbols from CSV")

# ===========================
# Telegram Function
# ===========================
def send_telegram_message(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram config missing — cannot send message")
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        resp = requests.post(url, data={"chat_id": CHAT_ID, "text": text})
        if resp.status_code != 200:
            print(f"Telegram API returned {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"Telegram error: {e}")

# ===========================
# Exchange Detection
# ===========================
def detect_exchange(symbol: str) -> str:
    """Auto detect exchange name based on CSV 'EXCHANGE' column or symbol suffix."""
    if "EXCHANGE" in symbols_df.columns:
        v = symbols_df.loc[symbols_df["SYMBOL"] == symbol, "EXCHANGE"]
        if not v.empty:
            return v.values[0]

    if symbol.endswith(".NS"):
        return "NSE"
    if symbol.endswith(".BO"):
        return "BSE"
    if any(x in symbol for x in ["CRUDE", "GOLD", "SILVER"]):
        return "MCX"
    if any(x in symbol for x in ["USD", "OIL", "EUR", "BTC", "INDEX"]):
        return "INDEX"
    return "NSE"

# ===========================
# Supertrend (Python) implementation
# ===========================
def compute_supertrend(df: pd.DataFrame, period: int = 10, multiplier: float = 3.0):
    """
    Returns (supertrend_series, direction_series)
    direction = 1  (bull)   or   -1 (bear)
    """
    from ta.volatility import AverageTrueRange
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

# ===========================
# Signal Logic (NEW – exact Pine version)
# ===========================
def calculate_signals(symbol: str):
    try:
        exchange = detect_exchange(symbol)
        df = tv.get_hist(symbol=symbol, exchange=exchange,
                         interval=Interval.in_30_minute, n_bars=200)
        if df is None or df.empty:
            print(f"No data for {symbol} ({exchange})")
            return

        df = df.reset_index()
        df = df.rename(columns={df.columns[0]: "datetime"})
        for col in ["close", "high", "low"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.dropna(inplace=True)

        if len(df) < 50:
            print(f"Insufficient bars for {symbol}")
            return

        # ---- Indicators ----
        ema20 = EMAIndicator(df["close"], window=20).ema_indicator()
        supertrend_series, _ = compute_supertrend(df, period=10, multiplier=3.0)

        # ---- Current & previous values ----
        close_now   = df["close"].iat[-1]
        close_prev  = df["close"].iat[-2]
        ema_now     = ema20.iat[-1]
        ema_prev    = ema20.iat[-2]
        super_now   = supertrend_series.iat[-1]
        super_prev  = supertrend_series.iat[-2]

        # ---- NEW BUY / SELL CONDITIONS (exact Pine logic) ----
        buy_condition = (
            (close_now > ema_now) and
            (close_now > super_now) and
            not ((close_prev > ema_prev) and (close_prev > super_prev))
        )

        sell_condition = (
            (close_now < ema_now) and
            (close_now < super_now) and
            not ((close_prev < ema_prev) and (close_prev < super_prev))
        )

        # ---- Send signal if condition met ----
        if buy_condition or sell_condition:
            # Simple ATR-based TP/SL (you can adjust)
            from ta.volatility import AverageTrueRange
            atr = AverageTrueRange(high=df['high'], low=df['low'],
                                   close=df['close'], window=14).average_true_range().iat[-1]

            if buy_condition:
                tp = close_now + atr * 2.0
                sl = close_now - atr * 1.0
                msg = (
                    f"**PERFECT 5 SIGNAL - BUY**\n"
                    f"**Symbol:** `{symbol}`\n"
                    f"**Price:** `{close_now:.2f}`\n"
                    f"**Exchange:** `{exchange}`\n"
                    f"**TP:** `{tp:.2f}`\n"
                    f"**SL:** `{sl:.2f}`\n"
                    f"**TF:** `30m`"
                )
                print(f"BUY signal for {symbol}")
                send_telegram_message(msg)

                with open("signals_log.txt", "a", encoding="utf-8") as f:
                    f.write(f"BUY,{symbol},{df['datetime'].iat[-1]}\n")

            if sell_condition:
                tp = close_now - atr * 2.0
                sl = close_now + atr * 1.0
                msg = (
                    f"**PERFECT 5 SIGNAL - SELL**\n"
                    f"**Symbol:** `{symbol}`\n"
                    f"**Price:** `{close_now:.2f}`\n"
                    f"**Exchange:** `{exchange}`\n"
                    f"**TP:** `{tp:.2f}`\n"
                    f"**SL:** `{sl:.2f}`\n"
                    f"**TF:** `30m`"
                )
                print(f"SELL signal for {symbol}")
                send_telegram_message(msg)

                with open("signals_log.txt", "a", encoding="utf-8") as f:
                    f.write(f"SELL,{symbol},{df['datetime'].iat[-1]}\n")

    except Exception as e:
        print(f"Error processing {symbol}: {e}")

# ===========================
# Main Loop
# ===========================
def main_loop():
    print(f"\n--- Starting scan at {datetime.now().strftime('%H:%M:%S')} ---")
    if os.path.exists("signals_log.txt"):
        os.remove("signals_log.txt")

    for symbol in symbols:
        calculate_signals(symbol)
        time.sleep(0.5)          # be gentle to the API

    print(f"--- Scan finished at {datetime.now().strftime('%H:%M:%S')} ---\n")

# ===========================
# Keep-Alive Server (for cloud deployment)
# ===========================
PORT = 8000


class HealthCheckHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is alive!")


def run_server():
    with socketserver.TCPServer(("", PORT), HealthCheckHandler) as httpd:
        print(f"Serving health check at port {PORT}")
        httpd.serve_forever()


if __name__ == "__main__":
    # Health-check server in background
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()

    # Run the bot (you can wrap it in a while True for continuous scanning)
    while True:
        main_loop()
        time.sleep(120)   # wait 2 minute before next full scan
