import time
import pandas as pd
from datetime import datetime, timedelta
from tvDatafeed import TvDatafeed, Interval
import logging as log

# --- Setup ---
log.basicConfig(level=log.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# TradingView login
try:
    tv = TvDatafeed()
    tv.login(username=TV_USERNAME, password=TV_PASSWORD)
    log.info("✅ Logged in via username/password.")
except Exception as e:
    log.warning(f"⚠️ Cookie login failed, using nologin fallback: {e}")
    tv = TvDatafeed()

# --- Load symbols ---
symbols_df = pd.read_csv("symbols.csv")
symbols = symbols_df["SYMBOL"].tolist()
log.info(f"✅ Loaded {len(symbols)} symbols from CSV")

# --- Candle Sync ---
def wait_for_next_30m_candle():
    """Waits until next 30m candle close (UTC-based)."""
    now = datetime.utcnow()
    minutes = now.minute % 30
    wait = (30 - minutes) * 60 - now.second
    if wait < 0:
        wait = 0
    next_candle_time = now + timedelta(seconds=wait)
    log.info(f"🕒 Waiting {int(wait)}s until next 30m candle close ({next_candle_time.strftime('%H:%M:%S')} UTC)")
    time.sleep(wait + 2)

# --- Signal Scan ---
def scan_signals():
    for sym in symbols:
        try:
            exchange, symbol = sym.split(":")
            data = tv.get_hist(symbol=symbol, exchange=exchange, interval=Interval.in_30_minute, n_bars=96)
            if data is None or data.empty:
                log.warning(f"No data for {sym}")
                continue

            # --- Indicator Logic (same as PineScript) ---
            data["EMA5"] = data["close"].ewm(span=5).mean()
            data["EMA20"] = data["close"].ewm(span=20).mean()
            data["ATR"] = data["high"] - data["low"]

            buy_cond = (data["EMA5"] > data["EMA20"]) & (data["EMA5"].shift(1) <= data["EMA20"].shift(1))
            sell_cond = (data["EMA5"] < data["EMA20"]) & (data["EMA5"].shift(1) >= data["EMA20"].shift(1))

            data["signal"] = None
            data.loc[buy_cond, "signal"] = "BUY"
            data.loc[sell_cond, "signal"] = "SELL"

            latest_signal = data.dropna(subset=["signal"]).iloc[-1] if not data["signal"].dropna().empty else None
            if latest_signal is None:
                continue

            latest_type = latest_signal["signal"]
            close_now = latest_signal["close"]
            atr = latest_signal["ATR"]

            # --- Calculate TP & SL (1:3 R:R) ---
            if latest_type == "BUY":
                tp, sl = close_now + atr * 3.0, close_now - atr * 1.5
            else:
                tp, sl = close_now - atr * 3.0, close_now + atr * 1.5

            # --- Convert time to IST ---
            ist_time = (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%d-%b %H:%M")

            # --- Telegram Message ---
            msg = (f"**PERFECT 5 SIGNAL - {latest_type}**\n"
                   f"Symbol: `{symbol}`\nExchange: `{exchange}`\n"
                   f"Price: `{close_now:.2f}`\nTP: `{tp:.2f}`\nSL: `{sl:.2f}`\nTime: `{ist_time} IST`")

            log.info(f"{latest_type} → {exchange}:{symbol}")
            send_telegram_message(msg)

        except Exception as e:
            log.error(f"Error scanning {sym}: {e}")

# --- Main Loop ---
log.info("🚀 Scanner started (30m UTC sync, last 96 candles mode).")
while True:
    wait_for_next_30m_candle()
    log.info(f"🔍 Starting scan at {datetime.utcnow().strftime('%H:%M:%S')} UTC")
    scan_signals()
