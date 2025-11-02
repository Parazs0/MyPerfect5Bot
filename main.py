# main.py — Perfect5Bot: Market Hours Filter Updated (INDEX 24/7 for Global)
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
# Market Timings (IST, Updated for INDEX 24/7 Global Rates)
# -----------------------------
MARKET_TIMINGS = {
    "NSE": {"start": "09:15", "end": "15:30", "days": [0,1,2,3,4]},  # Mon-Fri
    "BSE": {"start": "09:00", "end": "15:30", "days": [0,1,2,3,4]},  # Mon-Fri
    "MCX": {"start": "09:00", "end": "23:55", "days": [0,1,2,3,4]},  # Mon-Fri
    "IG": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # 24/5 Forex/CFDs
    "CAPITALCOM": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # 24/5 CFDs
    "SPREADEX": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # Approx Mon-Fri (12:30PM-2:30AM IST)
    "TVC": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # 24/7 Composites (Crypto/Indices vary)
    "INDEX": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4,5,6]},  # 24/7 Global (Gold/Silver rates)
    "OANDA": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # 24/5 Forex
    "NSEIX": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # NSE Indices (same as NSE)
    "SKILLING": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # 24/5 CFD/Forex
    "SZSE": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # Mon-Fri (CST to IST)
    "VANTAGE": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]},  # 24/5 Forex/CFD
    "DEFAULT": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4]}  # Fallback
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
            cookies
