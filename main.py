# main.py → Perfect5Bot (हर स्कैन में LATEST — पुराना कभी नहीं)
import os, base64, json, logging, threading, time
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify
import pandas as pd
import requests
from tvDatafeed import TvDatafeed, Interval
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange
import pickle

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("Perfect5Bot")

# ================== ENV ==================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
CSV_PATH = os.getenv("CSV_PATH", "ALL_WATCHLIST_SYMBOLS.csv")
PORT = int(os.getenv("PORT", 8000))
PAUSE_BETWEEN_SYMBOLS = float(os.getenv("PAUSE_BETWEEN_SYMBOLS", "5"))
SLEEP_BETWEEN_SCANS = float(os.getenv("SLEEP_BETWEEN_SCANS", "300"))
N_BARS = int(os.getenv("N_BARS", "96"))

FALLBACK_EXCHANGES = ["NSE","BSE","MCX","TVC","INDEX","OANDA","SKILLING","CAPITALCOM","VANTAGE","IG","SPREADEX","SZSE","NSEIX"]

# ★★★ नया ग्लोबल ★★★
last_latest_time = {}   # "NSE:RELIANCE:BUY" → datetime

# ================== MARKET TIMINGS ==================
MARKET_TIMINGS = {
    "NSE": {"start": "09:15", "end": "15:30", "days": [0,1,2,3,4]},
    "BSE": {"start": "09:00", "end": "15:30", "days": [0,1,2,3,4]},
    "MCX": {"start": "09:00", "end": "23:55", "days": [0,1,2,3,4]},
    "DEFAULT": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4,5,6]}
}

def is_market_open(exchange: str) -> bool:
    now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    t = now.time(); d = now.weekday()
    tm = MARKET_TIMINGS.get(exchange.upper(), MARKET_TIMINGS["DEFAULT"])
    if d not in tm["days"]: return False
    s = datetime.strptime(tm["start"], "%H:%M").time()
    e = datetime.strptime(tm["end"], "%H:%M").time()
    return s <= t <= e if s < e else t >= s or t <= e

# ================== TELEGRAM ==================
def send_telegram_message(text: str):
    if not BOT_TOKEN or not CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                      data={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"})
    except: pass

# ================== TV INIT ==================
tv = None
def init_tv():
    global tv
    if os.path.exists("cookies.b64.txt"):
        try:
            tv = TvDatafeed(cookies=pickle.loads(base64.b64decode(open("cookies.b64.txt").read().strip())))
            return
        except: pass
    u, p = os.getenv("TV_USERNAME"), os.getenv("TV_PASSWORD")
    if u and p:
        try: tv = TvDatafeed(u, p)
        except: pass
    if not tv: tv = TvDatafeed()

init_tv()

# ================== HELPERS ==================
def parse_symbol(s):
    s = s.strip()
    if ":" in s: return s.split(":",1)
    if s.upper().endswith((".NS",".BO")): return ("NSE" if ".NS" in s else "BSE", s[:-3])
    return "NSE", s

def try_get_hist(sym, ex, n):
    for e in [ex] + FALLBACK_EXCHANGES + [None]:
        try:
            df = tv.get_hist(sym, e, Interval.in_30_minute, n)
            if df is not None and not df.empty: return df, e or ex
        except: pass
    return None, None

def compute_supertrend(df, p=10, m=3):
    df = df.copy().reset_index(drop=True)
    hl2 = (df.high + df.low)/2
    atr = AverageTrueRange(df.high, df.low, df.close, p).average_true_range()
    up = hl2 + m*atr; dn = hl2 - m*atr
    trend = pd.Series(0.0, index=df.index)
    dir = pd.Series(1, index=df.index)
    for i in range(1, len(df)):
        up[i] = up[i] if up[i] < up[i-1] or df.close[i-1] > up[i-1] else up[i-1]
        dn[i] = dn[i] if dn[i] > dn[i-1] or df.close[i-1] < dn[i-1] else dn[i-1]
        if df.close[i] > up[i-1]: dir[i], trend[i] = 1, dn[i]
        elif df.close[i] < dn[i-1]: dir[i], trend[i] = -1, up[i]
        else: dir[i], trend[i] = dir[i-1], trend[i-1]
    return trend, dir

# ================== SYMBOLS ==================
symbols = pd.read_csv(CSV_PATH)["SYMBOL"].dropna().astype(str).tolist()

# ================== MAIN LOGIC ==================
def calculate_signals(raw):
    try:
        ex, sym = parse_symbol(raw)
        if not sym or not is_market_open(ex): return
        df, used = try_get_hist(sym, ex, N_BARS)
        if df is None or df.empty: return

        # normalize
        if 'datetime' not in df.columns: df['datetime'] = df.index
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df[['datetime','open','high','low','close']].dropna()

        ema20 = EMAIndicator(df.close, 20).ema_indicator()
        super_t, _ = compute_supertrend(df)
        atr = AverageTrueRange(df.high, df.low, df.close, 14).average_true_range()

        display = f"{used}:{sym}"
        key = f"{used}:{sym}"

        # find LATEST crossover
        latest_time = latest_buy = latest_sell = None
        for i in range(max(1, len(df)-N_BARS), len(df)):
            c, cp = df.close.iat[i], df.close.iat[i-1]
            e, ep = ema20.iat[i], ema20.iat[i-1]
            s, sp = super_t.iat[i], super_t.iat[i-1]
            if pd.isna(any([e,ep,s,sp])): continue
            buy  = c > e and c > s and not (cp > ep and cp > sp)
            sell = c < e and c < s and not (cp < ep and cp < sp)
            if buy or sell:
                t = df.datetime.iat[i]
                if latest_time is None or t > latest_time:
                    latest_time = t
                    latest_buy  = (c, atr.iat[i]) if buy else None
                    latest_sell = (c, atr.iat[i]) if sell else None

        # ★★★★ नया SEND ★★★★
        if latest_buy or latest_sell:
            close, a = (latest_buy or latest_sell)
            buy = latest_buy is not None
            ist = (latest_time.astimezone(timezone.utc).replace(tzinfo=None) + timedelta(hours=5,minutes=30)).strftime("%d-%b %H:%M") if latest_time else datetime.now().strftime("%d-%b %H:%M")
            full_key = f"{key}:{'BUY' if buy else 'SELL'}"

            # पुराना सिग्नल ब्लॉक
            if full_key in last_latest_time and latest_time < last_latest_time[full_key]:
                return

            tp = close + a*3 if buy else close - a*3
            sl = close - a*1.5 if buy else close + a*1.5
            msg = f"**PERFECT 5 SIGNAL - {'BUY' if buy else 'SELL'}**\nSymbol: `{display}`\nPrice: `{close:.2f}`\nTP: `{tp:.2f}`\nSL: `{sl:.2f}`\nTime: `{ist} IST`"
            log.info("LATEST %s → %s", 'BUY' if buy else 'SELL', ist)
            send_telegram_message(msg)

            # याद रखो
            last_latest_time[full_key] = latest_time

    except Exception as e:
        log.exception("ERR %s", raw)

# ================== LOOP ==================
def scan_loop():
    log.info("BOT STARTED")
    while True:
        for s in symbols:
            calculate_signals(s)
            time.sleep(PAUSE_BETWEEN_SYMBOLS)
        time.sleep(SLEEP_BETWEEN_SCANS)

# ================== FLASK ==================
app = Flask(__name__)

@app.route("/")
def home():
    return jsonify(status="Perfect5Bot OK", time=datetime.now(timezone.utc).isoformat())

@app.route("/health")
def health():
    return "OK"

if __name__ == "__main__":
    threading.Thread(target=scan_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=os.getenv("PORT", 8000), use_reloader=False)
    
