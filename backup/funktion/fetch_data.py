import asyncio, datetime, os, csv
import yfinance as yf

from flask import Flask, request, Response, jsonify
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from binance.client import Client
from dateutil.relativedelta import relativedelta

from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv("BINANCE_API_KEY") or "DEIN_KEY"
API_SECRET = os.getenv("BINANCE_API_SECRET") or "DEIN_SECRET"
binance_client = Client(API_KEY, API_SECRET)

CSV_FOLDER = "csv"
os.makedirs(CSV_FOLDER, exist_ok=True)
app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=8)

# --------------------
# Helper: Symbol format
# --------------------
def format_symbol(symbol, source):
    if source=="binance":
        if "USDT" not in symbol:
            symbol = symbol.replace("-USD","")+"USDT"
    elif source=="yahoo":
        if "USDT" in symbol:
            symbol = symbol.replace("USDT","-USD")
    return symbol

# --------------------
# Candle Chunking (für Yahoo & Binance)
# --------------------
def fetch_candles(symbol, source="yahoo", interval="1m", start=None, end=None):
    symbol = format_symbol(symbol, source)
    candles = []
    
    if not start: start = datetime.datetime.now() - relativedelta(days=7)
    if not end: end = datetime.datetime.now()
    
    if source=="yahoo":
        max_days = 7
        while start < end:
            chunk_end = min(start + relativedelta(days=max_days), end)
            hist = yf.Ticker(symbol).history(start=start, end=chunk_end, interval=interval)
            for i in range(1,len(hist)):
                prev, curr = hist["Close"].iloc[i-1], hist["Close"].iloc[i]
                candles.append({
                    "timestamp": hist.index[i].strftime("%Y-%m-%d %H:%M"),
                    "symbol": symbol,
                    "open": hist["Open"].iloc[i],
                    "high": hist["High"].iloc[i],
                    "low": hist["Low"].iloc[i],
                    "close": curr,
                    "color": "green" if curr>prev else "red"
                })
            start = chunk_end
    else:
        # Binance
        interval_map = {"1m":1,"3m":3,"5m":5,"15m":15,"30m":30,"1h":60,"2h":120,"4h":240,"1d":1440}
        minutes_per_candle = interval_map.get(interval,1)
        max_candles = 1000
        max_minutes = max_candles*minutes_per_candle
        while start < end:
            chunk_end = min(start + datetime.timedelta(minutes=max_minutes), end)
            klines = binance_client.get_klines(
                symbol=symbol,
                interval=interval,
                startTime=int(start.timestamp()*1000),
                endTime=int(chunk_end.timestamp()*1000),
                limit=1000
            )
            for k in klines:
                prev_close = float(k[4])
                curr_close = float(k[4])
                candles.append({
                    "timestamp": datetime.datetime.fromtimestamp(k[0]/1000).strftime("%Y-%m-%d %H:%M"),
                    "symbol": symbol,
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": curr_close,
                    "color": "green" if curr_close>prev_close else "red"
                })
            start = chunk_end + datetime.timedelta(minutes=minutes_per_candle)
    return candles

# --------------------
# Live Loop
# --------------------
async def live_loop(symbols, source="yahoo", interval="1m"):
    symbols = [format_symbol(s, source) for s in symbols]
    while True:
        output = {}
        for s in symbols:
            candle = fetch_candles(s, source, interval, start=datetime.datetime.now()-relativedelta(minutes=2), end=datetime.datetime.now())
            if candle:
                c = candle[-1]
                output[s] = c
                print(f"{c['symbol']} | {c['timestamp']} | O:{c['open']} H:{c['high']} L:{c['low']} C:{c['close']} | {c['color'].upper()}")
        await asyncio.sleep(1)

# --------------------
# CSV Export
# --------------------
def save_to_csv(candles, symbol, source, interval="1m"):
    folder = os.path.join(CSV_FOLDER, f"{symbol}-{source}")
    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, f"{symbol}-{interval}-{source}.csv")
    header = ["timestamp","symbol","open","high","low","close","color"]
    with open(file_path,"w",newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(candles)
    return file_path
