# live_loop_train_csv.py
from flask import Flask, request, Response, jsonify
import asyncio
import datetime
import os
import aiohttp
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from binance.client import Client
import yfinance as yf
from dotenv import load_dotenv
import csv
import pandas as pd
from dateutil.relativedelta import relativedelta
from colorama import init, Fore, Style

load_dotenv()

# --- Setup ---
API_KEY = os.getenv("BINANCE_API_KEY") or "DEIN_KEY"
API_SECRET = os.getenv("BINANCE_API_SECRET") or "DEIN_SECRET"
binance_client = Client(API_KEY, API_SECRET)

CSV_FOLDER = "csv"
os.makedirs(CSV_FOLDER, exist_ok=True)

app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=8)
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # optional

# --- Funktion für farbige Kerzenanzeige ---
def print_candle(candle, prefix="[LIVE]"):
    ts = candle.get("timestamp")
    symbol = candle.get("symbol")
    o = candle.get("open")
    c = candle.get("close")
    h = candle.get("high")
    l = candle.get("low")
    color = candle.get("color","yellow")

    # Farbauswahl
    if color=="green":
        col = Fore.GREEN
        arrow = "↑"
    elif color=="red":
        col = Fore.RED
        arrow = "↓"
    else:
        col = Fore.YELLOW
        arrow = "→"

    print(f"{prefix} {symbol} {ts} {col}{arrow} Open:{o:.2f} Close:{c:.2f}{Style.RESET_ALL} High:{h:.2f} Low:{l:.2f}")

# --- Helper Functions ---
def format_symbol(symbol, source):
    """Konvertiert Symbol je nach Source automatisch.
    USDT -> USD für Yahoo Finance
    Binance -> USDT Symbol
    """
    if source == "binance":
        if "USDT" not in symbol:
            symbol = symbol.replace("-USD","")+"USDT"
    elif source == "yahoo":
        symbol = symbol.replace("USDT","-USD")
    return symbol

# --- Candle Fetching ---
def fetch_binance_candle(symbol):
    """Hole letzte Candle von Binance für Live-Check"""
    klines = binance_client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE, limit=2)
    prev_close = float(klines[0][4])
    current_close = float(klines[1][4])
    color = "green" if current_close > prev_close else "red"
    return {
        "symbol": symbol,
        "prev_close": prev_close,
        "current_close": current_close,
        "color": color,
        "open": float(klines[1][1]),
        "high": float(klines[1][2]),
        "low": float(klines[1][3]),
        "close": current_close,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    }

def fetch_yf_candle(symbol, interval="1m", period="1mo"):
    """Yahoo Finance: letzte Candle"""
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period=period, interval=interval)
    if hist.empty or len(hist)<2:
        raise ValueError(f"Keine Daten für {symbol}")
    prev_close = hist["Close"].iloc[-2]
    current_close = hist["Close"].iloc[-1]
    color = "green" if current_close > prev_close else "red"
    return {
        "symbol": symbol,
        "prev_close": prev_close,
        "current_close": current_close,
        "color": color,
        "open": hist["Open"].iloc[-1],
        "high": hist["High"].iloc[-1],
        "low": hist["Low"].iloc[-1],
        "close": current_close,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    }

# --- Chunked Fetch für CSV / Training ---
def fetch_candles_chunked(symbol, source="yahoo", interval="1m", period="1mo"):
    """Fetch für CSV oder Training in Chunks.
    Funktioniert für Yahoo + Binance.
    period: z.B. '7d', '1mo', '3mo', '1y', '2024', '2024-2025', '1q', 'jan-feb'
    """
    # Start / End bestimmen
    now = pd.Timestamp.now()
    start, end = now - pd.Timedelta(days=30), now  # default fallback
    try:
        if period.endswith("d"):
            start = now - pd.Timedelta(days=int(period[:-1]))
        elif period.endswith("mo"):
            start = now - relativedelta(months=int(period[:-2]))
        elif period.endswith("y"):
            start = now - relativedelta(years=int(period[:-1]))
        elif period.isdigit() and len(period)==4:
            year = int(period)
            start = pd.Timestamp(year=year, month=1, day=1)
            end = pd.Timestamp(year=year, month=12, day=31, hour=23, minute=59)
        elif "-" in period:
            parts = period.split("-")
            if all(p.isdigit() for p in parts):
                start = pd.Timestamp(year=int(parts[0]), month=1, day=1)
                end = pd.Timestamp(year=int(parts[1]), month=12, day=31, hour=23, minute=59)
    except:
        pass

    candles = []
    ticker = yf.Ticker(symbol) if source=="yahoo" else None

    # Chunking Limit für Yahoo = 7 Tage für 1m
    max_days = 7 if interval=="1m" else 60
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + pd.Timedelta(days=max_days), end)
        try:
            if source=="yahoo":
                hist = ticker.history(start=chunk_start, end=chunk_end, interval=interval)
            else:  # Binance
                interval_map = {"1m":1, "3m":3, "5m":5, "15m":15, "30m":30, "1h":60, "2h":120, "4h":240, "1d":1440}
                minutes_per_candle = interval_map.get(interval,1)
                max_candles = 1000
                max_minutes = max_candles*minutes_per_candle
                klines = binance_client.get_klines(
                    symbol=symbol,
                    interval=interval,
                    startTime=int(chunk_start.timestamp()*1000),
                    endTime=int(chunk_end.timestamp()*1000),
                    limit=1000
                )
                hist = pd.DataFrame(klines, columns=["open_time","open","high","low","close","volume","close_time",
                                                     "quote_asset_volume","trades","taker_buy_base","taker_buy_quote","ignore"])
                hist["Open"] = hist["open"].astype(float)
                hist["High"] = hist["high"].astype(float)
                hist["Low"] = hist["low"].astype(float)
                hist["Close"] = hist["close"].astype(float)
                hist.index = pd.to_datetime(hist["open_time"], unit='ms')

            for i in range(1,len(hist)):
                prev_close = hist["Close"].iloc[i-1]
                current_close = hist["Close"].iloc[i]
                color = "green" if current_close>prev_close else "red"
                candles.append({
                    "timestamp": hist.index[i].strftime("%Y-%m-%d %H:%M"),
                    "symbol": symbol,
                    "open": hist["Open"].iloc[i],
                    "high": hist["High"].iloc[i],
                    "low": hist["Low"].iloc[i],
                    "close": current_close,
                    "color": color
                })
        except Exception as e:
            print(f"Fehler beim Chunk {chunk_start} - {chunk_end}: {e}")
        chunk_start = chunk_end + pd.Timedelta(minutes=1)

    return candles

# --- CSV Speicher ---
def save_to_csv(candles, symbol, source, interval, period="7d"):
    """
    Speichert Candle-Liste in CSV.
    
    Dateiname enthält:
    - Symbol
    - Interval (1m, 1h, etc.)
    - Period (7d, 1mo)
    - Source (yahoo, binance)
    - Exakte Erstellzeit (YYYY-MM-DD_HH-MM-SS)
    
    Beispiel:
    csv/yahoo/BTC-USD-1m-7d-yahoo-2025-09-09_20-10-15.csv
    """
    # Ordner nach API/Source
    folder = os.path.join(CSV_FOLDER, f"{source}")
    os.makedirs(folder, exist_ok=True)
    
    # Dateiname mit Intervall, Zeitraum, Quelle, Timestamp
    timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"{symbol}-{interval}-{period}-{source}-{timestamp_str}.csv"
    file_path = os.path.join(folder, file_name)
    
    # CSV Header
    header = ["timestamp","symbol","open","high","low","close","color"]
    with open(file_path,"w",newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(candles)
    
    return file_path


# --- Live Loop ---
async def live_loop(symbols, source="yahoo", interval="1m", send_queue=None, start_with_history=False):
    """Live Loop für Terminal-Anzeige und SSE/API"""
    symbols = [format_symbol(s, source) for s in symbols]
    # Optional: Train Mode -> zuerst 7 Tage laden
    if start_with_history:
        for symbol in symbols:
            candles = fetch_candles_chunked(symbol, source, interval, period="7d")
            for candle in candles:
                if send_queue:
                    await send_queue.put({symbol:candle})
                print_candle(candle, prefix="[HIST]")
            await asyncio.sleep(0.1)

    while True:
        for symbol in symbols:
            try:
                candle = fetch_binance_candle(symbol) if source=="binance" else fetch_yf_candle(symbol, interval, period="1d")
                if send_queue:
                    await send_queue.put({symbol:candle})
                # Terminal lesbar
                #print(f"[LIVE] {symbol} {candle['timestamp']} {candle['open']:.2f} {candle['close']:.2f} ({candle['color']})")
                print_candle(candle, prefix="[LIVE]")
            except Exception as e:
                print(f"[LIVE ERROR] {symbol}: {e}")
        await asyncio.sleep(1)

# --- Thread Helper ---
def start_live_thread(symbols, source, interval, send_queue=None, start_with_history=False):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(live_loop(symbols, source, interval, send_queue, start_with_history))

# --- --- API Endpoints --- ---

@app.route("/api/live")
def api_live():
    symbols = request.args.get("symbols")
    source = request.args.get("source","yahoo").lower()
    interval = request.args.get("interval","1m")
    if not symbols:
        return jsonify({"error":"Bitte Parameter symbols angeben"}), 400
    symbols = symbols.split(",")
    
    result = {}
    for symbol in symbols:
        symbol_fmt = format_symbol(symbol, source)
        try:
            candle = fetch_binance_candle(symbol_fmt) if source=="binance" else fetch_yf_candle(symbol_fmt, interval)
            result[symbol_fmt] = candle
        except Exception as e:
            result[symbol_fmt] = {"error": str(e)}
    return jsonify(result)

@app.route("/api/train_mode")
def api_train_mode():
    symbols = request.args.get("symbols")
    source = request.args.get("source","yahoo").lower()
    interval = request.args.get("interval","1m")
    if not symbols:
        return jsonify({"error":"Bitte Parameter symbols angeben"}), 400
    symbols = symbols.split(",")

    result = {}
    for symbol in symbols:
        symbol_fmt = format_symbol(symbol, source)
        try:
            # Historische Daten 7 Tage
            candles = fetch_candles_chunked(symbol_fmt, source, interval, period="7d")
            # Aktuelle Live-Candle
            live_candle = fetch_binance_candle(symbol_fmt) if source=="binance" else fetch_yf_candle(symbol_fmt, interval)
            result[symbol_fmt] = {
                "history": candles,
                "live": live_candle
            }
        except Exception as e:
            result[symbol_fmt] = {"error": str(e)}
    return jsonify(result)

@app.route("/api/fetch_candle")
def api_fetch_candle():
    """
    Einzel-Candle abrufen:
    Holt nur die aktuelle Candle für ein Symbol.
    Beispiel:
    /api/fetch_candle?symbol=BTC-USD&source=yahoo&interval=1m
    """
    symbol = request.args.get("symbol")
    source = request.args.get("source","yahoo").lower()
    interval = request.args.get("interval","1m")
    if not symbol:
        return jsonify({"error":"Bitte Parameter symbol angeben"}), 400
    symbol = format_symbol(symbol, source)
    try:
        candle = fetch_binance_candle(symbol) if source=="binance" else fetch_yf_candle(symbol, interval)
        return jsonify(candle)
    except Exception as e:
        return jsonify({"error":str(e)})

@app.route("/api/csv")
def api_csv():
    """
    CSV Export mit Chunking:
    Holt historische Daten, speichert sie als CSV (inkl. Farbe für ML).
    Beispiel:
    /api/csv?symbol=BTC-USD&source=yahoo&interval=1m&period=7d
    """
    symbol = request.args.get("symbol")
    source = request.args.get("source","yahoo").lower()
    interval = request.args.get("interval","1m")
    period = request.args.get("period","7d")
    if not symbol:
        return jsonify({"error":"Bitte Parameter symbol angeben"}), 400
    symbol = format_symbol(symbol, source)
    try:
        candles = fetch_candles_chunked(symbol, source, interval, period)
        file_path = save_to_csv(candles, symbol, source, interval)
        return jsonify({"status":"CSV erstellt","rows":len(candles),"file":file_path})
    except Exception as e:
        return jsonify({"error":str(e)})

# --- Start Flask ---
if __name__=="__main__":
    app.run(debug=True, port=5000)
