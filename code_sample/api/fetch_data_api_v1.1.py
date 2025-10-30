# live_loop_train_csv_clean.py
from flask import Flask, request, jsonify
import os
import csv
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from colorama import init, Fore, Style
from binance.client import Client
import yfinance as yf
import requests
from dotenv import load_dotenv

load_dotenv()

# --- Setup Binance & Flask ---
API_KEY = os.getenv("BINANCE_API_KEY") or "DEIN_KEY"
API_SECRET = os.getenv("BINANCE_API_SECRET") or "DEIN_SECRET"
binance_client = Client(API_KEY, API_SECRET)

CSV_FOLDER = "csv"
os.makedirs(CSV_FOLDER, exist_ok=True)

WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # optional

app = Flask(__name__)
init(autoreset=True)  # colorama init

# ---------------------------
# --- Helper Functions ---
# ---------------------------

def format_symbol(symbol, source):
    """Symbol für Binance/Yahoo anpassen"""
    if source == "binance":
        if "USDT" not in symbol:
            symbol = symbol.replace("-USD","")+"USDT"
    elif source == "yahoo":
        symbol = symbol.replace("USDT","-USD")
    return symbol

def print_candle(candle, mode="LIVE"):
    """
    candle: dict mit keys timestamp, open, high, low, close
    mode: "LIVE" oder "TRAIN"
    """
    ts = candle.get("timestamp")
    o = candle.get("open")
    c = candle.get("close")
    h = candle.get("high")
    l = candle.get("low")

    # Prefix-Farbe
    mode_color = Fore.CYAN if mode=="LIVE" else Fore.MAGENTA

    # Open/Close Farbe
    if c > o:
        oc_color = Fore.GREEN
        arrow = "↑"
    elif c < o:
        oc_color = Fore.RED
        arrow = "↓"
    else:
        oc_color = Fore.YELLOW
        arrow = "→"

    # High/Low Farben
    high_color = Fore.GREEN if h > o else Fore.RED if h < o else Fore.YELLOW
    low_color = Fore.GREEN if l > o else Fore.RED if l < o else Fore.YELLOW

    print(f"{mode_color}[{mode}]{Style.RESET_ALL} {ts} | {arrow} "
          f"Open:{oc_color}{o:.2f}{Style.RESET_ALL} "
          f"Close:{oc_color}{c:.2f}{Style.RESET_ALL} "
          f"High:{high_color}{h:.2f}{Style.RESET_ALL} "
          f"Low:{low_color}{l:.2f}{Style.RESET_ALL}")

def publish_to_webhook(symbol, candle):
    """Optional: Candle an Webhook senden"""
    if WEBHOOK_URL:
        try:
            requests.post(WEBHOOK_URL, json={symbol:candle}, timeout=2)
        except Exception as e:
            print(f"[Webhook Error] {e}")

# ---------------------------
# --- Candle Fetch ---
# ---------------------------

def fetch_binance_candle(symbol):
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
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def fetch_yf_candle(symbol, interval="1m", period="1d"):
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
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def fetch_candles_chunked(symbol, source="yahoo", interval="1m", period="7d"):
    """Chunked historische Daten"""
    now = pd.Timestamp.now()
    start, end = now - pd.Timedelta(days=7), now  # default fallback

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
    max_days = 7 if interval=="1m" else 60
    chunk_start = start

    while chunk_start < end:
        chunk_end = min(chunk_start + pd.Timedelta(days=max_days), end)
        try:
            if source=="yahoo":
                hist = ticker.history(start=chunk_start, end=chunk_end, interval=interval)
            else:  # Binance
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
                    "timestamp": hist.index[i].strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": symbol,
                    "open": hist["Open"].iloc[i],
                    "high": hist["High"].iloc[i],
                    "low": hist["Low"].iloc[i],
                    "close": current_close,
                    "color": color
                })
        except Exception as e:
            print(f"[Chunk Error] {chunk_start} - {chunk_end}: {e}")
        chunk_start = chunk_end + pd.Timedelta(minutes=1)

    return candles

def save_to_csv(candles, symbol, source, interval, period="7d"):
    folder = os.path.join(CSV_FOLDER, f"{source}")
    os.makedirs(folder, exist_ok=True)
    timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"{symbol}-{interval}-{period}-{source}-{timestamp_str}.csv"
    file_path = os.path.join(folder, file_name)
    header = ["timestamp","symbol","open","high","low","close","color"]
    with open(file_path,"w",newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(candles)
    return file_path

# ---------------------------
# --- API Endpoints ---
# ---------------------------

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
            print_candle(candle)  # Terminalausgabe
            publish_to_webhook(symbol_fmt, candle)  # optional
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
            candles = fetch_candles_chunked(symbol_fmt, source, interval, period="7d")
            live_candle = fetch_binance_candle(symbol_fmt) if source=="binance" else fetch_yf_candle(symbol_fmt, interval)
            # Terminalausgabe letzte 5 Candles + Live
            for c in candles[-5:]:
                print_candle(c, prefix="[HIST]")
            print_candle(live_candle, prefix="[LIVE]")
            publish_to_webhook(symbol_fmt, live_candle)  # optional
            result[symbol_fmt] = {"history": candles, "live": live_candle}
        except Exception as e:
            result[symbol_fmt] = {"error": str(e)}
    return jsonify(result)

@app.route("/api/fetch_candle")
def api_fetch_candle():
    symbol = request.args.get("symbol")
    source = request.args.get("source","yahoo").lower()
    interval = request.args.get("interval","1m")
    if not symbol:
        return jsonify({"error":"Bitte Parameter symbol angeben"}), 400
    symbol_fmt = format_symbol(symbol, source)
    try:
        candle = fetch_binance_candle(symbol_fmt) if source=="binance" else fetch_yf_candle(symbol_fmt, interval)
        print_candle(candle)  # Terminalausgabe
        publish_to_webhook(symbol_fmt, candle)
        return jsonify(candle)
    except Exception as e:
        return jsonify({"error":str(e)})

@app.route("/api/csv")
def api_csv():
    symbol = request.args.get("symbol")
    source = request.args.get("source","yahoo").lower()
    interval = request.args.get("interval","1m")
    period = request.args.get("period","7d")
    if not symbol:
        return jsonify({"error":"Bitte Parameter symbol angeben"}), 400
    symbol_fmt = format_symbol(symbol, source)
    try:
        candles = fetch_candles_chunked(symbol_fmt, source, interval, period)
        file_path = save_to_csv(candles, symbol_fmt, source, interval, period)
        return jsonify({"status":"CSV erstellt","rows":len(candles),"file":file_path})
    except Exception as e:
        return jsonify({"error":str(e)})

# ---------------------------
# --- Start Flask ---
# ---------------------------

if __name__=="__main__":
    app.run(debug=True, port=5000)
