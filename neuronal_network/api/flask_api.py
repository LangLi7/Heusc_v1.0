# flask_api.py
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

def print_candle(candle, mode="LIVE", prefix=None):
    label = prefix if prefix else mode
    ts = candle.get("timestamp")
    o = candle.get("open")
    c = candle.get("close")
    h = candle.get("high")
    l = candle.get("low")
    v = candle.get("volume", 0)

    # Label-Farbe
    if label in ["[LIVE]", "LIVE"]:
        lbl_color = Fore.CYAN
    elif label in ["[TRAIN]", "[TRAIN_MODE]"]:
        lbl_color = Fore.MAGENTA
    else:
        lbl_color = Fore.WHITE

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

    print(f"{lbl_color}{label}{Style.RESET_ALL} {ts} | {arrow} "
          f"Open:{oc_color}{o:.2f}{Style.RESET_ALL} "
          f"Close:{oc_color}{c:.2f}{Style.RESET_ALL} "
          f"High:{high_color}{h:.2f}{Style.RESET_ALL} "
          f"Low:{low_color}{l:.2f}{Style.RESET_ALL} "
          f"Vol:{v:.2f}")

def publish_to_webhook(symbol, candle):
    """Optional: Candle an Webhook senden"""
    if WEBHOOK_URL:
        try:
            requests.post(WEBHOOK_URL, json={symbol:candle}, timeout=2)
        except Exception as e:
            print(f"[Webhook Error] {e}")

def cast_float(val):
    """Sicherstellen, dass es ein float ist (keine np.float64 etc.)"""
    try:
        return float(val)
    except:
        return 0.0

def build_candle(open_, high, low, close, volume, symbol, timestamp=None, prev_close=None):
    """Generische Candle-Erstellung mit Farblogik"""
    close = cast_float(close)
    open_ = cast_float(open_)
    high = cast_float(high)
    low = cast_float(low)
    volume = cast_float(volume)
    
    if prev_close is None:
        prev_close = open_
    else:
        prev_close = cast_float(prev_close)

    color = "green" if close > prev_close else "red"
    if not timestamp:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return {
        "symbol": symbol,
        "prev_close": prev_close,
        "current_close": close,
        "color": color,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "timestamp": timestamp
    }

# ---------------------------
# --- Candle Fetch ---
# ---------------------------

# --- Binance ---
def fetch_binance_candle(symbol):
    klines = binance_client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE, limit=2)
    return build_candle(
        open_=klines[1][1],
        high=klines[1][2],
        low=klines[1][3],
        close=klines[1][4],
        volume=klines[1][5],
        symbol=symbol,
        prev_close=klines[0][4]
    )

# --- Yahoo Finance ---
def fetch_yf_candle(symbol, interval="1m", period="1d"):
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period=period, interval=interval)
    if hist.empty or len(hist)<2:
        raise ValueError(f"Keine Daten für {symbol}")
    return build_candle(
        open_=hist["Open"].iloc[-1],
        high=hist["High"].iloc[-1],
        low=hist["Low"].iloc[-1],
        close=hist["Close"].iloc[-1],
        volume=hist["Volume"].iloc[-1] if "Volume" in hist else 0,
        symbol=symbol,
        prev_close=hist["Close"].iloc[-2]
    )

# --- Chunked Fetch ---
def fetch_candles_chunked(symbol, source="yahoo", interval="1m", period="6mo"):
    candles = []

    now = pd.Timestamp.now()
    # Period Parsing
    if period.endswith("d"):
        total_days = int(period[:-1])
    elif period.endswith("mo"):
        total_days = int(period[:-2]) * 30
    elif period.endswith("y"):
        total_days = int(period[:-1]) * 365
    else:
        total_days = 7

    start = now - pd.Timedelta(days=total_days)
    current_start = start

    while current_start < now:
        if source == "yahoo" and interval == "1m":
            chunk_days = 7  # Yahoo 1m Limit
        else:
            chunk_days = total_days  # Binance kann mehr

        current_end = min(current_start + pd.Timedelta(days=chunk_days), now)

        # --- Yahoo ---
        if source == "yahoo":
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=current_start, end=current_end, interval=interval)
            if hist.empty:
                current_start = current_end
                continue
            hist["prev_close"] = hist["Close"].shift(1)
            hist["color"] = (hist["Close"] > hist["prev_close"]).map({True:"green", False:"red"})
            for ts, row in hist.iterrows():
                if pd.isna(row["prev_close"]): continue
                candles.append(build_candle(
                    open_=row["Open"],
                    high=row["High"],
                    low=row["Low"],
                    close=row["Close"],
                    volume=row["Volume"] if "Volume" in row else 0,
                    symbol=symbol,
                    timestamp=ts.strftime("%Y-%m-%d %H:%M:%S"),
                    prev_close=row["prev_close"]
                ))

        # --- Binance ---
        else:
            start_ts = int(current_start.timestamp() * 1000)
            end_ts = int(current_end.timestamp() * 1000)
            while start_ts < end_ts:
                klines = binance_client.get_klines(
                    symbol=symbol,
                    interval=interval,
                    startTime=start_ts,
                    endTime=end_ts,
                    limit=1000
                )
                if not klines:
                    break
                df = pd.DataFrame(klines, columns=[
                    "open_time","open","high","low","close","volume","close_time",
                    "quote_asset_volume","trades","taker_buy_base","taker_buy_quote","ignore"
                ])
                df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
                df.index = pd.to_datetime(df["open_time"], unit='ms')
                df["prev_close"] = df["close"].shift(1)
                df["color"] = (df["close"] > df["prev_close"]).map({True:"green", False:"red"})
                for ts, row in df.iterrows():
                    if pd.isna(row["prev_close"]): continue
                    candles.append(build_candle(
                        open_=row["open"],
                        high=row["high"],
                        low=row["low"],
                        close=row["close"],
                        volume=row["volume"],
                        symbol=symbol,
                        timestamp=ts.strftime("%Y-%m-%d %H:%M:%S"),
                        prev_close=row["prev_close"]
                    ))
                start_ts = int(df["open_time"].iloc[-1]) + 60_000  # nächste Minute

        current_start = current_end

    # Sortiere nach Zeitstempel aufsteigend (älteste zuerst)
    candles.sort(key=lambda x: x["timestamp"])
    return candles

# --- Save CSV ---
def save_to_csv(candles, symbol, source, interval, period="7d"):
    folder = os.path.join(CSV_FOLDER, f"{source}")
    os.makedirs(folder, exist_ok=True)
    timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"{symbol}-{interval}-{period}-{source}-{timestamp_str}.csv"
    file_path = os.path.join(folder, file_name)

    if source == "binance":
        header = ["timestamp","symbol","open","high","low","close","prev_close","current_close","volume","color"]
    else:  # Yahoo
        header = ["timestamp","symbol","open","high","low","close","volume","color"]

    with open(file_path,"w",newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for candle in candles:
            row = {key: candle.get(key, "") for key in header}
            writer.writerow(row)
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
            print_candle(candle, prefix="[LIVE]")
            publish_to_webhook(symbol_fmt, candle)
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
            for c in candles[-5:]:
                print_candle(c, prefix="[HIST]")
            print_candle(live_candle, prefix="[LIVE]")
            publish_to_webhook(symbol_fmt, live_candle)
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
        print_candle(candle)
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
