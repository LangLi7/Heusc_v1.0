# live_loop_sse_csv.py
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

# --- Helper Functions ---
def fetch_binance_candle(symbol):
    """Hole 2 letzte 1-Minuten-Candles von Binance und berechne Farbe"""
    klines = binance_client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE, limit=2)
    prev_close = float(klines[0][4])
    current_close = float(klines[1][4])
    color = "green" if current_close > prev_close else "red"
    candle_data = {
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
    return candle_data

def fetch_yf_candle(symbol, interval="1m"):
    """Yahoo Finance: hole letzte Kerze"""
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="1d", interval=interval)
    if hist.empty or len(hist) < 2:
        raise ValueError(f"Keine Daten für {symbol}")
    prev_close = hist["Close"].iloc[-2]
    current_close = hist["Close"].iloc[-1]
    color = "green" if current_close > prev_close else "red"
    candle_data = {
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
    return candle_data

def save_to_csv(candle, source):
    """Speichert Candle in CSV für ML Training"""
    symbol = candle["symbol"].replace("/","-")
    interval = "1m"
    folder = os.path.join(CSV_FOLDER, f"{symbol}-{source}")
    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, f"{symbol}-{interval}-{source}.csv")
    header = ["timestamp","symbol","open","high","low","close","color"]
    write_header = not os.path.exists(file_path)
    with open(file_path, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(header)
        writer.writerow([candle["timestamp"], candle["symbol"], candle["open"],
                         candle["high"], candle["low"], candle["close"], candle["color"]])

async def push_webhook(data):
    if not WEBHOOK_URL:
        return
    async with aiohttp.ClientSession() as session:
        try:
            await session.post(WEBHOOK_URL, json=data)
        except Exception as e:
            print("Webhook Fehler:", e)

# --- Live Loop ---
async def live_loop(symbols, source="binance", interval="1m", send_queue=None):
    """Holt jede Sekunde Candles und pusht/CSV speichert"""
    while True:
        output = {}
        for symbol in symbols:
            try:
                if source == "binance":
                    candle = await asyncio.to_thread(fetch_binance_candle, symbol)
                else:
                    yf_symbol = symbol.replace("USDT","-USD")
                    candle = await asyncio.to_thread(fetch_yf_candle, yf_symbol)
                output[symbol] = candle
                save_to_csv(candle, source)
            except Exception as e:
                output[symbol] = {"error": str(e)}
        print(datetime.datetime.now(), output)

        # SSE Queue push
        if send_queue:
            await send_queue.put(output)

        # Optional Webhook
        await push_webhook(output)
        await asyncio.sleep(1)

# --- Thread Helper ---
def start_live_loop_thread(symbols, source, interval, send_queue=None):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(live_loop(symbols, source, interval, send_queue))

# --- SSE Endpoint ---
@app.route("/api/live_sse")
def live_sse():
    symbols = request.args.get("symbols")
    source = request.args.get("source","binance").lower()
    interval = request.args.get("interval","1m")
    if not symbols:
        return jsonify({"error":"Bitte Parameter symbols angeben"}), 400
    symbols = symbols.split(",")

    # Asyncio Queue für SSE
    send_queue = asyncio.Queue()

    # Start Live Loop Thread
    thread = Thread(target=start_live_loop_thread, args=(symbols, source, interval, send_queue), daemon=True)
    thread.start()

    # SSE Response
    def event_stream():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while True:
            data = loop.run_until_complete(send_queue.get())
            yield f"data: {data}\n\n"

    return Response(event_stream(), mimetype="text/event-stream")

# --- API Endpoint zum Starten (nur Status) ---
@app.route("/api/live_loop")
def api_live_loop():
    symbols = request.args.get("symbols")
    source = request.args.get("source","binance").lower()
    interval = request.args.get("interval","1m")
    if not symbols:
        return jsonify({"error":"Bitte Parameter symbols angeben"}), 400
    symbols = symbols.split(",")
    thread = Thread(target=start_live_loop_thread, args=(symbols, source, interval), daemon=True)
    thread.start()
    return jsonify({"status":"Live Loop gestartet","symbols":symbols,"source":source})

@app.route("/api/fetch_candle")
def fetch_candle():
    symbol = request.args.get("symbol")
    source = request.args.get("source","binance")
    if not symbol:
        return jsonify({"error":"Bitte Parameter symbol angeben"}), 400
    try:
        if source=="binance":
            candle = fetch_binance_candle(symbol)
        else:
            yf_symbol = symbol.replace("USDT","-USD")
            candle = fetch_yf_candle(yf_symbol)
        return jsonify(candle)
    except Exception as e:
        return jsonify({"error":str(e)})

# --- Start Flask ---
if __name__ == "__main__":
    app.run(debug=True, port=5000)
