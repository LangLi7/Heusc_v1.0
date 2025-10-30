# live_loop_api_full.py
from flask import Flask, request, jsonify
import asyncio
import datetime
import os
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from binance.client import Client
import yfinance as yf
from dotenv import load_dotenv
from threading import Thread
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
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # optional, z.B. Trading-Bot Endpoint

# --- Helper Functions ---
def save_candle_to_csv(symbol, source, price_data):
    """
    Speichert die Candle als CSV: SYMBOL_SOURCE_DATUM_UHR.csv
    """
    now = datetime.datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H-%M")  # ohne Sekunden
    filename = f"{CSV_FOLDER}/{symbol}_{source}_{date_str}_{time_str}.csv"

    fieldnames = list(price_data.keys())
    with open(filename, mode="w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(price_data)

def fetch_binance_candle(symbol):
    """
    Holt die letzte Minute Candle von Binance inkl. Farbe
    """
    klines = binance_client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE, limit=2)
    prev_close = float(klines[0][4])
    current_close = float(klines[1][4])
    color = "green" if current_close > prev_close else "red"
    candle_data = {
        "symbol": symbol,
        "prev_close": prev_close,
        "current_close": current_close,
        "color": color
    }
    # CSV speichern
    save_candle_to_csv(symbol, "binance", candle_data)
    return candle_data

async def fetch_yf_candle(symbol, interval="1m"):
    """
    Holt die letzte Minute Candle von Yahoo Finance inkl. Farbe
    """
    def get_candle():
        # Symbol anpassen
        sym = symbol.replace("USDT","-USD") if "USDT" in symbol else symbol
        df = yf.Ticker(sym).history(period="2d", interval=interval)
        if len(df) < 2:
            raise ValueError("Nicht genug Daten für Candle")
        prev_close = df["Close"].iloc[-2]
        current_close = df["Close"].iloc[-1]
        color = "green" if current_close > prev_close else "red"
        candle_data = {
            "symbol": sym,
            "prev_close": prev_close,
            "current_close": current_close,
            "color": color
        }
        # CSV speichern
        save_candle_to_csv(sym, "yahoo", candle_data)
        return candle_data
    return await asyncio.to_thread(get_candle)

async def push_webhook(data):
    if not WEBHOOK_URL:
        return
    async with aiohttp.ClientSession() as session:
        try:
            await session.post(WEBHOOK_URL, json=data)
        except Exception as e:
            print("Webhook Fehler:", e)

# --- Live Loop ---
async def live_loop(symbols, source="binance", interval="1m"):
    while True:
        tasks = []
        for symbol in symbols:
            if source == "binance":
                tasks.append(asyncio.to_thread(fetch_binance_candle, symbol))
            elif source == "yahoo":
                tasks.append(fetch_yf_candle(symbol, interval))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        output = {}
        for sym, res in zip(symbols, results):
            if isinstance(res, Exception):
                output[sym] = {"error": str(res)}
            else:
                output[sym] = res

        # Print / Debug
        print(datetime.datetime.now(), output)

        # Optional: Webhook Push
        await push_webhook(output)

        await asyncio.sleep(1)  # jede Sekunde

# --- Thread Helper ---
def start_live_loop_thread(symbols, source, interval):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(live_loop(symbols, source, interval))

# --- API Endpoint ---
@app.route("/api/live_loop")
def api_live_loop():
    symbols = request.args.get("symbols")
    source = request.args.get("source", "binance").lower()
    interval = request.args.get("interval", "1m")

    if not symbols:
        return jsonify({"error": "Bitte Parameter symbols angeben (Komma getrennt)"}), 400

    symbols = symbols.split(",")

    # Live Loop in separatem Thread starten
    thread = Thread(target=start_live_loop_thread, args=(symbols, source, interval), daemon=True)
    thread.start()

    return jsonify({"status": "Live Loop gestartet", "symbols": symbols, "source": source})

# --- Start Flask ---
if __name__ == "__main__":
    app.run(debug=True, port=5000)
