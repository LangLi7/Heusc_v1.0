import csv
import os
from dotenv import load_dotenv
from binance.client import Client
from datetime import datetime, timedelta

load_dotenv()
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")

client = Client(api_key, api_secret)
coins = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]

# --- Zeitraum automatisch berechnen ---
# HEUTE
end_time = datetime.now()

# Beispiel: 1 Monat zurück
start_time_1m = end_time - timedelta(days=30)

# Beispiel: 3 Monate zurück
start_time_3m = end_time - timedelta(days=90)

# Binance erwartet Strings, also formatieren:
start_time = start_time_3m.strftime("%d %b, %Y")   # hier 3 Monate Beispiel
end_time_str = end_time.strftime("%d %b, %Y")

print("Starte Download von", start_time, "bis", end_time_str)

# --- Daten abrufen & speichern ---
for coin in coins:
    klines = client.get_historical_klines(
        coin,
        Client.KLINE_INTERVAL_1MINUTE,
        start_time,
        end_time_str
    )
    with open(f"{coin}_1m.csv", "w", newline="") as f:
        writer = csv.writer(f)
        columns = [
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ]
        writer.writerow(columns)
        writer.writerows(klines)
