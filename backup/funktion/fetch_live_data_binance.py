import time
import os
from dotenv import load_dotenv
from binance.client import Client

load_dotenv()
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")

client = Client(api_key, api_secret)
coins = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]

def fetch_live_prices():
    prices = {}
    for coin in coins:
        ticker = client.get_symbol_ticker(symbol=coin)
        prices[coin] = float(ticker["price"])
    return prices

def fetch_current_and_previous(coin):
    # 2 letzte 1-Minuten-Kerzen holen
    klines = client.get_klines(symbol=coin, interval=Client.KLINE_INTERVAL_1MINUTE, limit=2)
    
    # Vorletzte = 1 Minute vorher
    prev_close = float(klines[0][4])   # Index 4 = "close"
    
    # Letzte = aktuelle Minute (noch nicht abgeschlossen!)
    current_close = float(klines[1][4])
    
    return {
        "coin": coin,
        "prev_close": prev_close,
        "current_close": current_close
    }

# Endlosschleife -> alle Sekunden aktuelle Preise abholen

#while True:
#   live = fetch_live_prices()
#   print(live)  # hier könntest du weiterverarbeiten statt nur print
#   time.sleep(1)  # 1 Sekunde warten

# Beispiel: Live-Loop
while True:
    for coin in coins:
        data = fetch_current_and_previous(coin)
        print(f"{data['coin']} | Vor 1 Min: {data['prev_close']} | Jetzt: {data['current_close']}")
    time.sleep(5)  # alle 5 Sekunden abfragen