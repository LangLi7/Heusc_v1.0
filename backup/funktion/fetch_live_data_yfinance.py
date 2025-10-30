import yfinance as yf
import time

symbols = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"]

def fetch_live_prices():
    prices = {}
    for sym in symbols:
        ticker = yf.Ticker(sym)
        prices[sym] = ticker.history(period="1d", interval="1m")["Close"].iloc[-1]
    return prices

# Live-Loop: alle 5 Sekunden
while True:
    live = fetch_live_prices()
    print(live)
    time.sleep(5)
