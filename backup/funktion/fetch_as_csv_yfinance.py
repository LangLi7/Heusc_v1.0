import yfinance as yf
import datetime

# Coins / Assets (Yahoo nutzt Tickersymbole)
symbols = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"]

# Zeitraum: 3 Monate zurück bis heute
end = datetime.datetime.now()
start = end - datetime.timedelta(days=90)

for sym in symbols:
    data = yf.download(sym, start=start, end=end, interval="1m")  # 1 Minute Interval
    data.to_csv(f"{sym}_1m.csv")  # CSV speichern
    print(f"{sym} gespeichert mit {len(data)} Zeilen")
