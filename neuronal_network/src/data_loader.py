import pandas as pd

def load_csv(symbol: str, interval: str, period: str):
    """
    Lädt Candles aus CSV
    """
    file_path = f"data/{symbol}-{interval}-{period}.csv"
    df = pd.read_csv(file_path, parse_dates=["timestamp"])
    return df

df = load_csv("BTC-USD", "1m", "7d")
