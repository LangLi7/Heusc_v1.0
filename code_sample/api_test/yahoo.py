import os
import json
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import pytz

# ----------------------------
# --- Einstellungen aus JSON ---
# ----------------------------
BASE_DIR = os.path.dirname(__file__)
config_path = os.path.join(BASE_DIR, "settings.json")

with open(config_path, "r") as f:
    settings = json.load(f)["settings"]

DATA_STORE = {}  # symbol -> DataFrame

def get_csv_path(symbol):
    folder = settings["data_folder"]
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, f"{symbol}-{settings['interval']}-{settings['period']}.csv")

def fetch_yahoo_chunks(symbol, start, end, interval):
    """Daten in 7-Tage-Chunks abrufen, Zeitzonen berücksichtigen"""
    ticker = yf.Ticker(symbol)
    chunks = []

    # Zeitzone festlegen
    tz = pytz.UTC if "-" in symbol else pytz.timezone("America/New_York")

    # Start/End tz-aware machen
    if start.tzinfo is None:
        start = tz.localize(start)
    if end.tzinfo is None:
        end = tz.localize(end)

    while start < end:
        chunk_end = min(start + timedelta(days=7), end)
        chunk_data = ticker.history(start=start, end=chunk_end, interval=interval, actions=True)
        if chunk_data.empty:
            start = chunk_end
            continue
        if chunk_data.index.tz is None:
            chunk_data.index = chunk_data.index.tz_localize(tz)
        else:
            chunk_data.index = chunk_data.index.tz_convert(tz)
        chunk_data["timestamp"] = chunk_data.index
        chunks.append(chunk_data)
        start = chunk_end

    if chunks:
        df = pd.concat(chunks)
        df = df[~df.index.duplicated(keep="last")]
        df = df[["Open","High","Low","Close","Volume","Dividends","Stock Splits","timestamp"]]
        return df
    else:
        return pd.DataFrame(columns=["Open","High","Low","Close","Volume","Dividends","Stock Splits","timestamp"])

def load_initial(symbol):
    """CSV laden oder initial von Yahoo holen (silent)"""
    file_path = get_csv_path(symbol)
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, parse_dates=["timestamp"])
        df.set_index("timestamp", inplace=True)
    else:
        end = datetime.now(pytz.UTC if "-" in symbol else pytz.timezone("America/New_York"))
        start = end - timedelta(days=7)
        df = fetch_yahoo_chunks(symbol, start, end, settings["interval"])
        if not df.empty:
            df.to_csv(file_path, index=False)
            df.set_index("timestamp", inplace=True)
    DATA_STORE[symbol] = df

def append_live(symbol):
    """Neue Yahoo Daten anhängen (silent)"""
    df = DATA_STORE.get(symbol)
    if df is None:
        load_initial(symbol)
        df = DATA_STORE.get(symbol)
        if df is None:
            return

    end = datetime.now(pytz.UTC if "-" in symbol else pytz.timezone("America/New_York"))
    start = df.index.max()
    if start.tzinfo is None:
        start = start.tz_localize(pytz.UTC if "-" in symbol else pytz.timezone("America/New_York"))
    new_data = fetch_yahoo_chunks(symbol, start, end, settings["interval"])
    if new_data.empty:
        return

    df = pd.concat([df, new_data])
    df = df[~df.index.duplicated(keep="last")]
    DATA_STORE[symbol] = df
    df.to_csv(get_csv_path(symbol), index=False)

# --- Alle Symbole initial laden ---
for symbol in settings["symbols"]:
    load_initial(symbol)
    append_live(symbol)
