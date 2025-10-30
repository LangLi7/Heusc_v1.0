# live_train_client.py
import requests
import time
import os
import json
import pandas as pd
from colorama import init, Fore, Style
from pathlib import Path
from datetime import datetime

# ----------------------------
# --- Einstellungen aus JSON ---
# ----------------------------
print("Aktuelles Arbeitsverzeichnis:", os.getcwd())
BASE_DIR = os.path.dirname(__file__)
config_path = os.path.join(BASE_DIR, "api_settings.json")

with open(config_path, "r") as f:
    settings = json.load(f)["api_settings"]

API_URL = settings.get("api_url", "http://127.0.0.1:5000")
INTERVAL = settings.get("interval", "1m")
POLL_SECONDS = settings.get("poll_seconds", 10)
CSV_FOLDER = settings.get("csv_folder", "data/")

Path(CSV_FOLDER).mkdir(parents=True, exist_ok=True)

# Terminalfarben initialisieren
init(autoreset=True)

# ----------------------------
# --- Funktionen ---
# ----------------------------
def print_candle(label, candle):
    try:
        o = float(candle['open'])
        c = float(candle['close'])
        h = float(candle['high'])
        l = float(candle['low'])
        v = float(candle.get('volume', 0))
    except KeyError:
        print(f"{Fore.RED}[{label}] Ungültige Candle Daten: {candle}")
        return

    # Open/Close Pfeil
    if c > o:
        arrow = "↑"
        oc_color = Fore.GREEN
    elif c < o:
        arrow = "↓"
        oc_color = Fore.RED
    else:
        arrow = "→"
        oc_color = Fore.YELLOW

    # High/Low Farben
    h_color = Fore.GREEN if h > o else Fore.RED if h < o else Fore.YELLOW
    l_color = Fore.GREEN if l > o else Fore.RED if l < o else Fore.YELLOW

    # Label-Farbe
    lbl_color = Fore.CYAN if label=="[LIVE]" else Fore.MAGENTA if label=="[TRAIN_MODE]" else Fore.WHITE

    print(f"{lbl_color}{label}{Style.RESET_ALL} {candle.get('timestamp','N/A')} | "
          f"{oc_color}{arrow} Open:{o:.2f} Close:{c:.2f}{Style.RESET_ALL} "
          f"{h_color}High:{h:.2f}{Style.RESET_ALL} "
          f"{l_color}Low:{l:.2f}{Style.RESET_ALL} "
          f"Vol:{v:.2f}")

def fetch_data(pair, mode="live", interval=INTERVAL, period=None, source="auto"):
    endpoint = "live" if mode=="live" else "train_mode"
    try:
        # API erwartet: symbols, source, interval
        params = {"symbols": pair, "source": source, "interval": interval}
        if period:
            params["period"] = period
        resp = requests.get(f"{API_URL}/api/{endpoint}", params=params, timeout=15)
        data = resp.json()
        if pair in data:
            return data[pair]
        print(f"[{mode.upper()}] Keine Daten für {pair}: {data}")
        return None
    except Exception as e:
        print(f"[{mode.upper()} ERROR] {pair}: {e}")
        return None

def save_csv(candle_data, pair, interval, mode, source, period=""):
    df = pd.DataFrame(candle_data)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    period_name = period if period else mode
    csv_name = f"{pair}-{interval}-{period_name}-{source}-{timestamp}.csv"
    csv_path = Path(CSV_FOLDER) / csv_name

    # Append bei Live-Mode falls Datei existiert
    if mode=="live":
        existing_files = list(Path(CSV_FOLDER).glob(f"{pair}-{interval}-live-{source}-*.csv"))
        if existing_files:
            csv_path = existing_files[0]
            df.to_csv(csv_path, mode='a', index=False, header=False)
            print(f"[CSV] Appended in {csv_path}")
            return

    df.to_csv(csv_path, index=False)
    print(f"[CSV] Gespeichert: {csv_path}")

# ----------------------------
# --- LIVE MODE ---
# ----------------------------
def live_mode():
    symbol = input("Symbol (z.B. BTC, AAPL): ").strip().upper()
    currency = input("Currency (z.B. USD, EUR): ").strip().upper()
    source = input("Source (binance, yahoo, auto): ").strip().lower()

    # Binance USD -> USDT
    if source=="binance" and currency=="USD" and symbol in ["BTC","ETH","BNB","XRP"]:
        currency = "USDT"
    pair = f"{symbol}-{currency}"
    
    while True:
        candle = fetch_data(pair, mode="live", source=source)
        if candle:
            if "error" in candle:
                print(f"[LIVE] API Error: {candle['error']}")
            else:
                print_candle("[LIVE]", candle)
                save_csv([candle], pair, INTERVAL, "live", source)
        time.sleep(POLL_SECONDS)

# ----------------------------
# --- TRAIN MODE ---
# ----------------------------
def train_mode():
    symbol = input("Symbol (z.B. BTC, AAPL): ").strip().upper()
    currency = input("Currency (z.B. USD, EUR): ").strip().upper()
    source = input("Source (binance, yahoo, auto): ").strip().lower()
    period = input("Zeitraum (z.B. 6M, 1Y, 5d): ").strip()

    if source=="binance" and currency=="USD" and symbol in ["BTC","ETH","BNB","XRP"]:
        currency = "USDT"
    pair = f"{symbol}-{currency}"

    data = fetch_data(pair, mode="train_mode", source=source, period=period)
    if data:
        history = data.get("history", [])
        live = data.get("live", {})
        for c in history[-5:]:
            print_candle("[TRAIN_MODE]", c)
        print_candle("[TRAIN_MODE]", live)
        save_csv(history + [live], pair, INTERVAL, "train", source, period)

# ----------------------------
# --- CSV MODE ---
# ----------------------------
def csv_mode():
    symbol = input("Symbol (z.B. BTC, AAPL): ").strip().upper()
    currency = input("Currency (z.B. USD, EUR): ").strip().upper()
    source = input("Source (binance, yahoo, auto): ").strip().lower()
    interval = input("Interval (z.B. 1m, 1h, 1d): ").strip()
    period = input("Zeitraum (z.B. 6M, 1Y, 5d): ").strip()

    if source=="binance" and currency=="USD" and symbol in ["BTC","ETH","BNB","XRP"]:
        currency = "USDT"
    pair = f"{symbol}-{currency}"

    data = fetch_data(pair, mode="train_mode", interval=interval, period=period, source=source)
    if data:
        history = data.get("history", [])
        live = data.get("live", {})
        save_csv(history + [live], pair, interval, "csv", source, period)

# ----------------------------
# --- MAIN ---
# ----------------------------
if __name__=="__main__":
    mode = input("Modus wählen: (1) live / (2) train_mode / (3) csv_mode : ")
    if mode.strip() == "1":
        print("Starte LIVE Mode...")
        live_mode()
    elif mode.strip() == "2":
        print("Starte TRAIN Mode...")
        train_mode()
    elif mode.strip() == "3":
        print("Starte CSV Mode...")
        csv_mode()
    else:
        print("Ungültige Eingabe.")
