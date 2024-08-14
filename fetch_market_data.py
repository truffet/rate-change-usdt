import requests
import pandas as pd
import os

def get_usdt_pairs():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)
    data = response.json()
    # Filter and return only the symbols with USDT as the quote asset that are spot and actively trading
    usdt_pairs = [
        symbol['symbol'] for symbol in data['symbols']
        if symbol['quoteAsset'] == 'USDT' and symbol['status'] == 'TRADING' and symbol['isSpotTradingAllowed']
    ]
    return usdt_pairs

def fetch_latest_market_data(symbol, interval):
    # Fetch the last two 4-hour candlesticks
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit=2"
    response = requests.get(url)
    data = response.json()
    # Get the second candlestick (latest completed)
    latest_candle = data[0]
    timestamp = latest_candle[0]  # Opening time of the candlestick
    open_price = float(latest_candle[1])  # Opening price
    high_price = float(latest_candle[2])  # Highest price
    low_price = float(latest_candle[3])  # Lowest price
    close_price = float(latest_candle[4])  # Closing price
    volume = float(latest_candle[5])  # Trading volume
    pct_change = ((close_price - open_price) / open_price) * 100  # Percentage change
    return {
        'timestamp': timestamp,
        'symbol': symbol,
        'open': open_price,
        'high': high_price,
        'low': low_price,
        'close': close_price,
        'volume': volume,
        'pct_change': pct_change
    }

def main():
    usdt_pairs = get_usdt_pairs()
    interval = '4h'  # 4-hour interval
    all_data = []

    for pair in usdt_pairs:
        print(f"Fetching latest data for {pair}")  # Console print for debugging
        data = fetch_latest_market_data(pair, interval)
        all_data.append(data)
        print(f"Latest data for {pair} fetched successfully")  # Console print for debugging

    df = pd.DataFrame(all_data)
    if not os.path.exists('data'):
        os.makedirs('data')
    df.to_csv('data/market_data.csv', index=False)
    print("Market data saved to 'data/market_data.csv'")  # Console print for debugging

if __name__ == "__main__":
    main()
