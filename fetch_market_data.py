import requests
import pandas as pd
import os

def get_usdt_pairs():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)
    data = response.json()
    # Filter and return only the symbols with USDT as the quote asset
    usdt_pairs = [symbol['symbol'] for symbol in data['symbols'] if symbol['quoteAsset'] == 'USDT']
    return usdt_pairs

def fetch_market_data(symbol, interval):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}"
    response = requests.get(url)
    data = response.json()
    market_data = []
    for candle in data:
        timestamp = candle[0]  # Opening time of the candlestick
        open_price = float(candle[1])  # Opening price
        high_price = float(candle[2])  # Highest price
        low_price = float(candle[3])  # Lowest price
        close_price = float(candle[4])  # Closing price
        volume = float(candle[5])  # Trading volume
        pct_change = ((close_price - open_price) / open_price) * 100  # Percentage change
        # Append the data to the market_data list
        market_data.append({
            'timestamp': timestamp,
            'symbol': symbol,
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': volume,
            'pct_change': pct_change
        })
    return market_data

def main():
    usdt_pairs = get_usdt_pairs()
    interval = '1d'  # Example interval; you can change this as needed
    all_data = []

    for pair in usdt_pairs:
        print(f"Fetching data for {pair}")  # Console print for debugging
        data = fetch_market_data(pair, interval)
        all_data.extend(data)
        print(f"Data for {pair} fetched successfully")  # Console print for debugging

    df = pd.DataFrame(all_data)
    if not os.path.exists('data'):
        os.makedirs('data')
    df.to_csv('data/market_data.csv', index=False)
    print("Market data saved to 'data/market_data.csv'")  # Console print for debugging

if __name__ == "__main__":
    main()
