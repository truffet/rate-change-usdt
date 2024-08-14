import requests

def fetch_latest_market_data(symbol, interval):
    # Fetch the last two 4-hour candlesticks
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit=2"
    response = requests.get(url)
    data = response.json()
    # Get the second-to-last candlestick (latest completed)
    latest_candle = data[0]
    timestamp = latest_candle[0]  # Opening time of the candlestick
    open_price = float(latest_candle[1])  # Opening price
    high_price = float(latest_candle[2])  # Highest price
    low_price = float(latest_candle[3])  # Lowest price
    close_price = float(latest_candle[4])  # Closing price
    volume = float(latest_candle[5])  # Trading volume
    return {
        'timestamp': timestamp,
        'symbol': symbol,
        'open': open_price,
        'high': high_price,
        'low': low_price,
        'close': close_price,
        'volume': volume
    }
