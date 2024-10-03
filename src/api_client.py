import requests
import logging

class BinanceAPI:
    BASE_URL = 'https://api.binance.com'

    def __init__(self, interval='4h'):
        """Initialize the Binance API client with the desired interval."""
        self.interval = interval
        self.kline_endpoint = '/api/v3/klines'
        self.exchange_info_endpoint = '/api/v3/exchangeInfo'

    def get_usdt_pairs(self):
        """Fetches all actively traded USDT pairs from Binance."""
        try:
            response = requests.get(f"{self.BASE_URL}{self.exchange_info_endpoint}")
            response.raise_for_status()  # Raise an exception for 4XX/5XX errors
            symbols = response.json()['symbols']
            usdt_pairs = [
                symbol['symbol'] 
                for symbol in symbols 
                if symbol['quoteAsset'] == 'USDT' and symbol['status'] == 'TRADING'
            ]
            logging.info(f"Successfully fetched {len(usdt_pairs)} USDT pairs.")
            return usdt_pairs
        except requests.RequestException as e:
            logging.error(f"Error fetching USDT pairs from Binance: {e}")
            return []

    def fetch_candlestick_data_by_time(self, symbol, start_time, end_time=None, limit=1000):
        """
        Fetches candlestick data for a given symbol within a specified time range.
        
        Args:
            symbol (str): The trading pair symbol (e.g., 'BTCUSDT').
            start_time (int): Start time in milliseconds.
            end_time (int): End time in milliseconds (optional).
            limit (int): The number of candles to fetch (default is 1000).
        
        Returns:
            list: A list of candlestick data from the Binance API.
        """
        params = {
            'symbol': symbol,
            'interval': self.interval,
            'startTime': start_time,
            'limit': limit  # Fetch up to 'limit' candles in one request
        }
        if end_time:
            params['endTime'] = end_time

        try:
            response = requests.get(f"{self.BASE_URL}{self.kline_endpoint}", params=params)
            response.raise_for_status()
            candles = response.json()
            logging.info(f"Successfully fetched candlestick data for {symbol} from {start_time} to {end_time}.")
            return candles
        except requests.RequestException as e:
            logging.error(f"Error fetching candlestick data for {symbol}: {e}")
            return None
