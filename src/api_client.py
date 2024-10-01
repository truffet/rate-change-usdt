import requests
import logging

class BinanceAPI:
    BASE_URL = 'https://api.binance.com'

    def __init__(self, interval='4h'):
        """Initialize the Binance API client with the desired interval."""
        self.interval = interval
        self.kline_endpoint = '/api/v3/klines'
        self.exchange_info_endpoint = '/api/v3/exchangeInfo'

    def fetch_candlestick_data_by_time(self, symbol, start_time, end_time):
        """
        Fetches candlestick data for a given USDT pair within a specified time range.
        
        Args:
            symbol (str): The trading pair symbol (e.g., 'BTCUSDT').
            start_time (int): Start time in milliseconds (UTC).
            end_time (int): End time in milliseconds (UTC).
        
        Returns:
            list: The candlestick data for the specified time range, or None if the request fails.
        """
        params = {
            'symbol': symbol,
            'interval': self.interval,  # Use the interval directly from the class
            'startTime': start_time,
            'endTime': end_time,
            'limit': 1  # Fetch one candlestick for the specified range
        }

        try:
            response = requests.get(f"{self.BASE_URL}{self.kline_endpoint}", params=params)
            response.raise_for_status()
            candles = response.json()
            logging.info(f"Successfully fetched candlestick data for {symbol} from {start_time} to {end_time}.")
            
            if candles:
                return candles[0]  # Return the first (and only) candle
            else:
                logging.warning(f"No candlestick data found for {symbol} between {start_time} and {end_time}.")
                return None

        except requests.RequestException as e:
            logging.error(f"Error fetching candlestick data for {symbol}: {e}")
            return None

    def get_usdt_pairs(self):
        """
        Fetches all actively traded USDT pairs from Binance.
        
        Returns:
            list: A list of symbols representing all actively traded USDT pairs.
        """
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
