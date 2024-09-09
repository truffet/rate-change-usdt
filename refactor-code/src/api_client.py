# src/api_client.py

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

    def fetch_last_completed_candlestick(self, symbol):
        """
        Fetches the most recent completed candlestick for a given USDT pair.
        
        Args:
            symbol (str): The trading pair symbol (e.g., 'BTCUSDT').
        
        Returns:
            list: The most recent completed candlestick data, or None if the request fails.
        """
        params = {
            'symbol': symbol,
            'interval': self.interval,
            'limit': 2  # Fetching the last two candlesticks to ensure we get the completed one
        }
        try:
            response = requests.get(f"{self.BASE_URL}{self.kline_endpoint}", params=params)
            response.raise_for_status()
            candles = response.json()
            logging.info(f"Successfully fetched candlestick data for {symbol}.")

            # The second to last candlestick is the last completed one
            if len(candles) < 2:
                return None  # Not enough candlestick data to calculate

            last_completed_candle = candles[-2]  # Second to last entry is the last completed candle
            return last_completed_candle
        except requests.RequestException as e:
            logging.error(f"Error fetching candlestick data for {symbol}: {e}")
            return None
