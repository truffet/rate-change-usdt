import requests
import logging

class BinanceAPI:
    BASE_URL = 'https://api.binance.com'

    def __init__(self, interval='4h'):
        """Initialize the Binance API client with the desired interval."""
        self.interval = interval
        self.kline_endpoint = '/api/v3/klines'
        self.exchange_info_endpoint = '/api/v3/exchangeInfo'

    def interval_to_ms(self):
        """Helper function to convert the interval to milliseconds."""
        if self.interval == '4h':
            return 4 * 60 * 60 * 1000
        else:
            raise ValueError("Unsupported interval")

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

    def calculate_candles_to_fetch(self, start_time, end_time):
        """
        Calculate the total number of candles to fetch between start_time and end_time.
        """
        # Define the interval in milliseconds for 4h, daily (1d), and weekly (1w)
        interval_mapping = {
            '4h': 4 * 60 * 60 * 1000,  # 4 hours in milliseconds
            'd': 24 * 60 * 60 * 1000,  # 1 day in milliseconds
            'w': 7 * 24 * 60 * 60 * 1000  # 1 week in milliseconds
        }

        # Get the duration of the interval in milliseconds based on self.interval
        interval_duration = interval_mapping.get(self.interval)

        # Calculate the total number of candles to fetch
        total_candles = (end_time - start_time) // interval_duration

        return total_candles

    def fetch_candle_data(self, symbol, start_time, end_time, limit=1000):
        """
        Fetches candlestick data for a given symbol within a specified time range.
        
        Args:
            symbol (str): The trading pair symbol (e.g., 'BTCUSDT').
            start_time (int): Start time in milliseconds.
            end_time (int): End time in milliseconds.
            limit (int): The number of candles to fetch per batch (default is 1000).
    
        Returns:
            list: A list of candlestick data from the Binance API.
        """
        all_candles = []  # List to store all fetched candles
        
        # Calculate the total number of candles to fetch
        total_candles = self.calculate_candles_to_fetch(start_time, end_time)
        logging.info(f"Total candles to fetch for {symbol}: {total_candles}")

        # Keep track of how many candles we've fetched
        candles_fetched = 0

        # Loop until we've fetched the total number of candles
        while candles_fetched < total_candles:
            remaining_candles = total_candles - candles_fetched
            fetch_limit = min(remaining_candles, limit)

            params = {
                'symbol': symbol,
                'interval': self.interval,
                'startTime': start_time,
                'limit': fetch_limit  # Fetch up to 'limit' candles per batch
            }

            try:
                response = requests.get(f"{self.BASE_URL}{self.kline_endpoint}", params=params)
                response.raise_for_status()
                candles = response.json()
            
                if not candles:
                    logging.info(f"No more candles to fetch for {symbol} from {start_time} to {end_time}.")
                    break

                all_candles.extend(candles)
                logging.info(f"Fetched {len(candles)} candles for {symbol} from {start_time}.")

                # Update the number of candles fetched
                candles_fetched += len(candles)

                # Update the cursor to the last candle's close time plus 1 millisecond
                start_time = int(candles[-1][6]) + 1

            except requests.RequestException as e:
                logging.error(f"Error fetching candlestick data for {symbol}: {e}")
                break

        return all_candles

