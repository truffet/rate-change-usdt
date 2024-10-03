from datetime import datetime, timedelta
import pandas as pd
import sqlite3
from scipy.stats import zscore
import logging


class DataProcessor:
    def process_usdt_pair_data(self, symbol, candlestick_data):
        """
        Process the candlestick data for a given USDT pair.
        This method calculates rate change and volume in USDT.

        Args:
            symbol (str): The trading pair symbol.
            candlestick_data (list): A single candlestick data (as list).

        Returns:
            dict: Processed candlestick data with calculated fields or None if the input is invalid.
        """
        # Validate the format of candlestick data
        if not isinstance(candlestick_data, list) or len(candlestick_data) < 6:
            logging.error(f"Invalid candlestick data format for {symbol}: {candlestick_data}")
            return None

        try:
            open_time = candlestick_data[0]  # Keep in milliseconds
            open_price = float(candlestick_data[1])
            high_price = float(candlestick_data[2])
            low_price = float(candlestick_data[3])
            close_price = float(candlestick_data[4])
            volume = float(candlestick_data[5])
            quote_volume = float(candlestick_data[7])

            # Calculate rate change
            rate_change = (close_price - open_price) / open_price * 100

            return {
                "symbol": symbol,
                "open_price": open_price,
                "high_price": high_price,
                "low_price": low_price,
                "close_price": close_price,
                "volume": volume,  # Base asset volume
                "quote_volume": quote_volume,  # Quote volume in USDT
                "rate_change": rate_change,
                "open_time": open_time,
                "close_time": candlestick_data[6]  # Close time in milliseconds
            }

        except (IndexError, ValueError) as e:
            logging.error(f"Error processing candlestick data for {symbol}: {e}")
            return None

    def calculate_z_scores_for_pair(self, conn, symbol):
        """
        Calculate Z-scores for a specific trading pair and for all trading pairs.
        Saves the calculated Z-scores into the database.

        Args:
            conn: SQLite connection.
            symbol: The trading pair symbol (e.g., 'BTCUSDT').

        Returns:
            pd.DataFrame: DataFrame with calculated Z-scores.
        """
        query_pair = '''
        SELECT * FROM usdt_4h WHERE symbol = ? 
        '''
        df_pair = pd.read_sql_query(query_pair, conn, params=(symbol,))
        
        query_all = '''
        SELECT * FROM usdt_4h
        '''
        df_all = pd.read_sql_query(query_all, conn)

        df_pair['open_time'] = pd.to_datetime(df_pair['open_time'], unit='ms')
        df_all['open_time'] = pd.to_datetime(df_all['open_time'], unit='ms')

        df_pair['z_rate_change_pair'] = zscore(df_pair['rate_change'])
        df_pair['z_volume_pair'] = zscore(df_pair['volume'])
        df_pair['z_combined_pair'] = df_pair['z_rate_change_pair'] * df_pair['z_volume_pair']

        df_pair['z_rate_change_all_pairs'] = zscore(df_all['rate_change'])
        df_pair['z_volume_all_pairs'] = zscore(df_all['quote_volume'])
        df_pair['z_combined_all_pairs'] = df_pair['z_rate_change_all_pairs'] * df_pair['z_volume_all_pairs']

        return df_pair

    def get_latest_time_in_db(self, conn, symbol):
        """
        Get the latest open_time for a specific symbol from the database.
        
        Args:
            conn: SQLite connection.
            symbol: The trading pair symbol (e.g., 'BTCUSDT').

        Returns:
            int or None: The latest open_time in milliseconds, or None if no data is found.
        """
        query = '''
        SELECT MAX(open_time) FROM usdt_4h WHERE symbol = ?
        '''
        cursor = conn.cursor()
        cursor.execute(query, (symbol,))
        result = cursor.fetchone()
        if result and result[0]:
            return int(result[0])
        return None

    def backfill_missing_data(self, api_client, conn, symbol, start_time, end_time):
        """
        Backfill missing data for the given trading pair.
        Fetch the candlestick data in batches from the start_time to the end_time.
        """
        cursor = conn.cursor()
        
        while start_time < end_time:
            logging.info(f"Fetching candles for {symbol} from {start_time} to {end_time}")
            candles = api_client.fetch_candlestick_data_by_time(symbol, start_time, None, 1000)
            
            if not candles or len(candles) == 0:
                logging.info(f"No more candles to fetch for {symbol}. Breaking the loop.")
                break
            
            for candle in candles:
                if isinstance(candle, list) and len(candle) >= 6:
                    processed_data = self.process_usdt_pair_data(symbol, candle)
                    if processed_data:
                        self.save_candlestick_data_to_db([processed_data], conn)
                else:
                    logging.error(f"Unexpected candlestick format for {symbol}: {candle}")
            
            start_time = candles[-1][6]  # Close time of the last candle in the response

        conn.commit()
        logging.info(f"Backfill completed for {symbol}.")

    def save_candlestick_data_to_db(self, data, conn):
        """
        Save the candlestick data to the SQLite database, including the Z-scores.

        Args:
            data (list): List of candlestick data to be saved.
            conn: SQLite connection.
        """
        cursor = conn.cursor()

        for row in data:
            open_time_ms = int(row['open_time']) if isinstance(row['open_time'], int) else int(pd.Timestamp(row['open_time']).timestamp() * 1000)
            close_time_ms = int(row['close_time']) if isinstance(row['close_time'], int) else int(pd.Timestamp(row['close_time']).timestamp() * 1000)

            cursor.execute('''
            INSERT INTO usdt_4h 
            (symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, quote_volume, rate_change, 
            z_rate_change_pair, z_volume_pair, z_combined_pair, z_rate_change_all_pairs, z_volume_all_pairs, z_combined_all_pairs)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['symbol'], open_time_ms, close_time_ms,
                row['open_price'], row['high_price'], row['low_price'], row['close_price'],
                row['volume'], row['quote_volume'], row['rate_change'],
                row.get('z_rate_change_pair', None), row.get('z_volume_pair', None),
                row.get('z_combined_pair', None), row.get('z_rate_change_all_pairs', None),
                row.get('z_volume_all_pairs', None), row.get('z_combined_all_pairs', None)
            ))

        conn.commit()
        logging.info(f"Data saved to the database successfully.")
