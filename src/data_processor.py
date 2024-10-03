from datetime import datetime, timedelta
import pandas as pd
import sqlite3
from scipy.stats import zscore

class DataProcessor:

    def process_usdt_pair_data(self, symbol, candlestick_data):
        """
        Process the candlestick data for a given USDT pair.
        This method calculates rate change and volume in USDT.
        
        Args:
            symbol (str): The trading pair symbol.
            candlestick_data (list): The candlestick data from the Binance API.
        
        Returns:
            dict: Processed candlestick data with calculated fields.
        """
        if not candlestick_data:
            return None

        open_time = candlestick_data[0]  # Already in milliseconds
        close_time = candlestick_data[6]  # Already in milliseconds
        
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
            "close_time": close_time
        }

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
        # Fetch historical data for the specific trading pair
        query_pair = '''
        SELECT * FROM usdt_4h 
        WHERE symbol = ? 
        '''
        df_pair = pd.read_sql_query(query_pair, conn, params=(symbol,))
        
        # Fetch historical data for all trading pairs
        query_all = '''
        SELECT * FROM usdt_4h
        '''
        df_all = pd.read_sql_query(query_all, conn)

        # Ensure timestamps are properly parsed
        df_pair['open_time'] = pd.to_datetime(df_pair['open_time'], unit='ms')
        df_all['open_time'] = pd.to_datetime(df_all['open_time'], unit='ms')

        # Calculate Z-scores for the specific trading pair
        df_pair['z_rate_change_pair'] = zscore(df_pair['rate_change'])
        df_pair['z_volume_pair'] = zscore(df_pair['volume'])  # Base asset volume for pair-specific z-score
        df_pair['z_combined_pair'] = df_pair['z_rate_change_pair'] * df_pair['z_volume_pair']

        # Calculate Z-scores for all trading pairs
        df_pair['z_rate_change_all_pairs'] = zscore(df_all['rate_change'])  # Use all pairs' rate change for Z-score
        df_pair['z_volume_all_pairs'] = zscore(df_all['quote_volume'])  # Quote volume for all-pair Z-score
        df_pair['z_combined_all_pairs'] = df_pair['z_rate_change_all_pairs'] * df_pair['z_volume_all_pairs']

        return df_pair

    def get_latest_time_in_db(self, conn, symbol):
        """
        Fetch the latest timestamp of the data in the database for a given symbol.
        This is used to determine where to start backfilling.

        Args:
            conn: SQLite connection.
            symbol: The trading pair symbol (e.g., 'BTCUSDT').

        Returns:
            int: The latest open_time in milliseconds.
        """
        query = '''
        SELECT MAX(open_time) FROM usdt_4h WHERE symbol = ?
        '''
        cursor = conn.cursor()
        cursor.execute(query, (symbol,))
        latest_time = cursor.fetchone()[0]

        if latest_time is not None:
            return int(latest_time)
        return None

    def backfill_missing_data(self, api_client, conn, symbol, start_time, end_time):
        """
        Backfill missing data for the given trading pair.
        Fetches candles from Binance in batches (up to 1000 candles) and inserts them into the database.
        
        Args:
            api_client: Binance API client.
            conn: SQLite connection.
            symbol: The trading pair symbol (e.g., 'BTCUSDT').
            start_time: The start time in milliseconds to backfill from.
            end_time: The end time in milliseconds (most recent candle).
        """
        cursor = conn.cursor()

        while start_time < end_time:
            # Calculate the time range to fetch (up to 1000 candles at 4-hour intervals)
            time_diff = end_time - start_time
            total_candles = time_diff // (4 * 3600 * 1000)  # Number of 4-hour candles
            limit = min(1000, total_candles)

            # If the limit is 0, it means there's no data to fetch, so we break the loop
            if limit <= 0:
                print(f"No more candles to fetch for {symbol}. Breaking the loop.")
                break

            candlestick_data = api_client.fetch_candlestick_data_by_time(symbol, start_time, end_time, limit)

            if not candlestick_data:
                print(f"No data returned for {symbol}. Breaking the loop.")
                break

            # Process and save the batch of backfilled data
            processed_data = [self.process_usdt_pair_data(symbol, candle) for candle in candlestick_data]
            processed_data = [data for data in processed_data if data]  # Filter out None values
            if processed_data:
                self.save_candlestick_data_to_db(processed_data, conn)

            # Move the start_time forward by the number of candles we just fetched
            start_time = processed_data[-1]['close_time']  # Set the start_time to the last candle's close time

            # Safety check: If somehow the start_time exceeds the end_time, we stop the loop
            if start_time >= end_time:
                print(f"Reached the end time for {symbol}. Ending the backfill.")
                break

        conn.commit()
        print(f"Backfill completed for {symbol}.")

    def save_candlestick_data_to_db(self, data, conn):
        """
        Save the candlestick data to the SQLite database, including the Z-scores.
        
        Args:
            data (list): List of candlestick data to be saved.
            conn: SQLite connection.
        """
        cursor = conn.cursor()

        for row in data:
            if None in (row['open_price'], row['high_price'], row['low_price'], row['close_price']):
                print(f"Skipping row due to missing price data: {row}")
                continue

            # Print statements to debug
            print(f"Before conversion: open_time: {row['open_time']} ({type(row['open_time'])}), close_time: {row['close_time']} ({type(row['close_time'])})")

            # Ensure that open_time and close_time are in milliseconds
            open_time_ms = int(row['open_time']) if isinstance(row['open_time'], int) else int(pd.Timestamp(row['open_time']).timestamp() * 1000)
            close_time_ms = int(row['close_time']) if isinstance(row['close_time'], int) else int(pd.Timestamp(row['close_time']).timestamp() * 1000)

            # Check if the data for this symbol and open_time already exists
            cursor.execute('''
            SELECT COUNT(*) FROM usdt_4h WHERE symbol = ? AND open_time = ?
            ''', (row['symbol'], open_time_ms))
            exists = cursor.fetchone()[0]

            if exists:
                print(f"Skipping insert for {row['symbol']} at {open_time_ms}, data already exists.")
                continue  # Skip this row if it already exists

            # Insert the new data into the database
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
        print(f"Data saved to the database successfully.")
