import logging
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

        open_time = candlestick_data[0][0]  # Keep in milliseconds
        close_time = candlestick_data[-1][6]  # Keep in milliseconds
        
        open_price = float(candlestick_data[0][1])
        high_price = float(candlestick_data[0][2])
        low_price = float(candlestick_data[0][3])
        close_price = float(candlestick_data[0][4])
        volume = float(candlestick_data[0][5])
        quote_volume = float(candlestick_data[0][7])

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

    def check_and_clean_data(self, conn, symbol, latest_binance_timestamp):
        """
        Check for missing 4-hour intervals for the given trading pair.
        Also, remove data older than one year from the database.
        
        Args:
            conn: SQLite connection.
            symbol: The trading pair symbol (e.g., 'BTCUSDT').
            latest_binance_timestamp: Timestamp of the most recent 4-hour candlestick from Binance.
        
        Returns:
            List of missing time intervals (start_time, end_time) tuples.
        """
        query = '''
        SELECT open_time FROM usdt_4h 
        WHERE symbol = ? 
        ORDER BY open_time DESC
        '''
        df = pd.read_sql_query(query, conn, params=(symbol,))
<<<<<<< HEAD

        # Convert open_time from milliseconds to datetime
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')  # Ensure conversion from milliseconds

        # Calculate the timestamp 1 year before the latest Binance timestamp
        one_year_ago = datetime.utcfromtimestamp(latest_binance_timestamp / 1000) - timedelta(days=365)

        # Delete data older than one year
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM usdt_4h 
            WHERE symbol = ? 
            AND open_time < ?
        ''', (symbol, one_year_ago.strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        print(f"Deleted data older than one year for {symbol}.")

        # Identify missing intervals by navigating backwards from the latest timestamp
        missing_intervals = []
        current_time = datetime.utcfromtimestamp(latest_binance_timestamp / 1000)

        while current_time > one_year_ago:
            if current_time not in df['open_time'].values:
                start_time = int(current_time.timestamp() * 1000)
                end_time = int((current_time + timedelta(hours=4)).timestamp() * 1000)
                missing_intervals.append((start_time, end_time))
            
            # Go back 4 hours
            current_time -= timedelta(hours=4)

        return missing_intervals
=======

        # Convert open_time from milliseconds to datetime
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')  # Ensure conversion from milliseconds

        # Calculate the timestamp 1 year before the latest Binance timestamp
        one_year_ago = datetime.utcfromtimestamp(latest_binance_timestamp / 1000) - timedelta(days=365)

        # Delete data older than one year
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM usdt_4h 
            WHERE symbol = ? 
            AND open_time < ?
        ''', (symbol, one_year_ago.strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        print(f"Deleted data older than one year for {symbol}.")

        # Identify missing intervals by navigating backwards from the latest timestamp
        missing_intervals = []
        current_time = datetime.utcfromtimestamp(latest_binance_timestamp / 1000)
>>>>>>> parent of 1d2bffb (update before handling timestamp issue)

        while current_time > one_year_ago:
            if current_time not in df['open_time'].values:
                start_time = int(current_time.timestamp() * 1000)
                end_time = int((current_time + timedelta(hours=4)).timestamp() * 1000)
                missing_intervals.append((start_time, end_time))
            
            # Go back 4 hours
            current_time -= timedelta(hours=4)

        return missing_intervals

    def backfill_missing_data(self, api_client, conn, symbol, missing_intervals):
        """
        Backfill missing data for the given trading pair.
        This function utilizes Binance's 1000 candle limit to fetch data more efficiently.
        
        Args:
            api_client: Binance API client.
            conn: SQLite connection.
            symbol: The trading pair symbol (e.g., 'BTCUSDT').
<<<<<<< HEAD
            start_time: The timestamp to start backfilling from.
            end_time: The timestamp to end backfilling at.
        """
        cursor = conn.cursor()

        while start_time < end_time:
            missing_hours = (end_time - start_time) // (1000 * 3600)  # Calculate the hours between timestamps
            total_candles = missing_hours // 4  # Number of 4-hour candlesticks

            # Stop if no more data to fetch
            if total_candles <= 0:
                logging.info(f"No more candles to fetch for {symbol}. Breaking the loop.")
                break

            # Fetch data in chunks of 1000 candles (Binance's limit)
            fetch_limit = min(1000, total_candles)
            candlestick_data = api_client.fetch_candlestick_data_by_time(symbol, start_time, None, fetch_limit)

            if candlestick_data:
                processed_data = [self.process_usdt_pair_data(symbol, [candle]) for candle in candlestick_data]
                processed_data = [data for data in processed_data if data]  # Filter out None values
                if processed_data:
                    self.save_candlestick_data_to_db(processed_data, conn)

            start_time += fetch_limit * 4 * 3600 * 1000  # Move the start_time forward by the number of candles fetched
=======
            missing_intervals: List of (start_time, end_time) tuples to backfill.
        """
        cursor = conn.cursor()

        if missing_intervals:
            start_time = missing_intervals[0][0]
            end_time = missing_intervals[-1][1]
            
            # Calculate the number of missing candles (we assume 4-hour candles)
            missing_hours = (end_time - start_time) // (1000 * 3600)  # Convert to hours
            total_candles = missing_hours // 4  # Number of 4-hour candlesticks

            # Fetch data in batches up to Binance's 1000 candle limit
            while total_candles > 0:
                fetch_limit = min(1000, total_candles)
                candlestick_data = api_client.fetch_candlestick_data_by_time(symbol, start_time, end_time=None, limit=fetch_limit)

                if candlestick_data:
                    # Process and save the batch of backfilled data
                    processed_data = [self.process_usdt_pair_data(symbol, [candle]) for candle in candlestick_data]
                    processed_data = [data for data in processed_data if data]  # Filter out None values
                    if processed_data:
                        self.save_candlestick_data_to_db(processed_data, conn)

                # Move the start_time forward by the number of candles we just fetched
                start_time += fetch_limit * 4 * 3600 * 1000  # Move forward by 4 hours per candle in milliseconds
                total_candles -= fetch_limit
>>>>>>> parent of 1d2bffb (update before handling timestamp issue)

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

<<<<<<< HEAD
            # Print statement for debugging
            print(f"Before conversion: open_time: {row['open_time']} ({type(row['open_time'])}), close_time: {row['close_time']} ({type(row['close_time'])})")

=======
>>>>>>> parent of 1d2bffb (update before handling timestamp issue)
            # Ensure that open_time and close_time are in milliseconds
            open_time_ms = row['open_time'] if isinstance(row['open_time'], int) else int(row['open_time'].timestamp() * 1000)
            close_time_ms = row['close_time'] if isinstance(row['close_time'], int) else int(row['close_time'].timestamp() * 1000)

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

    def get_latest_time_in_db(self, conn, symbol):
        """
        Retrieve the most recent open_time for the given trading pair from the database.

        Args:
            conn: SQLite connection.
            symbol (str): The trading pair symbol (e.g., 'BTCUSDT').

        Returns:
            int: The most recent open_time in milliseconds for the given symbol.
        """
        cursor = conn.cursor()
        cursor.execute('''
            SELECT MAX(open_time) FROM usdt_4h WHERE symbol = ?
        ''', (symbol,))
        
        result = cursor.fetchone()
        
        # If no data is found, return None
        if result is None or result[0] is None:
            return None
        
        return int(result[0])
