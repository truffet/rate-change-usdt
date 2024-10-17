import sqlite3
import pandas as pd
from datetime import timedelta, datetime, timezone
import logging

class DatabaseHandler:
    def get_timestamp_cursor(self, conn, timeframe, symbol):
        """
        Fetch the most recent close_time from the database for a specific symbol and return the next timestamp
        (incremented by 1 ms) as the starting point for backfilling data.
        If no data is found, return a timestamp 1 year back from the current time in UTC.
        """
        # Concatenate the timeframe to create the table name
        table = f'usdt_{timeframe}'

        # Determine the increment based on the timeframe
        if timeframe == '4h':
            increment = timedelta(hours=4)
        elif timeframe == 'd':
            increment = timedelta(days=1)
        elif timeframe == 'w':
            increment = timedelta(weeks=1)
        else:
            raise ValueError("Invalid timeframe specified.")

        # Fetch the most recent close_time for the specified symbol
        query = f"SELECT MAX(close_time) FROM {table} WHERE symbol = ?"
        cursor = conn.cursor()
        cursor.execute(query, (symbol,))
        last_close_time = cursor.fetchone()[0]

        # If no data, return max_backfill timestamp rounded to the timeframe in UTC
        if last_close_time is None:
            max_backfill = datetime.now(timezone.utc) - timedelta(days=365)
            logging.warning(f"No data found in {table} for {symbol}. Returning max backfill timestamp.")
            rounded_backfill = self._round_timestamp_to_timeframe(max_backfill, timeframe)
            return int(rounded_backfill.timestamp() * 1000)

        # Convert the last close_time to a datetime object and return the next timestamp (increment by 1 ms)
        last_close_time_datetime = pd.to_datetime(last_close_time, unit='ms')
        next_timestamp = last_close_time_datetime + timedelta(milliseconds=1)

        return int(next_timestamp.timestamp() * 1000)

    def _round_timestamp_to_timeframe(self, timestamp, timeframe):
        """
        Round the given timestamp (datetime) to the nearest lower interval (4h, d, or w).
        """
        timestamp = timestamp.astimezone(timezone.utc)  # Ensure UTC
        if timeframe == '4h':
            rounded_time = timestamp.replace(minute=0, second=0, microsecond=0)
            rounded_time -= timedelta(hours=timestamp.hour % 4)
        elif timeframe == 'd':
            rounded_time = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        elif timeframe == 'w':
            rounded_time = timestamp - timedelta(days=timestamp.weekday())
            rounded_time = rounded_time.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            raise ValueError("Invalid timeframe specified for rounding.")
        return rounded_time

    def save_symbol_data_to_db(self, df, conn, timeframe, symbol):
        """
        Save the processed candlestick data from a pandas DataFrame to the database.
        
        Args:
            df (pd.DataFrame): The DataFrame containing the candlestick data to save.
            conn (sqlite3.Connection): The SQLite connection object.
            timeframe (str): The timeframe ('4h', 'd', or 'w') to determine the table name.
        """
        # Determine the appropriate table based on the timeframe
        table_name = f'usdt_{timeframe}'

        cursor = conn.cursor()

        for _, row in df.iterrows():
            # Prepare the data to be inserted or updated
            data_to_insert = (
                symbol,                                 # Trading pair symbol
                row['open_time'],                       # Open time (start of the candlestick)
                row['close_time'],                      # Close time (end of the candlestick)
                row['open_price'],                      # Opening price
                row['high_price'],                      # Highest price during the candlestick
                row['low_price'],                       # Lowest price during the candlestick
                row['close_price'],                     # Closing price
                row['volume'],                          # Volume traded (base asset)
                row['quote_volume'],                    # Volume traded (quote asset)

                # Rate change calculations
                row['rate_change_open_close'],          # Rate change based on open/close prices
                row['rate_change_high_low'],            # Rate change based on high/low prices

                # Z-scores specific to this trading pair (calculated later)
                row.get('z_rate_change_open_close', None),
                row.get('z_rate_change_high_low', None),
                row.get('z_volume_pair', None),

                # Cross-pair Z-scores (calculated after z-scores for individual pairs)
                row.get('z_rate_change_open_close_all_pairs', None),
                row.get('z_rate_change_high_low_all_pairs', None),
                row.get('z_volume_all_pairs', None)
            )

            # SQL query to insert or replace existing data into the appropriate table
            query = f'''
            INSERT OR REPLACE INTO {table_name} (
                symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, quote_volume, 
                rate_change_open_close, rate_change_high_low, 
                z_rate_change_open_close, z_rate_change_high_low, z_volume_pair, 
                z_rate_change_open_close_all_pairs, z_rate_change_high_low_all_pairs, z_volume_all_pairs
            ) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''

            try:
                # Execute the query
                cursor.execute(query, data_to_insert)
            except Exception as e:
                logging.error(f"Error saving candle data for {row['symbol']} at {row['open_time']}: {e}")

        # Commit changes to the database
        conn.commit()
        logging.info(f"{symbol} OHLCV and rate change data saved to the {table_name} table successfully.")

    def get_symbol_data_from_db(self, conn, timeframe, symbol):
        """
        Fetch all data for a specific symbol and timeframe from the database 
        and return it as a pandas DataFrame.
        
        Args:
            conn (sqlite3.Connection): The SQLite connection object.
            timeframe (str): The timeframe ('4h', 'd', or 'w') to determine the table name.
            symbol (str): The trading pair symbol (e.g., 'BTCUSDT').

        Returns:
            pd.DataFrame: The DataFrame containing the data for the specified symbol and timeframe.
        """
        # Determine the appropriate table based on the timeframe
        table_name = f'usdt_{timeframe}'

        # SQL query to fetch all data for the given symbol
        query = f'''
        SELECT 
            open_time, close_time, open_price, high_price, low_price, close_price, volume, quote_volume,
            rate_change_open_close, rate_change_high_low,
            z_rate_change_open_close, z_rate_change_high_low, z_volume_pair, 
            z_rate_change_open_close_all_pairs, z_rate_change_high_low_all_pairs, z_volume_all_pairs
        FROM {table_name}
        WHERE symbol = ?
        '''

        try:
            # Execute the query and fetch the data into a pandas DataFrame
            df = pd.read_sql_query(query, conn, params=(symbol,))

            # Convert 'open_time' and 'close_time' to datetime if needed
            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
            df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')

            logging.info(f"Fetched data for {symbol} from {table_name} table successfully.")
            return df

        except Exception as e:
            logging.error(f"Error fetching data for {symbol} from {table_name}: {e}")
            return pd.DataFrame()  # Return an empty DataFrame in case of an error

    def save_zscores_to_db(self, df, conn, timeframe, symbol=None, zscore_type='pair'):
        """
        Save Z-scores (pair-specific or cross-pair) to the database.
        
        Args:
            df (pd.DataFrame): The DataFrame containing the Z-scores to save.
            conn (sqlite3.Connection): The SQLite connection object.
            timeframe (str): The timeframe ('4h', 'd', or 'w') to determine the table name.
            symbol (str or None): The trading pair symbol, only used for 'pair' Z-scores.
            zscore_type (str): Specifies the type of Z-scores to save ('pair' or 'cross').
        """
        # Determine the appropriate table based on the timeframe
        table_name = f'usdt_{timeframe}'
        
        # Define the Z-score columns based on zscore_type
        if zscore_type == 'pair':
            zscore_columns = ['z_rate_change_open_close', 'z_rate_change_high_low', 'z_volume_pair']
        elif zscore_type == 'cross':
            zscore_columns = ['z_rate_change_open_close_all_pairs', 'z_rate_change_high_low_all_pairs', 'z_volume_all_pairs']
        else:
            raise ValueError("Invalid zscore_type. Must be 'pair' or 'cross'.")
        
        cursor = conn.cursor()

        for _, row in df.iterrows():
            # Prepare the Z-scores data to be inserted or updated
            open_time = row['open_time']
            if not isinstance(open_time, int):
                open_time = int(open_time.timestamp() * 1000)

            zscores_data = (
                row.get(zscore_columns[0], None),  # Z-score for rate change open/close
                row.get(zscore_columns[1], None),  # Z-score for rate change high/low
                row.get(zscore_columns[2], None),  # Z-score for volume
                open_time
            )

            if zscore_type == 'pair':
                # If saving pair-specific Z-scores, include the symbol in the query
                zscores_data += (row['symbol'],)  # Append the symbol to zscores_data

                query = f'''
                UPDATE {table_name} 
                SET {zscore_columns[0]} = ?, {zscore_columns[1]} = ?, {zscore_columns[2]} = ?
                WHERE symbol = ? AND open_time = ?
                '''
            elif zscore_type == 'cross':
                # For cross-pair Z-scores, update all symbols at the given open_time
                query = f'''
                UPDATE {table_name} 
                SET {zscore_columns[0]} = ?, {zscore_columns[1]} = ?, {zscore_columns[2]} = ?
                WHERE open_time = ?
                '''

            try:
                # Execute the query
                cursor.execute(query, zscores_data)
            except Exception as e:
                logging.error(f"Error saving Z-scores for {row['symbol']} at {row['open_time']}: {e}")
        
        # Commit changes to the database
        conn.commit()
        logging.info(f"Z-scores ({zscore_type}) saved to the {table_name} table.")

    def get_all_data_from_db(self, conn, timeframe):
        """
        Fetch all candlestick data for the given timeframe from the database.

        Args:
            conn (sqlite3.Connection): The SQLite connection object.
            timeframe (str): The timeframe ('4h', 'd', or 'w') to determine the table name.

        Returns:
            pd.DataFrame: A pandas DataFrame containing all candlestick data.
        """
        # Determine the appropriate table based on the timeframe
        table_name = f'usdt_{timeframe}'

        # SQL query to select all relevant columns
        query = f'''
        SELECT 
            symbol, 
            open_time, 
            close_time, 
            open_price, 
            high_price, 
            low_price, 
            close_price, 
            volume, 
            quote_volume, 
            rate_change_open_close, 
            rate_change_high_low
        FROM {table_name}
        '''

        # Fetch the data and load it into a pandas DataFrame
        try:
            df = pd.read_sql_query(query, conn)
            logging.info(f"Fetched all data from {table_name} for Z-score calculation.")
        except Exception as e:
            logging.error(f"Error fetching data from {table_name}: {e}")
            return pd.DataFrame()  # Return an empty DataFrame if there's an issue

        return df
