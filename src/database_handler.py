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

    def save_symbol_data_to_db(self, data, conn, timeframe, symbol=None):
        """
        Save candlestick data and Z-scores to the SQLite database using a dictionary approach.

        Args:
            data (list of dict): The list of dictionaries containing the candlestick data.
            conn (sqlite3.Connection): The SQLite connection object.
            timeframe (str): The timeframe ('4h', 'd', 'w') to determine the appropriate table.
            symbol (str): The trading pair symbol.
        """
        # Determine the appropriate table based on the timeframe
        table_name = f'usdt_{timeframe}'

        cursor = conn.cursor()

        # Iterate over the records (assuming `data` is a list of dictionaries)
        for row in data:
            if isinstance(row['open_time'], pd.Timestamp):
                open_time_ms = int(row['open_time'].timestamp() * 1000)
            else:
                open_time_ms = int(row['open_time'])

            # Use provided symbol if given, otherwise use the symbol from the data
            row_symbol = symbol if symbol else row.get('symbol')

            # Prepare the data to be inserted or updated
            data_to_insert = (
                row_symbol,  # Trading pair symbol
                open_time_ms,  # Open time
                row['close_time'],  # Close time
                row['open_price'],  # Open price
                row['high_price'],  # High price
                row['low_price'],   # Low price
                row['close_price'],  # Close price
                row['volume'],  # Volume
                row['quote_volume'],  # Quote volume
                row['rate_change_open_close'],  # Rate change open/close
                row['rate_change_high_low'],  # Rate change high/low
                row.get('z_rate_change_open_close', 0),  # Z-score open/close
                row.get('z_rate_change_high_low', 0),  # Z-score high/low
                row.get('z_volume_pair', 0),  # Z-score volume
                row.get('z_rate_change_open_close_all_pairs', 0),
                row.get('z_rate_change_high_low_all_pairs', 0),
                row.get('z_volume_all_pairs', 0)
            )

            # Check if the row already exists based on `symbol` and `open_time`
            cursor.execute(f'SELECT COUNT(*) FROM {table_name} WHERE symbol = ? AND open_time = ?', (row_symbol, open_time_ms))
            exists = cursor.fetchone()[0]

            if exists:
                # Update if the record exists
                query = (
                    f"UPDATE {table_name} SET z_rate_change_open_close = ?, z_rate_change_high_low = ?, z_volume_pair = ?, z_rate_change_open_close_all_pairs = ?, z_rate_change_high_low_all_pairs = ?, z_volume_all_pairs = ? "
                    f"WHERE symbol = ? AND open_time = ?"
                )
                cursor.execute(query, (
                    row.get('z_rate_change_open_close', 0),
                    row.get('z_rate_change_high_low', 0),
                    row.get('z_volume_pair', 0),
                    row.get('z_rate_change_open_close_all_pairs', 0),
                    row.get('z_rate_change_high_low_all_pairs', 0),
                    row.get('z_volume_all_pairs', 0),
                    row_symbol, open_time_ms
                ))
            else:
                # Insert if the record does not exist
                query = (
                    f"INSERT INTO {table_name} (symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, quote_volume, "
                    f"rate_change_open_close, rate_change_high_low, z_rate_change_open_close, z_rate_change_high_low, z_volume_pair, z_rate_change_open_close_all_pairs, z_rate_change_high_low_all_pairs, z_volume_all_pairs) "
                    f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                )
                cursor.execute(query, data_to_insert)
        
        logging.info(f"Data saved to the {table_name} table.")

        # Commit the transaction
        conn.commit()

