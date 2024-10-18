from datetime import datetime, timedelta, timezone
import pandas as pd
from scipy.stats import zscore
import logging
import sqlite3

class DataProcessor:

    def convert_candle_data_to_dataframe(self, candle_data, timeframe='4h'):
        """
        Converts the candle data fetched from Binance API to a pandas DataFrame.
        This function will only keep the necessary columns: 
        open_time, open_price, high_price, low_price, close_price, volume, close_time, and quote_volume.
        """
        if timeframe == '4h':
            filtered_candle_data = [
                [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]] for row in candle_data
            ]
        else:
            filtered_candle_data = [
                [row[0], row[2], row[3], row[4], row[5], row[6], row[1], row[7]] for row in candle_data
            ]

        # Define the column names to keep
        columns_to_keep = ['open_time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'close_time', 'quote_volume']

        # Convert the filtered_candle_data into a pandas DataFrame
        df = pd.DataFrame(filtered_candle_data, columns=columns_to_keep)

        # Convert the necessary columns to numeric (if required)
        df['open_price'] = pd.to_numeric(df['open_price'])
        df['high_price'] = pd.to_numeric(df['high_price'])
        df['low_price'] = pd.to_numeric(df['low_price'])
        df['close_price'] = pd.to_numeric(df['close_price'])
        df['volume'] = pd.to_numeric(df['volume'])  # This is the base asset volume
        df['quote_volume'] = pd.to_numeric(df['quote_volume'])  # This is the quote asset volume (in USDT)

        return df

    def calculate_rate_changes(self, df):
        # Calculate Open/Close Rate Change
        df['rate_change_open_close'] = ((df['close_price'] - df['open_price']) / df['open_price']) * 100
    
        # Calculate High/Low Rate Change
        df['rate_change_high_low'] = ((df['high_price'] - df['low_price']) / df['low_price']) * 100
    
        return df

    def calculate_zscores_for_pair(self, conn, symbol, timeframe='4h'):
        """
        Calculate Z-scores for a specific trading pair (pair-specific Z-scores).
        
        Args:
            conn (sqlite3.Connection): The SQLite connection object.
            symbol (str): The trading pair symbol.
            timeframe (str): Specifies the timeframe ('4h', 'd', 'w') to determine the table name.
            
        Returns:
            pd.DataFrame: The DataFrame with the new Z-score columns for the specific pair.
        """
        # Determine the appropriate table based on the timeframe
        table_name = f'usdt_{timeframe}'
        
        # Query the specific pair data
        query = f'''SELECT * FROM {table_name} WHERE symbol = ?'''
        df = pd.read_sql_query(query, conn, params=(symbol,))
        
        # Convert the open_time column from milliseconds to datetime
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        
        # Calculate pair-specific Z-scores
        df['z_rate_change_open_close'] = zscore(df['rate_change_open_close'])
        df['z_rate_change_high_low'] = zscore(df['rate_change_high_low'])
        df['z_volume_pair'] = zscore(df['volume'])
        
        logging.info(f"{symbol} Z-scores data calculated.")

        return df

    def calculate_zscores_for_all_pairs(self, conn, timeframe='4h'):
        """
        Calculate Z-scores across all trading pairs for the last completed 4-hour candle (cross-pair Z-scores).
        
        Args:
            conn (sqlite3.Connection): The SQLite connection object.
            timeframe (str): Specifies the timeframe ('4h', 'd', 'w') to determine the table name.
            
        Returns:
            pd.DataFrame: The DataFrame with the new Z-score columns for all pairs.
        """
        # Determine the appropriate table based on the timeframe
        table_name = f'usdt_{timeframe}'
        
        # Query all pairs data
        query = f'''SELECT * FROM {table_name}'''
        df = pd.read_sql_query(query, conn)
        
        # Convert the open_time column from milliseconds to datetime
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        
        # Calculate cross-pair Z-scores across the entire dataset
        df['z_rate_change_open_close_all_pairs'] = zscore(df['rate_change_open_close'])
        df['z_rate_change_high_low_all_pairs'] = zscore(df['rate_change_high_low'])
        df['z_volume_all_pairs'] = zscore(df['volume'])

        logging.info("Cross Z-scores data calculated.")
        
        return df

    def aggregate_candle_data(self, symbol, timeframe, start_time, end_time):
        """
        Aggregate 4-hour candles into daily or weekly candles and return in the same format as Binance API data (list of lists).
        """
        # Connect to the database
        conn = sqlite3.connect('trading_data.db')

        # Fetch 4-hour candles from the database within the specified time range
        query = f"SELECT * FROM usdt_4h WHERE symbol = ? AND open_time >= ? AND open_time <= ?"
        df_4h = pd.read_sql_query(query, conn, params=(symbol, start_time, end_time))

        if df_4h.empty:
            print(f"No 4-hour data available for {symbol} between {start_time} and {end_time}.")
            return []

        # Convert 'open_time' and 'close_time' to datetime for resampling
        df_4h['open_time'] = pd.to_datetime(df_4h['open_time'], unit='ms')
        df_4h['close_time'] = pd.to_datetime(df_4h['close_time'], unit='ms')

        # Resample the data into daily ('1D') or weekly ('1W') candles based on the 'timeframe'
        if timeframe == 'd':  # Daily candles (1 day = 6 x 4-hour candles)
            resample_rule = '1D'
            required_candles = 6  # 6 x 4-hour candles in a day
        elif timeframe == 'w':  # Weekly candles (1 week = 6 x 7 x 4-hour candles)
            resample_rule = '1W'
            required_candles = 42  # 42 x 4-hour candles in a week
        else:
            raise ValueError("Invalid timeframe. Use 'd' for daily or 'w' for weekly.")

        # Resample to aggregate the 4-hour candles into daily or weekly candles
        df_resampled = df_4h.resample(resample_rule, on='open_time').agg({
            'open_price': 'first',   # The first open price of the period
            'high_price': 'max',     # The highest price during the period
            'low_price': 'min',      # The lowest price during the period
            'close_price': 'last',   # The last close price of the period
            'volume': 'sum',         # Total volume during the period
            'quote_volume': 'sum',   # Total quote volume during the period
            'close_time': 'last'     # The last close time of the period
        }).reset_index()

        # Calculate how many 4-hour candles contributed to each period
        df_resampled['candle_count'] = df_4h.resample(resample_rule, on='open_time').size().values

        # Filter out incomplete periods (where the number of 4-hour candles is less than required)
        df_resampled = df_resampled[df_resampled['candle_count'] == required_candles]

        # Convert 'open_time' and 'close_time' back to milliseconds for consistency with Binance API
        df_resampled['open_time'] = df_resampled['open_time'].astype(int) // 10**6
        df_resampled['close_time'] = df_resampled['close_time'].astype(int) // 10**6

        # Return the columns in the correct order for database insertion
        aggregated_data = df_resampled[['open_time', 'close_time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'quote_volume']].values.tolist()

        # Close the database connection
        conn.close()

        return aggregated_data


