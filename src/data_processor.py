from datetime import datetime, timedelta, timezone
import pandas as pd
from scipy.stats import zscore
import logging

class DataProcessor:

    def convert_candle_data_to_dataframe(self, candle_data):
        """
        Converts the candle data fetched from Binance API to a pandas DataFrame.
        This function will only keep the necessary columns: 
        open_time, open_price, high_price, low_price, close_price, volume, close_time, and quote_volume.
        """
        # Extract only the necessary columns from the candle_data
        filtered_candle_data = [
            [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]] for row in candle_data
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
        df['z_volume_pair'] = zscore(df['quote_volume'])
        
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
        df['z_volume_all_pairs'] = zscore(df['quote_volume'])

        logging.info("Cross Z-scores data calculated.")
        
        return df

