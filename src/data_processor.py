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

    def calculate_zscores(self, df):
        """
        Calculate Z-scores for rate changes and volume in the given DataFrame.
        
        Args:
            df (pd.DataFrame): The DataFrame containing the candlestick data, including rate changes and volume.

        Returns:
            pd.DataFrame: The DataFrame with the new Z-score columns.
        """
        # Calculate Z-scores for rate change open/close
        if 'rate_change_open_close' in df.columns:
            df['z_rate_change_open_close'] = zscore(df['rate_change_open_close'].fillna(0))

        # Calculate Z-scores for rate change high/low
        if 'rate_change_high_low' in df.columns:
            df['z_rate_change_high_low'] = zscore(df['rate_change_high_low'].fillna(0))

        # Calculate Z-scores for volume
        if 'volume' in df.columns:
            df['z_volume_pair'] = zscore(df['volume'].fillna(0))

        return df