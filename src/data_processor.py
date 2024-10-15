from datetime import datetime, timedelta, timezone
import pandas as pd
from scipy.stats import zscore
import logging

class DataProcessor:

    def process_usdt_pair_data(self, symbol, candlestick_data):
        """Process the candlestick data for a given USDT pair."""
        if not candlestick_data:
            return None

        open_time = candlestick_data[0]
        close_time = candlestick_data[6]
        open_price = float(candlestick_data[1])
        high_price = float(candlestick_data[2])
        low_price = float(candlestick_data[3])
        close_price = float(candlestick_data[4])
        volume = float(candlestick_data[5])
        quote_volume = float(candlestick_data[7])

        rate_change = (close_price - open_price) / open_price * 100

        return {
            "symbol": symbol,
            "open_price": open_price,
            "high_price": high_price,
            "low_price": low_price,
            "close_price": close_price,
            "volume": volume,
            "quote_volume": quote_volume,
            "rate_change": rate_change,
            "open_time": open_time,
            "close_time": close_time
        }

    def calculate_z_scores_for_pair(self, df_pair):
        """Calculate Z-scores for a specific trading pair (pair-specific Z-scores)."""
        df_pair['open_time'] = pd.to_datetime(df_pair['open_time'], unit='ms')
        df_pair['z_rate_change_pair'] = zscore(df_pair['rate_change'])
        df_pair['z_volume_pair'] = zscore(df_pair['volume'])
        return df_pair

    def calculate_z_scores_for_all_pairs(self, df_all):
        """Calculate Z-scores across all trading pairs for the last completed 4-hour candle."""
        df_all['open_time'] = pd.to_datetime(df_all['open_time'], unit='ms')
        df_all['z_rate_change_all_pairs'] = zscore(df_all['rate_change'])
        df_all['z_volume_all_pairs'] = zscore(df_all['quote_volume'])
        return df_all

    def get_most_recent_completed_candle_time(self):
        """
        Calculate the most recent completed candlestick time for a 4-hour interval.

        Returns:
            int: The most recent completed 4-hour candlestick time in milliseconds.
        """
        now = datetime.now(timezone.utc)  # Get the current time in UTC
        hours_to_subtract = now.hour % 4
        most_recent_candle_time = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=hours_to_subtract + 4)
        return int(most_recent_candle_time.timestamp() * 1000)
