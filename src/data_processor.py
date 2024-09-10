# src/data_processor.py

import pandas as pd
from scipy.stats import zscore
from datetime import datetime, timezone

class DataProcessor:
    
    def calculate_z_scores(self, data):
        """Calculate Z-scores for percentage change and volume (in USDT)."""
        # Ensure data is a pandas DataFrame
        df = pd.DataFrame(data)
        
        # Calculate Z-scores for the relevant columns
        df['z_pct_change'] = zscore(df['pct_change'])
        df['z_volume_usdt'] = zscore(df['volume_usdt'])
        
        return df

    def combine_z_scores(self, df):
        """Combine the Z-scores of percentage change and volume in USDT."""
        df['combined_z_score'] = df['z_pct_change'] * df['z_volume_usdt']
        return df

    def calculate_rate_change(self, open_price, close_price):
        """Calculate the percentage rate change between open and close prices."""
        return (float(close_price) - float(open_price)) / float(open_price) * 100

    def process_usdt_pair_data(self, usdt_pair, candlestick_data):
        """
        Process the candlestick data for a given USDT pair.
        Calculates rate change and volume in USDT.
        """
        if not candlestick_data:
            return None

        # Extract the open and close prices and calculate rate change
        open_price = candlestick_data[1]
        close_price = candlestick_data[4]
        rate_change = self.calculate_rate_change(open_price, close_price)

        # Get the volume in USDT (quote asset volume, index 7)
        volume_in_usdt = float(candlestick_data[7])

        return {
            "pair": usdt_pair,
            "pct_change": rate_change,
            "volume_usdt": volume_in_usdt
        }

    def process_all_usdt_pairs(self, api_client, usdt_pairs):
        """
        Fetch candlestick data for each USDT pair, calculate rate change and volume, 
        and return the processed data along with open/close times.
        """
        candlestick_data_list = []
        open_time = None
        close_time = None

        # Process each USDT pair's data
        for usdt_pair in usdt_pairs:
            candlestick_data = api_client.fetch_last_completed_candlestick(usdt_pair)

            processed_data = self.process_usdt_pair_data(usdt_pair, candlestick_data)

            if processed_data is None:
                logging.warning(f"No candlestick data available for {usdt_pair}. Skipping.")
                continue

            # Set open and close times (once, since they are the same for all pairs)
            if not open_time:
                open_time = datetime.fromtimestamp(candlestick_data[0] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                close_time = datetime.fromtimestamp(candlestick_data[6] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            # Collect the processed data for z-score calculation
            candlestick_data_list.append(processed_data)

        # Return the processed data and open/close times
        return candlestick_data_list, open_time, close_time
