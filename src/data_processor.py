from datetime import datetime, timezone
import pandas as pd
from scipy.stats import zscore
import sqlite3

class DataProcessor:
    
    def calculate_z_scores(self, data):
        """Calculate Z-scores for percentage change and volume (in USDT)."""
        df = pd.DataFrame(data)
        df['z_pct_change'] = zscore(df['pct_change'])
        df['z_quote_volume'] = zscore(df['quote_volume'])  # Use quote volume (USDT) for Z-scores
        return df

    def combine_z_scores(self, df):
        """Combine the Z-scores of percentage change and volume in USDT."""
        df['z_combined'] = df['z_pct_change'] * df['z_quote_volume']  # Use quote volume Z-score
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

        open_time = datetime.fromtimestamp(candlestick_data[0] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        close_time = datetime.fromtimestamp(candlestick_data[6] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        
        open_price = float(candlestick_data[1])
        high_price = float(candlestick_data[2])
        low_price = float(candlestick_data[3])
        close_price = float(candlestick_data[4])
        volume = float(candlestick_data[5])  # Base asset volume, not used for Z-scores
        quote_volume = float(candlestick_data[7])  # Quote volume in USDT

        # Calculate rate change
        rate_change = self.calculate_rate_change(open_price, close_price)

        return {
            "symbol": usdt_pair,
            "open_price": open_price,
            "high_price": high_price,
            "low_price": low_price,
            "close_price": close_price,
            "volume": volume,  # Base asset volume (not used for Z-scores)
            "quote_volume": quote_volume,  # This is the volume in USDT
            "pct_change": rate_change,
            "open_time": open_time,
            "close_time": close_time
        }

    def fetch_and_process_historical_data(self, api_client, symbol, start_time, end_time):
        """
        Fetches historical data for the given symbol and time range and processes it.
        """
        candlestick_data = api_client.fetch_candlestick_data_by_time(symbol, start_time, end_time)
        if candlestick_data:
            return self.process_usdt_pair_data(symbol, candlestick_data)
        else:
            return None

    def save_candlestick_data_to_db(self, data, db_file="trading_data.db"):
        """Save the candlestick data to the SQLite database."""
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        for row in data:
            if None in (row['open_price'], row['high_price'], row['low_price'], row['close_price']):
                print(f"Skipping row due to missing price data: {row}")
                continue

            # Insert the new data into the database
            cursor.execute('''
            INSERT INTO usdt_4h 
            (symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, quote_volume, rate_change, z_quote_volume, z_pct_change, z_combined)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['symbol'], row['open_time'], row['close_time'],
                row['open_price'], row['high_price'], row['low_price'], row['close_price'],
                row['volume'], row['quote_volume'], row['pct_change'],
                row.get('z_quote_volume', None), row.get('z_pct_change', None),
                row.get('z_combined', None)
            ))

        conn.commit()
        conn.close()
        print(f"Data saved successfully.")
