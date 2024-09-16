import pandas as pd
from scipy.stats import zscore
from datetime import datetime, timezone
import sqlite3

class DataProcessor:

    def calculate_z_scores(self, data):
        """Calculate Z-scores for percentage change and volume (in USDT)."""
        df = pd.DataFrame(data)
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
        Extract OHLC (open, high, low, close), rate change, volume, and quote volume.
        """
        if not candlestick_data:
            return None

        # Extract OHLC and volumes
        open_price = candlestick_data[1]
        high_price = candlestick_data[2]
        low_price = candlestick_data[3]
        close_price = candlestick_data[4]
        volume_in_usdt = float(candlestick_data[5])  # Candlestick volume (base asset)
        quote_volume = float(candlestick_data[7])  # Quote asset volume

        # Calculate rate change
        rate_change = self.calculate_rate_change(open_price, close_price)

        return {
            "symbol": usdt_pair,
            "open_price": open_price,
            "high_price": high_price,
            "low_price": low_price,
            "close_price": close_price,
            "pct_change": rate_change,
            "volume_usdt": volume_in_usdt,
            "quote_volume": quote_volume,
            "open_time": datetime.fromtimestamp(candlestick_data[0] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            "close_time": datetime.fromtimestamp(candlestick_data[6] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        }

    def process_all_usdt_pairs(self, api_client, usdt_pairs):
        """
        Fetch candlestick data for each USDT pair, calculate rate change and volume,
        and return the processed data along with open/close times.
        """
        candlestick_data_list = []
        open_time = None
        close_time = None

        for usdt_pair in usdt_pairs:
            candlestick_data = api_client.fetch_last_completed_candlestick(usdt_pair)
            processed_data = self.process_usdt_pair_data(usdt_pair, candlestick_data)

            if processed_data is None:
                continue

            if not open_time:
                open_time = processed_data['open_time']
                close_time = processed_data['close_time']

            candlestick_data_list.append(processed_data)

        return candlestick_data_list, open_time, close_time

    def save_candlestick_data_to_db(self, data, db_file="trading_data.db"):
        """Save the candlestick data, including Z-scores, to the SQLite database."""
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        try:
            # Insert the data into the SQLite database
            for row in data:
                cursor.execute('''
                INSERT OR IGNORE INTO usdt_4h 
                (symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, quote_volume, rate_change, volume_zscore, rate_change_zscore, combined_zscore)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row['symbol'], row['open_time'], row['close_time'],
                    row['open_price'], row['high_price'], row['low_price'], row['close_price'],
                    row['volume_usdt'], row['quote_volume'], row['pct_change'],
                    row.get('z_volume_usdt', None), row.get('z_pct_change', None),
                    row.get('combined_z_score', None)
                ))
            conn.commit()
        except sqlite3.Error as e:
            print(f"Error inserting data into the database: {e}")
        finally:
            conn.close()

        print(f"Data saved to {db_file} successfully.")
