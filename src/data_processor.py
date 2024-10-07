from datetime import datetime, timedelta
import pandas as pd
import sqlite3
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

    def calculate_z_scores_for_pair(self, conn, symbol):
        """Calculate Z-scores for a specific trading pair."""
        query_pair = '''SELECT * FROM usdt_4h WHERE symbol = ?'''
        df_pair = pd.read_sql_query(query_pair, conn, params=(symbol,))
        query_all = '''SELECT * FROM usdt_4h'''
        df_all = pd.read_sql_query(query_all, conn)

        df_pair['open_time'] = pd.to_datetime(df_pair['open_time'], unit='ms')
        df_all['open_time'] = pd.to_datetime(df_all['open_time'], unit='ms')

        df_pair['z_rate_change_pair'] = zscore(df_pair['rate_change'])
        df_pair['z_volume_pair'] = zscore(df_pair['volume'])
        df_pair['z_combined_pair'] = df_pair['z_rate_change_pair'] * df_pair['z_volume_pair']

        df_pair['z_rate_change_all_pairs'] = zscore(df_all['rate_change'])
        df_pair['z_volume_all_pairs'] = zscore(df_all['quote_volume'])
        df_pair['z_combined_all_pairs'] = df_pair['z_rate_change_all_pairs'] * df_pair['z_volume_all_pairs']

        return df_pair

    def check_and_clean_data(self, conn, symbol, latest_binance_timestamp):
        query = '''SELECT open_time FROM usdt_4h WHERE symbol = ? ORDER BY open_time DESC'''
        df = pd.read_sql_query(query, conn, params=(symbol,))
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        one_year_ago = datetime.utcfromtimestamp(latest_binance_timestamp / 1000) - timedelta(days=365)

        cursor = conn.cursor()
        cursor.execute('DELETE FROM usdt_4h WHERE symbol = ? AND open_time < ?', (symbol, one_year_ago.strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()

        missing_intervals = []
        current_time = datetime.utcfromtimestamp(latest_binance_timestamp / 1000)
        while current_time > one_year_ago:
            if current_time not in df['open_time'].values:
                start_time = int(current_time.timestamp() * 1000)
                end_time = int((current_time + timedelta(hours=4)).timestamp() * 1000)
                missing_intervals.append((start_time, end_time))
            current_time -= timedelta(hours=4)

        return missing_intervals

    def backfill_missing_data(self, api_client, conn, symbol, start_time, end_time):
        cursor = conn.cursor()

        # Ensure start_time and end_time are integers
        if not isinstance(start_time, int):
            start_time = int(start_time)
        if not isinstance(end_time, int):
            end_time = int(end_time)

        while start_time < end_time:
            candles = api_client.fetch_candlestick_data_by_time(symbol, start_time, end_time)
            if not candles:
                break

            processed_data = [self.process_usdt_pair_data(symbol, candle) for candle in candles]
            processed_data = [data for data in processed_data if data]
            if processed_data:
                self.save_candlestick_data_to_db(processed_data, conn)

            start_time = candles[-1][6] + 1  # Move start_time to the next candle

        logging.info(f"Backfill completed for {symbol}.")

    def save_candlestick_data_to_db(self, data, conn):
        cursor = conn.cursor()

        for row in data:
            if None in (row['open_price'], row['high_price'], row['low_price'], row['close_price']):
                continue


            # debug print to be removed after
            print("open time:")
            print(row['open_time'])
            print(type(row['open_time']))
            print("close time:")
            print(row['close_time'])
            print(type(row['close_time']))

            close_time_ms = row['close_time']

            if isinstance(row['open_time'], pd.Timestamp):
                open_time_ms = int(row['open_time'].timestamp() * 1000)
            else:
                open_time_ms = int(row['open_time'])

            cursor.execute('SELECT COUNT(*) FROM usdt_4h WHERE symbol = ? AND open_time = ?', (row['symbol'], open_time_ms))
            exists = cursor.fetchone()[0]

            if exists:
                cursor.execute('''
                UPDATE usdt_4h 
                SET z_rate_change_pair = ?, z_volume_pair = ?, z_combined_pair = ?, 
                    z_rate_change_all_pairs = ?, z_volume_all_pairs = ?, z_combined_all_pairs = ?
                WHERE symbol = ? AND open_time = ?
                ''', (
                    row.get('z_rate_change_pair'), row.get('z_volume_pair'), row.get('z_combined_pair'),
                    row.get('z_rate_change_all_pairs'), row.get('z_volume_all_pairs'), row.get('z_combined_all_pairs'),
                    row['symbol'], open_time_ms
                ))
                continue

            cursor.execute('''
            INSERT INTO usdt_4h 
            (symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, quote_volume, rate_change, 
            z_rate_change_pair, z_volume_pair, z_combined_pair, z_rate_change_all_pairs, z_volume_all_pairs, z_combined_all_pairs)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['symbol'], open_time_ms, close_time_ms, row['open_price'], row['high_price'], row['low_price'], row['close_price'],
                row['volume'], row['quote_volume'], row['rate_change'], row.get('z_rate_change_pair'),
                row.get('z_volume_pair'), row.get('z_combined_pair'), row.get('z_rate_change_all_pairs'),
                row.get('z_volume_all_pairs'), row.get('z_combined_all_pairs')
            ))

        conn.commit()
        logging.info(f"Data saved to the database successfully.")
    
    def get_latest_time_in_db(self, conn, symbol):
        """Retrieve the latest timestamp in milliseconds for a given trading pair."""
        cursor = conn.cursor()
        cursor.execute('SELECT MAX(open_time) FROM usdt_4h WHERE symbol = ?', (symbol,))
        result = cursor.fetchone()
        if result and result[0]:
            return result[0]
        return None
