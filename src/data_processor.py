import pandas as pd
from scipy.stats import zscore
from datetime import datetime, timezone
import sqlite3

class DataProcessor:

    def get_historical_data_for_pair(self, conn, symbol):
        """
        Fetch all available historical data for the given symbol from the database.
        
        Args:
            conn: SQLite connection.
            symbol: The trading pair symbol (e.g., 'BTCUSDT').

        Returns:
            pd.DataFrame: Historical data for the given trading pair.
        """
        query = '''
        SELECT * FROM usdt_4h 
        WHERE symbol = ? 
        ORDER BY open_time DESC
        '''
        df = pd.read_sql_query(query, conn, params=(symbol,))
        return df

    def get_historical_data_all_pairs(self, conn):
        """
        Fetch all available historical data for all trading pairs from the database.

        Args:
            conn: SQLite connection.

        Returns:
            pd.DataFrame: Historical data for all trading pairs.
        """
        query = '''
        SELECT * FROM usdt_4h 
        ORDER BY open_time DESC
        '''
        df = pd.read_sql_query(query, conn)
        return df

    def calculate_z_scores_for_pair(self, conn, symbol):
        """
        Calculate Z-scores for the percentage change and volume for the given symbol
        using all available data in the database (pair-specific Z-scores).
        
        Args:
            conn: SQLite connection.
            symbol: The trading pair symbol (e.g., 'BTCUSDT').

        Returns:
            pd.DataFrame: Z-scores for the given symbol.
        """
        # Get historical data for the pair
        historical_data = self.get_historical_data_for_pair(conn, symbol)
        if historical_data.empty:
            return None

        # Calculate pair-specific Z-scores for percentage change and volume
        historical_data['z_rate_change_pair'] = zscore(historical_data['rate_change'])
        historical_data['z_volume_pair'] = zscore(historical_data['quote_volume'])
        historical_data['z_combined_pair'] = historical_data['z_rate_change_pair'] * historical_data['z_volume_pair']

        return historical_data

    def calculate_z_scores_across_pairs(self, conn):
        """
        Calculate Z-scores for the percentage change and volume across all pairs
        using all available data in the database (cross-pair Z-scores).
        
        Args:
            conn: SQLite connection.

        Returns:
            pd.DataFrame: Z-scores across all pairs.
        """
        # Get historical data for all pairs
        historical_data_all = self.get_historical_data_all_pairs(conn)
        if historical_data_all.empty:
            return None

        # Calculate cross-pair Z-scores for percentage change and volume
        historical_data_all['z_rate_change_all_pairs'] = zscore(historical_data_all['rate_change'])
        historical_data_all['z_volume_all_pairs'] = zscore(historical_data_all['quote_volume'])
        historical_data_all['z_combined_all_pairs'] = historical_data_all['z_rate_change_all_pairs'] * historical_data_all['z_volume_all_pairs']

        return historical_data_all

    def save_candlestick_data_to_db(self, data, db_file="trading_data.db"):
        """
        Save the candlestick data to the SQLite database, including the Z-scores.
        """
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        for row in data:
            if None in (row['open_price'], row['high_price'], row['low_price'], row['close_price']):
                print(f"Skipping row due to missing price data: {row}")
                continue

            # Insert the new data into the database
            cursor.execute('''
            INSERT INTO usdt_4h 
            (symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, quote_volume, rate_change, 
            z_rate_change_pair, z_volume_pair, z_combined_pair, z_rate_change_all_pairs, z_volume_all_pairs, z_combined_all_pairs)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['symbol'], row['open_time'], row['close_time'],
                row['open_price'], row['high_price'], row['low_price'], row['close_price'],
                row['volume'], row['quote_volume'], row['rate_change'],
                row.get('z_rate_change_pair', None), row.get('z_volume_pair', None),
                row.get('z_combined_pair', None), row.get('z_rate_change_all_pairs', None),
                row.get('z_volume_all_pairs', None), row.get('z_combined_all_pairs', None)
            ))

        conn.commit()
        conn.close()
        print(f"Data saved successfully.")
