import sqlite3
import pandas as pd
import logging
import argparse
from scipy.stats import zscore
from datetime import timedelta

logging.basicConfig(level=logging.INFO)

def get_last_candle_time(conn, table, symbol):
    """
    Fetch the most recent open_time from the daily/weekly table for a specific symbol.
    """
    query = f"SELECT MAX(open_time) FROM {table} WHERE symbol = ?"
    cursor = conn.cursor()
    cursor.execute(query, (symbol,))
    result = cursor.fetchone()[0]
    return result

def get_first_4h_candle_time(conn, symbol):
    """
    Fetch the first open_time from the 4-hour table for a specific symbol.
    """
    query = "SELECT MIN(open_time) FROM usdt_4h WHERE symbol = ?"
    cursor = conn.cursor()
    cursor.execute(query, (symbol,))
    result = cursor.fetchone()[0]
    return result

def fetch_4h_data(conn, symbol, last_candle_time):
    """
    Fetch 4-hour data from the usdt_4h table starting from last_candle_time.
    """
    query = '''
        SELECT open_time, close_time, open_price, close_price, high_price, low_price, volume, rate_change 
        FROM usdt_4h 
        WHERE symbol = ? AND open_time > ?
        ORDER BY open_time ASC
    '''
    df = pd.read_sql_query(query, conn, params=(symbol, last_candle_time))
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    return df

def aggregate_candles(df, timeframe):
    """
    Aggregate 4-hour candles into daily or weekly candles based on the timeframe.
    """

    if timeframe == 'd':  # Daily aggregation
        df['date'] = df['open_time'].dt.date

        # Check if there are exactly 6 candles per day (for daily aggregation)
        valid_days = df.groupby('date').filter(lambda x: len(x) == 6)
        
        if valid_days.empty:
            logging.info("No valid days with 6 full 4-hour candles available. Skipping aggregation.")
            return pd.DataFrame()  # Return empty DataFrame if no valid days found
        
        # Perform daily aggregation
        agg_df = valid_days.groupby(['date']).agg({
            'open_time': 'first',  
            'close_time': 'last',  
            'open_price': 'first',  
            'close_price': 'last',  
            'high_price': 'max',  
            'low_price': 'min',  
            'volume': 'sum'
        }).reset_index()

        agg_df['close_time'] = pd.to_datetime(agg_df['close_time'], unit='ms') + timedelta(hours=24)
    
    elif timeframe == 'w':  # Weekly aggregation
        df['week'] = df['open_time'].dt.to_period('W').apply(lambda r: r.start_time)

        # Check if there are exactly 42 candles per week (for weekly aggregation)
        valid_weeks = df.groupby('week').filter(lambda x: len(x) == 42)
        
        if valid_weeks.empty:
            logging.info("No valid weeks with 42 full 4-hour candles available. Skipping aggregation.")
            return pd.DataFrame()  # Return empty DataFrame if no valid weeks found
        
        # Perform weekly aggregation
        agg_df = valid_weeks.groupby(['week']).agg({
            'open_time': 'first',
            'close_time': 'last',
            'open_price': 'first',
            'close_price': 'last',
            'high_price': 'max',
            'low_price': 'min',
            'volume': 'sum'
        }).reset_index()

        agg_df['close_time'] = pd.to_datetime(agg_df['close_time'], unit='ms') + timedelta(days=7)

    return agg_df


def calculate_rate_change_after_aggregation(agg_df):
    """
    Calculate rate change for each row in the aggregated DataFrame.
    """
    rate_changes = []
    
    for _, row in agg_df.iterrows():
        open_price = row['open_price']
        close_price = row['close_price']

        # Ensure open_price is not zero to avoid division by zero
        if open_price == 0:
            rate_change = float('nan')  # Assign NaN if open_price is zero
        else:
            rate_change = ((close_price - open_price) / open_price) * 100
        
        logging.info(f"Calculating rate change: open_price = {open_price}, close_price = {close_price}, rate_change = {rate_change}")
        rate_changes.append(rate_change)

    # Add the calculated rate change to the DataFrame
    agg_df['rate_change'] = rate_changes

    return agg_df

def calculate_z_scores_for_pair(agg_df):
    """
    Calculate Z-scores for the aggregated data (pair-specific Z-scores).
    """
    agg_df['z_rate_change_pair'] = zscore(agg_df['rate_change'])
    agg_df['z_volume_pair'] = zscore(agg_df['volume'])
    agg_df['z_combined_pair'] = agg_df['z_rate_change_pair'] * agg_df['z_volume_pair']

    return agg_df

def calculate_cross_pair_z_scores(conn, full_agg_df):
    """
    Calculate Z-scores across all trading pairs (cross-pair Z-scores).
    """
    full_agg_df['z_rate_change_all_pairs'] = zscore(full_agg_df['rate_change'])
    full_agg_df['z_volume_all_pairs'] = zscore(full_agg_df['volume'])
    full_agg_df['z_combined_all_pairs'] = full_agg_df['z_rate_change_all_pairs'] * full_agg_df['z_volume_all_pairs']

    return full_agg_df

def save_aggregated_candles_to_db(aggregated_df, conn, table, symbol):
    """
    Save the aggregated candle data along with Z-scores to the database.
    """
    cursor = conn.cursor()
    for _, row in aggregated_df.iterrows():
        open_time_ms = int(row['open_time'].timestamp() * 1000)
        close_time_ms = int(row['close_time'].timestamp() * 1000)

        cursor.execute(f'SELECT COUNT(*) FROM {table} WHERE symbol = ? AND open_time = ?', (symbol, open_time_ms))
        exists = cursor.fetchone()[0]

        if exists:
            logging.info(f"Candle for {symbol} at {open_time_ms} already exists in {table}. Skipping...")
            continue

        cursor.execute(f'''
        INSERT INTO {table} 
        (symbol, open_time, close_time, open_price, close_price, high_price, low_price, volume, rate_change, 
        z_rate_change_pair, z_volume_pair, z_combined_pair, z_rate_change_all_pairs, z_volume_all_pairs, z_combined_all_pairs)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            symbol, open_time_ms, close_time_ms, row['open_price'], row['close_price'], row['high_price'], row['low_price'], 
            row['volume'], row['rate_change'], row['z_rate_change_pair'], row['z_volume_pair'], row['z_combined_pair'], 
            row['z_rate_change_all_pairs'], row['z_volume_all_pairs'], row['z_combined_all_pairs']
        ))

    conn.commit()
    logging.info(f"Aggregated data for {symbol} saved to {table}.")

def main(timeframe):
    table = 'usdt_d' if timeframe == 'd' else 'usdt_w'
    
    # Connect to the database
    conn = sqlite3.connect('trading_data.db')

    # Get distinct trading pairs
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM usdt_4h")
    symbols = cursor.fetchall()

    full_agg_df = pd.DataFrame()  # DataFrame to store aggregated data for all pairs

    for symbol in symbols:
        symbol = symbol[0]
        logging.info(f"Processing symbol: {symbol}")
        
        # Get last candle time from the daily/weekly table
        last_candle_time = get_last_candle_time(conn, table, symbol)

        # If no data in the daily/weekly table, backfill from the first 4-hour candle
        if last_candle_time is None:
            logging.info(f"No existing candles in {table} for {symbol}. Backfilling from the first available 4-hour candle.")
            last_candle_time = get_first_4h_candle_time(conn, symbol)

        if last_candle_time is None:
            logging.warning(f"No available 4-hour candles to backfill for {symbol}. Skipping...")
        else:
            logging.info(f"Backfilling from time: {pd.to_datetime(last_candle_time, unit='ms')} for symbol: {symbol}")
            
            # Fetch 4-hour data from last candle time
            df = fetch_4h_data(conn, symbol, last_candle_time)

            if df.empty:
                logging.info(f"No new 4-hour data available for {symbol}. Skipping...")
                continue
            
            # Aggregate candles based on timeframe
            aggregated_df = aggregate_candles(df, timeframe)

            # Check if aggregated_df is empty after aggregation
            if aggregated_df.empty:
                logging.warning(f"Aggregated DataFrame is empty for {symbol} after aggregation. Skipping this symbol.")
                continue  # Skip to the next symbol

            # Calculate rate change after aggregation
            aggregated_df = calculate_rate_change_after_aggregation(aggregated_df)

            # Calculate Z-scores for the pair
            aggregated_df = calculate_z_scores_for_pair(aggregated_df)

            # Append to the full DataFrame for cross-pair Z-score calculation
            aggregated_df['symbol'] = symbol  # Add symbol column for cross-pair analysis
            full_agg_df = pd.concat([full_agg_df, aggregated_df], ignore_index=True)

    # Calculate cross-pair Z-scores once for all pairs
    full_agg_df = calculate_cross_pair_z_scores(conn, full_agg_df)

    # Save aggregated candles with Z-scores to the database
    for symbol in symbols:
        symbol = symbol[0]
        symbol_df = full_agg_df[full_agg_df['symbol'] == symbol]
        save_aggregated_candles_to_db(symbol_df, conn, table, symbol)

    # Close the connection
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggregate 4-hour candles into daily or weekly candles.")
    parser.add_argument("timeframe", choices=["d", "w"], help="Specify 'd' for daily or 'w' for weekly aggregation.")
    args = parser.parse_args()

    main(args.timeframe)
