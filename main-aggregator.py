import sqlite3
import argparse
import logging
import pandas as pd

# Initialize logging
logging.basicConfig(level=logging.INFO)

def get_distinct_trading_pairs(conn):
    """
    Get a list of all distinct trading pairs (symbols) in the 4-hour DB table.
    
    Args:
        conn (sqlite3.Connection): SQLite connection object.
        
    Returns:
        list: List of distinct trading pairs.
    """
    cursor = conn.cursor()
    
    # Query to get all distinct symbols
    query = "SELECT DISTINCT symbol FROM usdt_4h"
    cursor.execute(query)
    
    # Fetch all symbols
    trading_pairs = [row[0] for row in cursor.fetchall()]
    
    return trading_pairs

def get_last_candle_time(conn, table, symbol):
    """
    Get the most recent candle time from the daily or weekly table for a given trading pair.
    
    Args:
        conn (sqlite3.Connection): SQLite connection object.
        table (str): The table to check ('usdt_d' or 'usdt_w').
        symbol (str): The trading pair symbol.
        
    Returns:
        int or None: The timestamp (in milliseconds) of the last candle for the given symbol, or None if no candles exist.
    """
    cursor = conn.cursor()
    
    # Query to get the most recent candle's open time for the symbol
    query = f"SELECT MAX(open_time) FROM {table} WHERE symbol = ?"
    cursor.execute(query, (symbol,))
    
    result = cursor.fetchone()
    return result[0] if result and result[0] else None

def get_first_4h_candle_time(conn, symbol):
    """
    Get the first available 4-hour candle time for a given trading pair.
    
    Args:
        conn (sqlite3.Connection): SQLite connection object.
        symbol (str): The trading pair symbol.
        
    Returns:
        int or None: The timestamp (in milliseconds) of the first 4-hour candle for the given symbol.
    """
    cursor = conn.cursor()
    
    # Query to get the first available 4-hour candle time for the symbol
    query = "SELECT MIN(open_time) FROM usdt_4h WHERE symbol = ?"
    cursor.execute(query, (symbol,))
    
    result = cursor.fetchone()
    return result[0] if result and result[0] else None

def main(timeframe):
    # Validate the argument
    if timeframe not in ['d', 'w']:
        raise ValueError("Invalid timeframe argument. Use 'd' for daily or 'w' for weekly.")
    
    logging.info(f"Starting aggregation for timeframe: {'Daily' if timeframe == 'd' else 'Weekly'}")

    # Connect to the SQLite database
    conn = sqlite3.connect('trading_data.db')

    # Get distinct trading pairs in the 4-hour DB table
    logging.info("Fetching distinct trading pairs from the 4-hour table...")
    trading_pairs = get_distinct_trading_pairs(conn)

    if not trading_pairs:
        logging.info("No trading pairs found in the 4-hour table.")
    else:
        logging.info(f"Found {len(trading_pairs)} distinct trading pairs.")
        
        # Define the target table (daily or weekly)
        table = 'usdt_d' if timeframe == 'd' else 'usdt_w'

        for symbol in trading_pairs:
            logging.info(f"Processing symbol: {symbol}")

            # Step 3: Check the most recent candle time in the daily or weekly table
            last_candle_time = get_last_candle_time(conn, table, symbol)

            # If no data in the daily/weekly table, backfill from the first 4-hour candle
            if last_candle_time is None:
                logging.info(f"No existing candles in {table} for {symbol}. Backfilling from the first available 4-hour candle.")
                last_candle_time = get_first_4h_candle_time(conn, symbol)

            if last_candle_time is None:
                logging.warning(f"No available 4-hour candles to backfill for {symbol}. Skipping...")
            else:
                logging.info(f"Backfilling from time: {pd.to_datetime(last_candle_time, unit='ms')} for symbol: {symbol}")

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Candle Aggregation Script")
    parser.add_argument("timeframe", type=str, help="The aggregation timeframe ('d' for daily, 'w' for weekly)")
    args = parser.parse_args()

    # Call the main function
    main(args.timeframe)
