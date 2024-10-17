import argparse
import logging
import sqlite3
from datetime import datetime, timezone
import pandas as pd

from src.config_loader import ConfigLoader
from src.api_client import BinanceAPI
from src.data_processor import DataProcessor
from src.telegram_client import TelegramBot
from src.database_handler import DatabaseHandler

def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Process 4-hour, daily, or weekly data.")
    parser.add_argument('timeframe', choices=['4h', 'd', 'w'], help="Specify the timeframe: '4h' for 4-hour, 'd' for daily, or 'w' for weekly")

    # Parse the provided arguments
    args = parser.parse_args()

    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logging.info(f"Selected timeframe: {args.timeframe}")

    # Load configuration from config.json
    try:
        config = ConfigLoader.load_config()
        bot_token = config['telegram']['bot_token']
        chat_id = config['telegram']['chat_id']
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        return

    # Initialize the API client, DataProcessor, and TelegramBot based on config
    api_client = BinanceAPI(interval="4h")  # Passing the interval and initializing BinanceAPI
    data_processor = DataProcessor()        # Initialize DataProcessor
    telegram_bot = TelegramBot(bot_token, chat_id)  # Initialize TelegramBot with bot token and chat ID
    database_handler = DatabaseHandler()    # Initialize DatabaseHandler for DB interactions

    # Connect to the SQLite database
    conn = sqlite3.connect('trading_data.db')

    # Get all USDT pairs
    usdt_pairs = api_client.get_usdt_pairs()

    # Process each pair
    for symbol in usdt_pairs:
        logging.info(f"Processing {symbol}...")

        # Get the timestamp starting point for backfilling data from the last timestamp saved in db for a timeframe
        try:
            timestamp_cursor = database_handler.get_timestamp_cursor(conn, args.timeframe, symbol)
            logging.info(f"For {symbol}: timestamp cursor: {args.timeframe}: {timestamp_cursor}")
        except ValueError as e:
            logging.error(f"Error fetching timestamp cursor: {e}")
            return

        if args.timeframe == '4h':
            # Get current time in UTC as end_time
            end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
            # Fetch and backfill 4-hour candles from Binance API
            candle_data = api_client.fetch_candle_data(symbol, timestamp_cursor, end_time)
            if candle_data:
                logging.info(f"Fetched {len(candle_data)} 4-hour candles for {symbol} starting from timestamp {timestamp_cursor}.")
            else:
                logging.info(f"No new 4-hour candles available for {symbol}.")
    
        else:
            # Backfill aggregate by fetching 4-hour data or 1-day from the database (for daily or weekly)
            logging.info(f"Not implemented yet - wait for next iteration..")        #######################
            return
        if candle_data:
            # Convert candles to pandas DF, calculate rate change and save data to db
            df = data_processor.convert_candle_data_to_dataframe(candle_data)
            df = data_processor.calculate_rate_changes(df)
            database_handler.save_symbol_data_to_db(df.to_dict(orient='records'), conn, args.timeframe)

            # Calculate Z-scores for a specific pair and store to db table
            df_pair = data_processor.calculate_zscores_for_pair(conn, symbol, args.timeframe)
            database_handler.save_symbol_data_to_db(df_pair.to_dict(orient='records'), conn, args.timeframe)

    # Calculate Z-scores for all pairs (cross) and store to db table
    df_all_pairs = data_processor.calculate_zscores_for_all_pairs(conn, args.timeframe)
    database_handler.save_symbol_data_to_db(df_all_pairs.to_dict(orient='records'), conn, args.timeframe)

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    main()
