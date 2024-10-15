import argparse
import logging
import sqlite3
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

    # Get the timestamp starting point for backfilling data from the last timestamp saved in db for a timeframe
    try:
        timestamp_cursor = database_handler.get_timestamp_cursor(conn, args.timeframe)
        logging.info(f"Timestamp cursor {args.timeframe}: {timestamp_cursor}")
    except ValueError as e:
        logging.error(f"Error fetching timestamp cursor: {e}")
        return

    # Proceed with further processing logic (fetching or aggregating data, etc.)

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    main()
