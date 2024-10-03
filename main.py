import logging
import asyncio
import sqlite3
from src.config_loader import ConfigLoader
from src.api_client import BinanceAPI
from src.data_processor import DataProcessor
from src.telegram_client import TelegramBot

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def main():
    logging.info("Starting the Rate Change USDT script.")
    
    # Load configuration from config.json
    try:
        config = ConfigLoader.load_config()  # Load and validate the config
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        return

    bot_token = config['telegram']['bot_token']
    chat_id = config['telegram']['chat_id']
    interval = config['interval']

    # Initialize the API client, data processor, and Telegram bot
    api_client = BinanceAPI(interval=interval)
    data_processor = DataProcessor()
    telegram_bot = TelegramBot(bot_token, chat_id)

    # Create SQLite connection
    conn = sqlite3.connect('trading_data.db')

    # Fetch actively traded USDT pairs
    usdt_pairs = api_client.get_usdt_pairs()

    for symbol in usdt_pairs:
        # Fetch the most recent completed candlestick time
        most_recent_candle = api_client.get_most_recent_candle_time()

        # Get the latest time in the database for this symbol
        latest_time_in_db = data_processor.get_latest_time_in_db(conn, symbol)

        # If no data exists in the DB, set latest_time_in_db to 1 year before the most recent candle
        if latest_time_in_db is None:
            latest_time_in_db = most_recent_candle - 365 * 24 * 60 * 60 * 1000  # 1 year in milliseconds

        logging.info(f"Backfilling data for {symbol} from {latest_time_in_db} to {most_recent_candle}.")

        # Backfill data for missing intervals
        data_processor.backfill_missing_data(api_client, conn, symbol, latest_time_in_db, most_recent_candle)

        # Re-fetch complete data after backfilling
        df = data_processor.calculate_z_scores_for_pair(conn, symbol)

        # Save the processed data into the database
        data_processor.save_candlestick_data_to_db(df.to_dict(orient='records'), conn)

    # Close the connection
    conn.close()

    logging.info("Script completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())  # Use asyncio to run the main function
