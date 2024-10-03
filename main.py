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

    # Get the most recent completed 4-hour candle
    most_recent_candle = api_client.get_most_recent_candle_time()

    for symbol in usdt_pairs:
        # Get the latest time in DB for this symbol
        latest_time_in_db = data_processor.get_latest_time_in_db(conn, symbol)

        if latest_time_in_db is None:
            logging.info(f"No data found for {symbol}, backfilling entire year of data.")
            # If no data is found in DB, backfill the past year
            one_year_ago = most_recent_candle - 365 * 24 * 3600 * 1000  # Subtract one year in milliseconds
            latest_time_in_db = one_year_ago

        # Backfill missing data
        data_processor.backfill_missing_data(api_client, conn, symbol, latest_time_in_db, most_recent_candle)

        # Calculate Z-scores for the specific pair and all pairs
        df = data_processor.calculate_z_scores_for_pair(conn, symbol)

        # Save the processed data into the database
        data_processor.save_candlestick_data_to_db(df.to_dict(orient='records'), conn)

    # Close the connection
    conn.close()

    logging.info("Script completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())  # Use asyncio to run the main function
