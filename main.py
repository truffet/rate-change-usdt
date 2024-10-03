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
        # Fetch the latest candlestick data for each symbol
        latest_candlestick = api_client.fetch_candlestick_data_by_time(symbol, None, None)

        if not latest_candlestick:
            logging.error(f"No latest candlestick data found for {symbol}.")
            continue

        # Check for missing data, backfill if necessary
        missing_intervals = data_processor.check_and_clean_data(conn, symbol, latest_candlestick[-1][6])

        if missing_intervals:
            data_processor.backfill_missing_data(api_client, conn, symbol, missing_intervals)

        # Re-fetch complete data after backfilling
        df = data_processor.calculate_z_scores_for_pair(conn, symbol)

        # Save the processed data into the database
        data_processor.save_candlestick_data_to_db(df.to_dict(orient='records'), conn)

    # Close the connection
    conn.close()

    logging.info("Script completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())  # Use asyncio to run the main function
