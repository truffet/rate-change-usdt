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
    
    try:
        config = ConfigLoader.load_config()  # Load and validate the config
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        return

    bot_token = config['telegram']['bot_token']
    chat_id = config['telegram']['chat_id']
    interval = config['interval']

    api_client = BinanceAPI(interval=interval)
    data_processor = DataProcessor()
    telegram_bot = TelegramBot(bot_token, chat_id)

    conn = sqlite3.connect('trading_data.db')

    usdt_pairs = api_client.get_usdt_pairs()

    for symbol in usdt_pairs:
        logging.info(f"Processing {symbol}...")

        latest_time_in_db = data_processor.get_latest_time_in_db(conn, symbol)
        most_recent_candle = api_client.get_most_recent_candle_time()

        if latest_time_in_db is None:
            logging.info(f"No data found in DB for {symbol}. Starting backfill from a year ago.")
            start_time = most_recent_candle - (365 * 24 * 60 * 60 * 1000)  # Start 1 year ago
        else:
            start_time = latest_time_in_db

        if most_recent_candle and start_time:
            data_processor.backfill_missing_data(api_client, conn, symbol, start_time, most_recent_candle)

        df = data_processor.calculate_z_scores_for_pair(conn, symbol)
        data_processor.save_candlestick_data_to_db(df.to_dict(orient='records'), conn)

    conn.close()
    logging.info("Script completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
