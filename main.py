import logging
import asyncio
from src.config_loader import ConfigLoader
from src.api_client import BinanceAPI
from src.data_processor import DataProcessor
from src.telegram_client import TelegramBot
from src.utils import get_latest_window
import sqlite3

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def main():
    logging.info("Starting the Rate Change USDT script.")
    
    # Load configuration from config.json
    try:
        config = ConfigLoader.load_config()
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        return

    bot_token = config['telegram']['bot_token']
    chat_id = config['telegram']['chat_id']
    interval_str = config['interval']

    # Initialize the API client, data processor, and Telegram bot
    api_client = BinanceAPI(interval=interval_str)
    data_processor = DataProcessor()
    telegram_bot = TelegramBot(bot_token, chat_id)

    # Get the latest time window based on the interval from config
    start_time, end_time = get_latest_window(interval_str)

    # Open the database connection
    conn = sqlite3.connect('trading_data.db')

    # Fetch the actively traded USDT pairs
    usdt_pairs = api_client.get_usdt_pairs()

    # Calculate Z-scores for each pair (pair-specific and cross-pair)
    for symbol in usdt_pairs:
        # Pair-specific Z-scores
        pair_z_scores = data_processor.calculate_z_scores_for_pair(conn, symbol)
        if pair_z_scores is not None:
            data_processor.save_candlestick_data_to_db(pair_z_scores.to_dict(orient='records'))

    # Cross-pair Z-scores
    cross_pair_z_scores = data_processor.calculate_z_scores_across_pairs(conn)
    if cross_pair_z_scores is not None:
        data_processor.save_candlestick_data_to_db(cross_pair_z_scores.to_dict(orient='records'))

    conn.close()

    logging.info("Script completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
