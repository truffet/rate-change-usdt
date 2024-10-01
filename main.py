import logging
import asyncio
from src.config_loader import ConfigLoader
from src.api_client import BinanceAPI
from src.data_processor import DataProcessor
from src.telegram_client import TelegramBot
from src.utils import get_latest_window  # Helper function to calculate time window

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
    interval_str = config['interval']  # Load interval as a string (e.g., "4h")

    # Initialize the API client, data processor, and Telegram bot
    api_client = BinanceAPI(interval=interval_str)  # Pass the interval as a string (e.g., "4h")
    data_processor = DataProcessor()
    telegram_bot = TelegramBot(bot_token, chat_id)

    # Get the latest time window based on the interval from config
    start_time, end_time = get_latest_window(interval_str)

    # Fetch the actively traded USDT pairs
    usdt_pairs = api_client.get_usdt_pairs()

    # Fetch and process data for each pair
    candlestick_data_list = []
    for symbol in usdt_pairs:
        processed_data = data_processor.fetch_and_process_historical_data(api_client, symbol, start_time, end_time)
        if processed_data:
            candlestick_data_list.append(processed_data)

    # Calculate Z-scores for the fetched data
    df = data_processor.calculate_z_scores(candlestick_data_list)
    df = data_processor.combine_z_scores(df)

    # Store the data in the database
    data_processor.save_candlestick_data_to_db(df.to_dict(orient='records'))

    # Post results to Telegram
    await telegram_bot.send_candlestick_summary(df, start_time, end_time)

    logging.info("Script completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())  # Use asyncio to run the main function
