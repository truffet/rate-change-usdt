import logging
import asyncio
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
        config = ConfigLoader.load_config()
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

    # Fetch USDT pairs and their last candlestick data
    usdt_pairs_with_candlestick_data = api_client.get_usdt_pairs_and_candlestick_data()

    # Process all candlestick data and get open/close times
    processed_data, open_time, close_time = data_processor.process_all_candlestick_data(usdt_pairs_with_candlestick_data)

    # Calculate and combine z-scores using the DataProcessor
    df = data_processor.calculate_z_scores(processed_data)
    df = data_processor.combine_z_scores(df)

    # Save candlestick data (including Z-scores) to the database
    data_processor.save_candlestick_data_to_db(df.to_dict(orient='records'))

    # Filter by combined z-scores with absolute value > 2
    df = df[abs(df['z_combined']) > 2]

    # Send the candlestick summary message using TelegramBot (async)
    await telegram_bot.send_candlestick_summary(df, open_time, close_time)

    logging.info("Script completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())  # Use asyncio to run the main function
