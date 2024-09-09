# main.py

import logging
from src.config_loader import ConfigLoader  # Handles loading and validating the configuration
from src.api_client import BinanceAPI  # Handles fetching data from the Binance API
from src.data_processor import DataProcessor  # Handles data processing (rate changes, z-scores)
from src.telegram_client import TelegramBot  # Handles sending messages to the Telegram bot

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]  # Outputs logs to the console; can also add file handler if needed
)

def main():
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

    # Fetch actively traded USDT pairs
    usdt_pairs = api_client.get_usdt_pairs()

    # Process each USDT pair's data
    for usdt_pair in usdt_pairs:
        candles = api_client.fetch_candlestick_data(usdt_pair)
        if not candles:
            logging.warning(f"No candlestick data available for {usdt_pair}. Skipping.")
            continue

        rate_change = data_processor.calculate_rate_change(candles)
        if rate_change is None:
            logging.warning(f"Failed to calculate rate change for {usdt_pair}. Skipping.")
            continue

        z_scores = data_processor.calculate_z_scores([rate_change])

        # Prepare and send the result message to Telegram
        message = f"Rate Change for {usdt_pair}: {rate_change}%\nZ-Scores: {z_scores}"
        try:
            telegram_bot.send_message(message)
        except Exception as e:
            logging.error(f"Failed to send message for {usdt_pair}: {e}")

    logging.info("Script completed successfully.")

if __name__ == "__main__":
    main()
