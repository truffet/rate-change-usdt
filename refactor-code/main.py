# main.py

import logging
from datetime import datetime, timezone
from src.config_loader import ConfigLoader
from src.api_client import BinanceAPI
from src.data_processor import DataProcessor
from src.telegram_client import TelegramBot

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

    candlestick_data_list = []
    open_time = None
    close_time = None

    # Process each USDT pair's data
    for usdt_pair in usdt_pairs:
        candlestick_data = api_client.fetch_last_completed_candlestick(usdt_pair)

        if not candlestick_data:
            logging.warning(f"No candlestick data available for {usdt_pair}. Skipping.")
            continue

        # Calculate rate change based on open and close prices
        rate_change = (float(candlestick_data[4]) - float(candlestick_data[1])) / float(candlestick_data[1]) * 100

        # Get the volume in USDT (quote asset volume, index 7)
        volume_in_usdt = float(candlestick_data[7])

        # Set open and close times (once, since they are the same for all pairs)
        if not open_time:
            open_time = datetime.fromtimestamp(candlestick_data[0] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            close_time = datetime.fromtimestamp(candlestick_data[6] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        # Collect data for each pair
        candlestick_data_list.append({
            "pair": usdt_pair,
            "pct_change": rate_change,
            "volume_usdt": volume_in_usdt
        })

    # Calculate and combine z-scores using the DataProcessor
    df = data_processor.calculate_z_scores(candlestick_data_list)
    df = data_processor.combine_z_scores(df)

    # Filter by combined z-scores with absolute value > 2
    df = df[abs(df['combined_z_score']) > 2]

    # Prepare message for Telegram
    full_message = f"📅 **Candlestick Data**\nOpen Time: {open_time} | Close Time: {close_time}\n\n"
    for _, row in df.iterrows():
        rate_change_icon = "🔺" if row['pct_change'] > 0 else "🔻"
        volume_icon = "🟩" if row['pct_change'] > 0 else "🟥"
        full_message += (
            f"💲 {row['pair']} {rate_change_icon}{row['pct_change']:.2f}% {volume_icon}{row['volume_usdt']:.0f} USDT "
            f"| R-Z: {row['z_pct_change']:.2f} | V-Z: {row['z_volume_usdt']:.2f} | C-Z: {row['combined_z_score']:.2f}\n"
        )

    # Send the entire message as a single Telegram message
    try:
        telegram_bot.send_message(full_message)
    except Exception as e:
        logging.error(f"Failed to send message: {e}")

    logging.info("Script completed successfully.")

if __name__ == "__main__":
    main()
