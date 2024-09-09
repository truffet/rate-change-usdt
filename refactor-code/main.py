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

    rate_changes = []
    volumes = []
    symbols = []
    combined_z_scores_list = []
    open_time = None
    close_time = None

    # Process each USDT pair's data
    for usdt_pair in usdt_pairs:
        candlestick_data = api_client.fetch_last_completed_candlestick(usdt_pair)

        if not candlestick_data:
            logging.warning(f"No candlestick data available for {usdt_pair}. Skipping.")
            continue

        # Calculate rate change based on open and close prices
        rate_change = data_processor.calculate_rate_change(candlestick_data)

        # Get the volume in USDT (quote asset volume, index 7)
        volume_in_usdt = float(candlestick_data[7])

        if rate_change is None:
            logging.warning(f"Failed to calculate rate change for {usdt_pair}. Skipping.")
            continue

        # Set open and close times (once, since they are the same for all pairs)
        if not open_time:
            open_time = datetime.fromtimestamp(candlestick_data[0] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            close_time = datetime.fromtimestamp(candlestick_data[6] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        # Append for z-score calculations
        rate_changes.append(rate_change)
        volumes.append(volume_in_usdt)  # Add the volume in USDT
        symbols.append(usdt_pair)

    # Calculate z-scores for rate changes and volumes
    rate_z_scores = data_processor.calculate_z_scores(rate_changes)
    volume_z_scores = data_processor.calculate_z_scores(volumes)

    # Combine z-scores by multiplying them
    combined_z_scores = data_processor.calculate_multiplied_z_scores(rate_z_scores, volume_z_scores)

    # Collect the data and sort by absolute value of combined z-score
    combined_z_scores_list = [
        {
            "pair": symbols[i],
            "rate_change": rate_changes[i],
            "volume": volumes[i],
            "rate_z_score": rate_z_scores[i],
            "volume_z_score": volume_z_scores[i],
            "combined_z_score": combined_z_scores[i]
        }
        for i in range(len(symbols))
        if abs(combined_z_scores[i]) > 2  # Only include z-scores with absolute value > 2
    ]

    # Sort by absolute value of combined z-score (highest to lowest)
    sorted_results = sorted(combined_z_scores_list, key=lambda x: abs(x["combined_z_score"]), reverse=True)

    # Prepare the entire message to send at once
    full_message = f"📅 **Candlestick Data**\nOpen Time: {open_time} | Close Time: {close_time}\n\n"

    for result in sorted_results:
        rate_change_icon = "🔺" if result["rate_change"] > 0 else "🔻"
        volume_icon = "🟩" if result["rate_change"] > 0 else "🟥"

        full_message += (
            f"💲 {result['pair']} {rate_change_icon}{result['rate_change']:.2f}% {volume_icon}{result['volume']:.0f} USDT "
            f"| R-Z: {result['rate_z_score']:.2f} | V-Z: {result['volume_z_score']:.2f} | C-Z: {result['combined_z_score']:.2f}\n"
        )

    # Send the entire message as a single Telegram message
    try:
        telegram_bot.send_message(full_message)
    except Exception as e:
        logging.error(f"Failed to send message: {e}")

    logging.info("Script completed successfully.")

if __name__ == "__main__":
    main()
