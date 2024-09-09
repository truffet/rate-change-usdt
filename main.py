#!/usr/bin/env python3

import os
import json
import asyncio
from telegram import Bot
from data_fetcher.get_usdt_pairs import get_usdt_pairs
from data_fetcher.fetch_latest_market_data import fetch_latest_market_data
from data_processor.calculate_rate_change import calculate_rate_change
from data_processor.convert_volume_to_usdt import convert_volume_to_usdt
from data_processor.calculate_z_scores import calculate_z_scores, combine_z_scores
from data_processor.data_sort_by import data_sort_by
from io_operations.save_to_csv import save_to_csv
import pandas as pd

# Function to load config with absolute path
def load_config():
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Full path to config.json
    config_file = os.path.join(script_dir, 'config.json')
    
    # Load the configuration file
    with open(config_file, 'r') as file:
        config = json.load(file)
    return config

# Function to send a message via Telegram
async def send_telegram_message(bot_token, chat_id, message):
    bot = Bot(token=bot_token)
    await bot.send_message(chat_id=chat_id, text=message)

# Main async function
async def main():
    # Load configuration
    config = load_config()
    interval = config.get('interval', '4h')  # Default to '4h' if not found

    # Extract Telegram configuration
    telegram_config = config.get('telegram', {})
    bot_token = telegram_config.get('bot_token')
    chat_id = telegram_config.get('chat_id')

    # Get USDT pairs
    usdt_pairs = get_usdt_pairs()
    if not usdt_pairs:
        print("No USDT pairs fetched. Exiting.")
        return

    all_data = []
    timestamp = None  # Initialize timestamp

    # Fetch data for each USDT pair
    for pair in usdt_pairs:
        print(f"Fetching latest data for {pair}")  # Console print for debugging
        data = fetch_latest_market_data(pair, interval)
        if not data:
            print(f"Failed to fetch data for {pair}. Skipping.")
            continue
        all_data.append(data)
        print(f"Latest data for {pair} fetched successfully")  # Console print for debugging
        if not timestamp:  # Set timestamp from the first pair data
            timestamp = data['timestamp']

    if not all_data:
        print("No data fetched for any USDT pairs. Exiting.")
        return

    # Process data
    processed_data = calculate_rate_change(all_data)
    processed_data = convert_volume_to_usdt(processed_data)
    processed_data = calculate_z_scores(processed_data)
    processed_data = combine_z_scores(processed_data)

    # Sort data
    rate_sorted_data = data_sort_by(processed_data, 'pct_change')
    z_sorted_data = data_sort_by(processed_data, 'combined_z_score')

    # Filter data where combined Z-score > 2
    filtered_data = processed_data[processed_data['combined_z_score'] > 2]

    # Get the directory where this script is located for saving the data
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, 'data')

    # Ensure the data directory exists
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # Save the sorted data to CSV with timestamp in filenames
    rate_file = os.path.join(data_dir, f'{timestamp}_rate_sorted_market_data.csv')
    z_file = os.path.join(data_dir, f'{timestamp}_z_sorted_market_data.csv')
    filtered_file = os.path.join(data_dir, f'{timestamp}_filtered_market_data.csv')

    save_to_csv(rate_sorted_data, rate_file)
    save_to_csv(z_sorted_data, z_file)
    save_to_csv(filtered_data, filtered_file)

    # Read the filtered data from CSV and format it for Telegram
    filtered_data_df = pd.read_csv(filtered_file)
    formatted_message = f'{timestamp} Filtered Market Data (Z-score > 2):\n\n'
    for index, row in filtered_data_df.iterrows():
        formatted_message += f"{row['symbol']}: {row['combined_z_score']:.2f}\n"

    # Send Telegram message with the filtered data results
    if bot_token and chat_id:
        await send_telegram_message(
            bot_token=bot_token,
            chat_id=chat_id,
            message=formatted_message
        )

# Entry point for script execution
if __name__ == "__main__":
    asyncio.run(main())
