import logging
import sqlite3
from datetime import datetime, timedelta, timezone
import pandas as pd
import asyncio
from src.config_loader import ConfigLoader  # Load config loader to fetch from config.json
from src.api_client import BinanceAPI
from src.data_processor import DataProcessor
from src.telegram_client import TelegramBot  # Import the TelegramBot class

async def main():
    logging.basicConfig(level=logging.INFO)
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

    # Connect to the SQLite database
    conn = sqlite3.connect('trading_data.db')

    # Get all USDT pairs
    usdt_pairs = api_client.get_usdt_pairs()
    logging.info(f"Successfully fetched {len(usdt_pairs)} USDT pairs.")

    # Process each pair
    for symbol in usdt_pairs:
        logging.info(f"Processing {symbol}...")

        # Check the latest time in the database
        latest_time_in_db = data_processor.get_latest_time_in_db(conn, symbol)

        # If there's no data, backfill from one year ago
        if not latest_time_in_db:
            logging.info(f"No data found in DB for {symbol}. Starting backfill from a year ago.")
            start_time = int((datetime.now(timezone.utc) - timedelta(days=365)).timestamp() * 1000)
        else:
            start_time = latest_time_in_db

        # Get the most recent completed candlestick time
        most_recent_candle = api_client.get_most_recent_candle_time()
        if most_recent_candle is None:
            logging.error(f"Failed to fetch most recent candlestick for {symbol}. Skipping...")
            continue

        # Backfill missing data
        data_processor.backfill_missing_data(api_client, conn, symbol, start_time, most_recent_candle)

    # After all pairs have been backfilled, calculate Z-scores
    for symbol in usdt_pairs:
        df = data_processor.calculate_z_scores_for_last_completed_candle(conn, symbol)
        data_processor.save_candlestick_data_to_db(df.to_dict(orient='records'), conn)

    # Fetch the latest completed candlestick data for all pairs
    query_last_candle = '''SELECT * FROM usdt_4h WHERE open_time = (SELECT MAX(open_time) FROM usdt_4h)'''
    df = pd.read_sql_query(query_last_candle, conn)

    # Filter pairs where either combined Z-score has an absolute value >= 2
    filtered_df = df[
        (df['z_combined_pair'].abs() >= 2) | (df['z_combined_all_pairs'].abs() >= 2)
    ]

    # Send filtered Z-scores data to Telegram
    if not filtered_df.empty:
        latest_open_time = pd.to_datetime(filtered_df['open_time'].max(), unit='ms')
        latest_close_time = pd.to_datetime(filtered_df['close_time'].max(), unit='ms')
        await telegram_bot.send_candlestick_summary(filtered_df, latest_open_time, latest_close_time)
    else:
        logging.info("No pairs meet the Z-score threshold, no message sent.")

    # Close the database connection
    conn.close()

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())  # Use asyncio to run the main function
