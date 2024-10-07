import logging
import sqlite3
from datetime import datetime, timedelta, timezone  # Ensure timedelta is imported and handle timezone
import asyncio
from src.api_client import BinanceAPI
from src.data_processor import DataProcessor

async def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the Rate Change USDT script.")

    # Create the API and DataProcessor objects
    api_client = BinanceAPI()
    data_processor = DataProcessor()

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
            # Handle timezone-aware datetime to avoid deprecation warning
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

    # Close the database connection
    conn.close()

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())  # Use asyncio to run the main function
