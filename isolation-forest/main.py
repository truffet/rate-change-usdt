import os
import pandas as pd

from src.db_handler import DatabaseHandler
from src.data_processor import DataProcessor

# Database file path (one level up in the directory)
DB_FILE = os.path.join("..", "trading_data.db")

def main():
    """
    Main function to create the isolation forest table specifically for the 4-hour timeframe.
    """
    # Set the timeframe to '4h' as we're focusing on 4-hour data
    timeframe = "4h"
    
    # Initialize DatabaseHandler with the database file path and timeframe
    database_handler = DatabaseHandler(DB_FILE, timeframe)
    # Initialize DataProcessor class
    data_processor = DataProcessor()

    # Create the isolation forest table for the 4-hour timeframe
    database_handler.create_isolation_forest_table()

    # Fetch z-score data from the database specifically for the 4-hour timeframe
    df = database_handler.fetch_zscore_data()

    print("Database operation completed.")

if __name__ == "__main__":
    main()
