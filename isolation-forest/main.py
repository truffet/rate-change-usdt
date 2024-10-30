import argparse
import sqlite3
from db_table_setup import create_isolation_forest_table

# Database file name
DB_FILE = "trading_data.db"

def main(timeframe):
    """
    Main function to create the isolation forest table for the specified timeframe.

    Args:
        timeframe (str): The timeframe for which to create the table ('4h', 'd', or 'w').
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(DB_FILE)

    # Call the function to create the table
    create_isolation_forest_table(conn, timeframe)

    # Close the database connection
    conn.close()
    print(f"Database connection closed.")

if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Create isolation forest table for specified timeframe.")
    parser.add_argument("timeframe", choices=["4h", "d", "w"], help="Specify the timeframe: '4h', 'd', or 'w'")
    
    # Parse the arguments
    args = parser.parse_args()

    # Run the main function with the specified timeframe
    main(args.timeframe)
