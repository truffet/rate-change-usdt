import sqlite3
import os

# Set the database file name
DB_FILE = "trading_data.db"

def create_table(cursor, table_name):
    """
    Create a table with the specified name and common schema.
    """
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol VARCHAR(10) NOT NULL,      -- Trading pair symbol
        open_time INTEGER NOT NULL,          -- Start time of the candlestick
        close_time INTEGER NOT NULL,         -- End time of the candlestick
        open_price DECIMAL(18, 8) NOT NULL,  -- Opening price
        high_price DECIMAL(18, 8) NOT NULL,  -- Highest price
        low_price DECIMAL(18, 8) NOT NULL,   -- Lowest price
        close_price DECIMAL(18, 8) NOT NULL, -- Closing price
        volume DECIMAL(18, 8) NOT NULL,      -- Volume traded during the time period
        quote_volume DECIMAL(18, 8),         -- Quote asset volume (in USDT)

        -- Two different rate changes: open/close and high/low
        rate_change_open_close DECIMAL(18, 8), -- Percentage rate change based on open/close prices
        rate_change_high_low DECIMAL(18, 8),   -- Percentage rate change based on high/low prices

        -- Pair-specific Z-scores (Z-scores based on the trading pair's own history)
        z_rate_change_open_close DECIMAL(18, 8), -- Z-score for rate change open/close
        z_rate_change_high_low DECIMAL(18, 8),   -- Z-score for rate change high/low
        z_volume_pair DECIMAL(18, 8),            -- Z-score of volume for the specific trading pair

        -- Cross-pair Z-scores (Z-scores based on comparison with other pairs)
        z_rate_change_open_close_all_pairs DECIMAL(18, 8), -- Z-score of rate change open/close across all pairs
        z_rate_change_high_low_all_pairs DECIMAL(18, 8),   -- Z-score of rate change high/low across all pairs
        z_volume_all_pairs DECIMAL(18, 8),        -- Z-score of volume across all pairs

        UNIQUE(symbol, open_time)  -- Ensure unique data per trading pair and time
    );
    ''')
    print(f"Table {table_name} created successfully!")

def main():
    # Step 1: Check if the database file already exists
    if os.path.exists(DB_FILE):
        print(f"Database '{DB_FILE}' already exists.")
        return
    
    # Step 2: Create a connection to the SQLite database (this creates the file if it doesn't exist)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Step 3: Create the necessary tables using the refactored function
    table_names = ['usdt_4h', 'usdt_d', 'usdt_w']  # Define all the table names
    for table in table_names:
        create_table(cursor, table)

    # Step 4: Commit the changes and close the connection
    conn.commit()
    conn.close()

    print(f"Database and tables {', '.join(table_names)} created successfully in '{DB_FILE}'.")

if __name__ == "__main__":
    main()
