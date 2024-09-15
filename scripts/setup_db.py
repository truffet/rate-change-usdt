import sqlite3
import os

# Set the database file name
DB_FILE = "trading_data.db"

# Step 1: Check if the database file already exists
if os.path.exists(DB_FILE):
    print(f"Database '{DB_FILE}' already exists.")
else:
    # Step 2: Create a connection to the SQLite database (this creates the file if it doesn't exist)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Step 3: Create the usdt_4h table if it does not already exist
    print("Creating usdt_4h table...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS usdt_4h (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol VARCHAR(10) NOT NULL,       -- Trading pair symbol
        open_time TEXT NOT NULL,           -- Start time of the 4-hour candlestick
        close_time TEXT NOT NULL,          -- End time of the 4-hour candlestick
        open_price DECIMAL(18, 8) NOT NULL,-- Opening price
        high_price DECIMAL(18, 8) NOT NULL,-- Highest price
        low_price DECIMAL(18, 8) NOT NULL, -- Lowest price
        close_price DECIMAL(18, 8) NOT NULL,-- Closing price
        volume DECIMAL(18, 8) NOT NULL,    -- Volume traded during the 4-hour period
        quote_volume DECIMAL(18, 8),       -- Quote asset volume during the 4-hour period
        rate_change DECIMAL(18, 8),        -- Percentage rate change
        volume_zscore DECIMAL(18, 8),      -- Z-score of volume for this trading pair
        rate_change_zscore DECIMAL(18, 8), -- Z-score of rate change for this trading pair
        combined_zscore DECIMAL(18, 8),    -- Combined Z-score (volume Z-score * rate change Z-score)
        UNIQUE(symbol, open_time)          -- Ensure unique data per trading pair and time
    );
    ''')

    # Step 4: Commit the changes and close the connection
    conn.commit()
    conn.close()

    print(f"Database and usdt_4h table created successfully in '{DB_FILE}'.")
