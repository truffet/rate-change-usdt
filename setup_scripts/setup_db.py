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

    # Step 3: Create the `usdt_4h` table if it does not already exist
    print("Creating usdt_4h table...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS usdt_4h (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol VARCHAR(10) NOT NULL,      -- Trading pair symbol
        open_time TEXT NOT NULL,          -- Start time of the 4-hour candlestick
        close_time TEXT NOT NULL,         -- End time of the 4-hour candlestick
        open_price DECIMAL(18, 8) NOT NULL,  -- Opening price
        high_price DECIMAL(18, 8) NOT NULL,  -- Highest price
        low_price DECIMAL(18, 8) NOT NULL,   -- Lowest price
        close_price DECIMAL(18, 8) NOT NULL, -- Closing price
        volume DECIMAL(18, 8) NOT NULL,      -- Volume traded during the 4-hour period
        quote_volume DECIMAL(18, 8),         -- Quote asset volume (in USDT)
        rate_change DECIMAL(18, 8),          -- Percentage rate change

        -- Pair-specific Z-scores (Z-scores based on the trading pair's own history)
        z_rate_change_pair DECIMAL(18, 8),   -- Z-score of rate change for the specific trading pair
        z_volume_pair DECIMAL(18, 8),        -- Z-score of volume for the specific trading pair
        z_combined_pair DECIMAL(18, 8),      -- Combined Z-score for the specific trading pair

        -- Cross-pair Z-scores (Z-scores based on comparison with other pairs)
        z_rate_change_all_pairs DECIMAL(18, 8),   -- Z-score of rate change across all pairs
        z_volume_all_pairs DECIMAL(18, 8),        -- Z-score of volume across all pairs
        z_combined_all_pairs DECIMAL(18, 8),      -- Combined Z-score across all pairs

        UNIQUE(symbol, open_time)  -- Ensure unique data per trading pair and time
    );
    ''')

    # Step 4: Create the `usdt_d` table for daily candlesticks
    print("Creating usdt_d table (daily candles)...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS usdt_d (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol VARCHAR(10) NOT NULL,      -- Trading pair symbol
        open_time TEXT NOT NULL,          -- Start time of the daily candlestick
        close_time TEXT NOT NULL,         -- End time of the daily candlestick
        open_price DECIMAL(18, 8) NOT NULL,  -- Opening price
        high_price DECIMAL(18, 8) NOT NULL,  -- Highest price
        low_price DECIMAL(18, 8) NOT NULL,   -- Lowest price
        close_price DECIMAL(18, 8) NOT NULL, -- Closing price
        volume DECIMAL(18, 8) NOT NULL,      -- Volume traded during the day
        quote_volume DECIMAL(18, 8),         -- Quote asset volume (in USDT)
        rate_change DECIMAL(18, 8),          -- Percentage rate change

        -- Pair-specific Z-scores (Z-scores based on the trading pair's own history)
        z_rate_change_pair DECIMAL(18, 8),   -- Z-score of rate change for the specific trading pair
        z_volume_pair DECIMAL(18, 8),        -- Z-score of volume for the specific trading pair
        z_combined_pair DECIMAL(18, 8),      -- Combined Z-score for the specific trading pair

        -- Cross-pair Z-scores (Z-scores based on comparison with other pairs)
        z_rate_change_all_pairs DECIMAL(18, 8),   -- Z-score of rate change across all pairs
        z_volume_all_pairs DECIMAL(18, 8),        -- Z-score of volume across all pairs
        z_combined_all_pairs DECIMAL(18, 8),      -- Combined Z-score across all pairs

        UNIQUE(symbol, open_time)  -- Ensure unique data per trading pair and time
    );
    ''')

    # Step 5: Create the `usdt_w` table for weekly candlesticks
    print("Creating usdt_w table (weekly candles)...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS usdt_w (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol VARCHAR(10) NOT NULL,      -- Trading pair symbol
        open_time TEXT NOT NULL,          -- Start time of the weekly candlestick
        close_time TEXT NOT NULL,         -- End time of the weekly candlestick
        open_price DECIMAL(18, 8) NOT NULL,  -- Opening price
        high_price DECIMAL(18, 8) NOT NULL,  -- Highest price
        low_price DECIMAL(18, 8) NOT NULL,   -- Lowest price
        close_price DECIMAL(18, 8) NOT NULL, -- Closing price
        volume DECIMAL(18, 8) NOT NULL,      -- Volume traded during the week
        quote_volume DECIMAL(18, 8),         -- Quote asset volume (in USDT)
        rate_change DECIMAL(18, 8),          -- Percentage rate change

        -- Pair-specific Z-scores (Z-scores based on the trading pair's own history)
        z_rate_change_pair DECIMAL(18, 8),   -- Z-score of rate change for the specific trading pair
        z_volume_pair DECIMAL(18, 8),        -- Z-score of volume for the specific trading pair
        z_combined_pair DECIMAL(18, 8),      -- Combined Z-score for the specific trading pair

        -- Cross-pair Z-scores (Z-scores based on comparison with other pairs)
        z_rate_change_all_pairs DECIMAL(18, 8),   -- Z-score of rate change across all pairs
        z_volume_all_pairs DECIMAL(18, 8),        -- Z-score of volume across all pairs
        z_combined_all_pairs DECIMAL(18, 8),      -- Combined Z-score across all pairs

        UNIQUE(symbol, open_time)  -- Ensure unique data per trading pair and time
    );
    ''')

    # Step 6: Commit the changes and close the connection
    conn.commit()
    conn.close()

    print(f"Database and `usdt_4h`, `usdt_d`, `usdt_w` tables created successfully in '{DB_FILE}'.")
