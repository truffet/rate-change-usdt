import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import argparse
from datetime import datetime, timedelta

def fetch_data_from_db(conn, symbol, num_units, grouping):
    """Fetch data for a given number of units (days, weeks, or months) and a symbol from the SQLite database."""
    # Get the current date
    now = datetime.now()

    if grouping == 'd':
        start_time = now - timedelta(days=num_units)
    elif grouping == 'w':
        start_time = now - timedelta(weeks=num_units)
    elif grouping == 'm':
        # Calculate the date `num_units` months ago
        month_diff = num_units
        year = now.year
        month = now.month

        for _ in range(month_diff):
            if month == 1:
                year -= 1
                month = 12
            else:
                month -= 1
        
        start_time = datetime(year, month, 1)  # Start at the 1st day of the month
    else:
        raise ValueError("Invalid grouping. Use 'd' for daily, 'w' for weekly, or 'm' for monthly.")

    # Convert start_time to milliseconds
    start_time_ms = int(start_time.timestamp() * 1000)
    end_time_ms = int(now.timestamp() * 1000)

    # Query data between start_time and now
    query = '''SELECT open_time, open_price, high_price, low_price, close_price, 
                      rate_change, volume, z_rate_change_pair, z_volume_pair, 
                      z_combined_pair, z_rate_change_all_pairs, z_volume_all_pairs, z_combined_all_pairs
               FROM usdt_4h 
               WHERE symbol = ? AND open_time BETWEEN ? AND ?
               ORDER BY open_time ASC'''
    df = pd.read_sql_query(query, conn, params=(symbol, start_time_ms, end_time_ms))
    
    return df

def plot_candlestick(df, symbol):
    """Plot the 4-hour candlestick chart."""
    plt.figure(figsize=(10, 6))
    plt.plot(df['open_time'], df['open_price'], label='Open Price')
    plt.plot(df['open_time'], df['close_price'], label='Close Price')
    plt.fill_between(df['open_time'], df['low_price'], df['high_price'], color='gray', alpha=0.3, label='Price Range')
    plt.title(f"{symbol} 4-hour Candlestick Chart")
    plt.xlabel("Time")
    plt.ylabel("Price")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_rate_change(df, symbol):
    """Plot the rate change chart."""
    plt.figure(figsize=(10, 6))
    plt.plot(df['open_time'], df['rate_change'], label='Rate Change', color='blue')
    plt.title(f"{symbol} Rate Change (%) Over Time")
    plt.xlabel("Time")
    plt.ylabel("Rate Change (%)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_volume(df, symbol):
    """Plot the volume chart."""
    plt.figure(figsize=(10, 6))
    plt.plot(df['open_time'], df['volume'], label='Volume', color='green')
    plt.title(f"{symbol} Volume Over Time")
    plt.xlabel("Time")
    plt.ylabel("Volume (USDT)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_z_scores(df, symbol):
    """Plot the Z-scores (Pair-specific and Cross-pair) on different graphs but the same page."""
    # Create subplots for pair-specific Z-scores
    fig, axs = plt.subplots(3, 1, figsize=(10, 10))
    fig.suptitle(f"{symbol} Pair-Specific Z-Scores", fontsize=16)

    axs[0].plot(df['open_time'], df['z_rate_change_pair'], label='Z-Rate Change (Pair)', color='red')
    axs[0].set_ylabel("Z-Rate Change (Pair)")
    axs[0].set_xticks([])

    axs[1].plot(df['open_time'], df['z_volume_pair'], label='Z-Volume (Pair)', color='orange')
    axs[1].set_ylabel("Z-Volume (Pair)")
    axs[1].set_xticks([])

    axs[2].plot(df['open_time'], df['z_combined_pair'], label='Z-Combined (Pair)', color='purple')
    axs[2].set_ylabel("Z-Combined (Pair)")
    axs[2].set_xlabel("Time")

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.subplots_adjust(top=0.88)
    plt.show()

    # Create subplots for cross-pair Z-scores
    fig, axs = plt.subplots(3, 1, figsize=(10, 10))
    fig.suptitle(f"{symbol} Cross-Pair Z-Scores", fontsize=16)

    axs[0].plot(df['open_time'], df['z_rate_change_all_pairs'], label='Z-Rate Change (Cross-Pair)', color='red')
    axs[0].set_ylabel("Z-Rate Change (Cross-Pair)")
    axs[0].set_xticks([])

    axs[1].plot(df['open_time'], df['z_volume_all_pairs'], label='Z-Volume (Cross-Pair)', color='orange')
    axs[1].set_ylabel("Z-Volume (Cross-Pair)")
    axs[1].set_xticks([])

    axs[2].plot(df['open_time'], df['z_combined_all_pairs'], label='Z-Combined (Cross-Pair)', color='purple')
    axs[2].set_ylabel("Z-Combined (Cross-Pair)")
    axs[2].set_xlabel("Time")

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.subplots_adjust(top=0.88)
    plt.show()

def main(symbol, num_units, grouping):
    # Connect to the SQLite database
    conn = sqlite3.connect('trading_data.db')

    # Fetch data for the specified trading pair and grouping (d, w, or m)
    df = fetch_data_from_db(conn, symbol, num_units, grouping)

    # Convert open_time to datetime for better plotting
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')

    # Plot the different charts
    plot_candlestick(df, symbol)
    plot_rate_change(df, symbol)
    plot_volume(df, symbol)
    plot_z_scores(df, symbol)

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot graphs for a given trading pair and time period from the database.")
    parser.add_argument("symbol", type=str, help="The symbol of the trading pair (e.g., BTCUSDT)")
    parser.add_argument("num_units", type=int, help="The number of units (e.g., 7 for days, weeks, or months)")
    parser.add_argument("grouping", type=str, choices=['d', 'w', 'm'], help="The grouping ('d' for days, 'w' for weeks, 'm' for months)")
    args = parser.parse_args()

    main(args.symbol, args.num_units, args.grouping)
