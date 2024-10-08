import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import argparse

def fetch_data_from_db(conn, symbol, num_days):
    """Fetch data for the last `num_days` for a given trading pair from the SQLite database."""
    # Calculate how many 4-hour candles we need to fetch
    num_candles = num_days * 6

    # Query the last `num_candles` for the given symbol, ordered by open_time in descending order
    query = '''SELECT open_time, open_price, high_price, low_price, close_price, 
                      rate_change, volume, z_rate_change_pair, z_volume_pair, 
                      z_combined_pair, z_rate_change_all_pairs, z_volume_all_pairs, z_combined_all_pairs
               FROM usdt_4h 
               WHERE symbol = ? 
               ORDER BY open_time DESC
               LIMIT ?'''
    df = pd.read_sql_query(query, conn, params=(symbol, num_candles))

    # Reverse the order so the oldest candles come first
    df = df.iloc[::-1].reset_index(drop=True)
    
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
    """Plot the Z-scores (Pair-specific and Cross-pair)."""
    plt.figure(figsize=(10, 6))
    plt.plot(df['open_time'], df['z_rate_change_pair'], label='Z-Rate Change (Pair)', color='red')
    plt.plot(df['open_time'], df['z_volume_pair'], label='Z-Volume (Pair)', color='orange')
    plt.plot(df['open_time'], df['z_combined_pair'], label='Z-Combined (Pair)', color='purple')
    plt.title(f"{symbol} Pair-Specific Z-Scores Over Time")
    plt.xlabel("Time")
    plt.ylabel("Z-Scores")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(10, 6))
    plt.plot(df['open_time'], df['z_rate_change_all_pairs'], label='Z-Rate Change (Cross-Pair)', color='red')
    plt.plot(df['open_time'], df['z_volume_all_pairs'], label='Z-Volume (Cross-Pair)', color='orange')
    plt.plot(df['open_time'], df['z_combined_all_pairs'], label='Z-Combined (Cross-Pair)', color='purple')
    plt.title(f"{symbol} Cross-Pair Z-Scores Over Time")
    plt.xlabel("Time")
    plt.ylabel("Z-Scores")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def main(symbol, num_days):
    # Connect to the SQLite database
    conn = sqlite3.connect('trading_data.db')

    # Fetch data for the specified trading pair and number of days
    df = fetch_data_from_db(conn, symbol, num_days)

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
    parser = argparse.ArgumentParser(description="Plot graphs for a given trading pair and number of days from the database.")
    parser.add_argument("symbol", type=str, help="The symbol of the trading pair (e.g., BTCUSDT)")
    parser.add_argument("num_days", type=int, help="The number of days to fetch data for (e.g., 7)")
    args = parser.parse_args()

    main(args.symbol, args.num_days)
