import sqlite3
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import argparse

def fetch_data_from_db(conn, symbol, num_units, grouping):
    """Fetch data from the daily or weekly table for a given symbol and units."""
    table = 'usdt_d' if grouping == 'd' else 'usdt_w'

    # Query the most recent num_units candles from the daily or weekly table
    query = f'''SELECT open_time, open_price, high_price, low_price, close_price, 
                        rate_change, volume, z_rate_change_pair, z_volume_pair, 
                        z_combined_pair, z_rate_change_all_pairs, z_volume_all_pairs, z_combined_all_pairs
                FROM {table}
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT ?'''
    df = pd.read_sql_query(query, conn, params=(symbol, num_units))

    return df

def plot_candlestick(fig, df, symbol, row):
    """Add the OHLC candlestick chart to the figure."""
    candle = go.Candlestick(x=df['open_time'],
                            open=df['open_price'],
                            high=df['high_price'],
                            low=df['low_price'],
                            close=df['close_price'],
                            name=f"{symbol} Candlestick")
    
    fig.add_trace(candle, row=row, col=1)
    
    fig.update_xaxes(title_text="Time", row=row, col=1)
    fig.update_yaxes(title_text="Price", row=row, col=1)

def plot_volume(fig, df, symbol, row):
    """Add the volume bar chart to the figure."""
    volume = go.Bar(x=df['open_time'], y=df['volume'], name=f"{symbol} Volume", marker_color='green')
    
    fig.add_trace(volume, row=row, col=1)
    
    fig.update_xaxes(title_text="Time", row=row, col=1)
    fig.update_yaxes(title_text="Volume", row=row, col=1)

def plot_z_scores(fig, df, symbol, start_row):
    """Add Z-score heatmaps for both pair-specific and cross-pair Z-scores to the figure."""
    z_pair_data = [df['z_rate_change_pair'], df['z_volume_pair'], df['z_combined_pair']]
    z_cross_data = [df['z_rate_change_all_pairs'], df['z_volume_all_pairs'], df['z_combined_all_pairs']]

    # Pair-specific Z-score heatmap with separate color scales
    for i, (z_data, name) in enumerate(zip(z_pair_data, ['Z-Rate Change (Pair)', 'Z-Volume (Pair)', 'Z-Combined (Pair)'])):
        heatmap = go.Heatmap(z=[z_data], x=df['open_time'], y=[name], colorscale='RdBu', zmin=-3, zmax=3, colorbar_title='Z-Scores')
        fig.add_trace(heatmap, row=start_row + i, col=1)

    # Cross-pair Z-score heatmap with separate color scales
    for i, (z_data, name) in enumerate(zip(z_cross_data, ['Z-Rate Change (Cross)', 'Z-Volume (Cross)', 'Z-Combined (Cross)'])):
        heatmap = go.Heatmap(z=[z_data], x=df['open_time'], y=[name], colorscale='RdBu', zmin=-3, zmax=3, colorbar_title='Z-Scores')
        fig.add_trace(heatmap, row=start_row + 3 + i, col=1)

def main(symbol, num_units, grouping):
    # Connect to the SQLite database
    conn = sqlite3.connect('trading_data.db')

    # Fetch data for the specified trading pair and grouping (daily or weekly)
    df = fetch_data_from_db(conn, symbol, num_units, grouping)

    # Convert open_time to datetime for better plotting
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')

    # Create a subplot figure
    fig = make_subplots(
        rows=9, cols=1,  # We need 9 rows (2 for OHLC and volume, 3 for pair-specific Z-scores, 3 for cross-pair Z-scores)
        shared_xaxes=True,  # Share the x-axis between all charts
        vertical_spacing=0.03,  # Adjust spacing between charts
        subplot_titles=[
            f"{symbol} OHLC Candlestick", f"{symbol} Volume",
            f"{symbol} Z-Rate Change (Pair)", f"{symbol} Z-Volume (Pair)", f"{symbol} Z-Combined (Pair)",
            f"{symbol} Z-Rate Change (Cross)", f"{symbol} Z-Volume (Cross)", f"{symbol} Z-Combined (Cross)"
        ]
    )

    # Add OHLC, Volume, and Z-score charts to the figure
    plot_candlestick(fig, df, symbol, row=1)
    plot_volume(fig, df, symbol, row=2)
    plot_z_scores(fig, df, symbol, start_row=3)

    # Update layout to ensure charts are well-aligned
    fig.update_layout(height=1800, title_text=f"{symbol} Analysis - OHLC, Volume, Z-Scores", showlegend=False)

    # Show the figure
    fig.show()

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot graphs for a given trading pair and time period from the database.")
    parser.add_argument("symbol", type=str, help="The symbol of the trading pair (e.g., BTCUSDT)")
    parser.add_argument("num_units", type=int, help="The number of units (e.g., 7 for days or weeks)")
    parser.add_argument("grouping", type=str, choices=['d', 'w'], help="The grouping ('d' for daily, 'w' for weekly)")
    args = parser.parse_args()

    main(args.symbol, args.num_units, args.grouping)
