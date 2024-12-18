import sqlite3
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def fetch_data_from_db(conn, symbol, num_units, grouping):
    """Fetch data from the daily, weekly, or 4h table for a given symbol and units."""
    if grouping == 'd':
        table = 'usdt_d'
    elif grouping == 'w':
        table = 'usdt_w'
    elif grouping == '4h':
        table = 'usdt_4h'
    else:
        raise ValueError("Invalid grouping. Supported values are 'd', 'w', '4h'.")

    # Query the most recent num_units candles from the specified table
    query = f'''SELECT open_time, open_price, high_price, low_price, close_price, volume, 
                        z_rate_change_open_close, z_rate_change_high_low, z_volume_pair, 
                        z_rate_change_open_close_all_pairs, z_rate_change_high_low_all_pairs, z_volume_all_pairs
                FROM {table}
                WHERE symbol = ?
                ORDER BY open_time DESC
                LIMIT ?'''
    df = pd.read_sql_query(query, conn, params=(symbol, num_units))

    return df

def plot_ohlcv_with_z_scores(df, symbol):
    """Create OHLC candlestick chart, volume chart, and Z-score heatmaps for pair and cross-pairs."""
    # Convert open_time to numeric and then to datetime
    df['open_time'] = pd.to_numeric(df['open_time'])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')

    # Create a subplot with 9 rows: OHLC, Volume, and 6 Z-scores (Pair and Cross-pairs)
    fig = make_subplots(rows=9, cols=1, shared_xaxes=True,
                        row_heights=[0.4, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2], vertical_spacing=0.03,
                        specs=[[{}], [{}], [{}], [{}], [{}], [{}], [{}], [{}], [{}]],
                        subplot_titles=[f"{symbol} OHLC Candlestick",
                                        f"{symbol} Volume",
                                        "Z-Rate Change Open-Close (Pair)", 
                                        "Z-Rate Change High-Low (Pair)",
                                        "Z-Volume (Pair)",
                                        "Z-Rate Change Open-Close (Cross)", 
                                        "Z-Rate Change High-Low (Cross)",
                                        "Z-Volume (Cross)"])

    # OHLC candlestick chart
    fig.add_trace(go.Candlestick(x=df['open_time'],
                                 open=df['open_price'],
                                 high=df['high_price'],
                                 low=df['low_price'],
                                 close=df['close_price'],
                                 name='OHLC'),
                  row=1, col=1)

    # Volume bar chart
    fig.add_trace(go.Bar(x=df['open_time'], y=df['volume'], name='Volume', 
                         marker_color='green'),
                  row=2, col=1)

    # Z-Rate Change Open-Close (Pair) heatmap
    fig.add_trace(go.Heatmap(z=[df['z_rate_change_open_close']], x=df['open_time'], y=["Z-Rate Change Open-Close (Pair)"],
                             colorscale='RdBu', zmin=-3, zmax=3, showscale=False),
                  row=3, col=1)

    # Z-Rate Change High-Low (Pair) heatmap
    fig.add_trace(go.Heatmap(z=[df['z_rate_change_high_low']], x=df['open_time'], y=["Z-Rate Change High-Low (Pair)"],
                             colorscale='RdBu', zmin=-3, zmax=3, showscale=False),
                  row=4, col=1)

    # Z-Volume (Pair) heatmap
    fig.add_trace(go.Heatmap(z=[df['z_volume_pair']], x=df['open_time'], y=["Z-Volume (Pair)"],
                             colorscale='RdBu', zmin=-3, zmax=3, showscale=False), 
                  row=5, col=1)

    # Z-Rate Change Open-Close (Cross) heatmap
    fig.add_trace(go.Heatmap(z=[df['z_rate_change_open_close_all_pairs']], x=df['open_time'], y=["Z-Rate Change Open-Close (Cross)"],
                             colorscale='RdBu', zmin=-2, zmax=2, showscale=False),
                  row=6, col=1)

    # Z-Rate Change High-Low (Cross) heatmap
    fig.add_trace(go.Heatmap(z=[df['z_rate_change_high_low_all_pairs']], x=df['open_time'], y=["Z-Rate Change High-Low (Cross)"],
                             colorscale='RdBu', zmin=-2, zmax=2, showscale=False),
                  row=7, col=1)

    # Z-Volume (Cross) heatmap
    fig.add_trace(go.Heatmap(z=[df['z_volume_all_pairs']], x=df['open_time'], y=["Z-Volume (Cross)"],
                             colorscale='RdBu', zmin=-2, zmax=2, showscale=True, colorbar_x=1.02, colorbar_y=0.5, colorbar_len=0.7),
                  row=8, col=1)

    # Update layout for better visual clarity
    fig.update_layout(height=1400, title=f'{symbol} OHLCV Chart and Pair/Cross-Pair Z-Scores', 
                      xaxis_rangeslider_visible=False,
                      yaxis_title='Price', 
                      yaxis2_title='Volume')

    # Show the chart
    fig.show()

def main(symbol, num_units, grouping):
    # Connect to the SQLite database
    conn = sqlite3.connect('trading_data.db')

    # Fetch data for the specified trading pair and grouping (daily, weekly, or 4h)
    df = fetch_data_from_db(conn, symbol, num_units, grouping)

    # Plot the OHLCV chart with Z-scores for the pair and cross-pair
    plot_ohlcv_with_z_scores(df, symbol)

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Plot OHLCV and Z-scores for a given trading pair and cross-pairs.")
    parser.add_argument("symbol", type=str, help="The symbol of the trading pair (e.g., BTCUSDT)")
    parser.add_argument("num_units", type=int, help="The number of units (e.g., 7 for days or weeks)")
    parser.add_argument("grouping", type=str, choices=['d', 'w', '4h'], help="The grouping ('d' for daily, 'w' for weekly, '4h' for 4-hour)")
    args = parser.parse_args()

    main(args.symbol, args.num_units, args.grouping)
