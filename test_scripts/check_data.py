import sqlite3
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def fetch_data_from_db(conn, symbol, num_units, grouping):
    """Fetch data from the daily or weekly table for a given symbol and units."""
    table = 'usdt_d' if grouping == 'd' else 'usdt_w'

    # Query the most recent num_units candles from the daily or weekly table
    query = f'''SELECT open_time, open_price, high_price, low_price, close_price, volume, 
                        z_rate_change_pair, z_volume_pair, z_combined_pair, 
                        z_rate_change_all_pairs, z_volume_all_pairs, z_combined_all_pairs
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

    # Create a subplot with 8 rows: OHLC, Volume, and 3 Z-scores for pair, 3 Z-scores for cross-pairs
    fig = make_subplots(rows=8, cols=1, shared_xaxes=True,
                        row_heights=[0.4, 0.2, 0.13, 0.13, 0.13, 0.13, 0.13, 0.13], vertical_spacing=0.03,
                        specs=[[{}], [{}], [{}], [{}], [{}], [{}], [{}], [{}]],
                        subplot_titles=[f"{symbol} OHLC Candlestick",
                                        f"{symbol} Volume",
                                        "Z-Rate Change (Pair)", 
                                        "Z-Volume (Pair)", 
                                        "Z-Combined (Pair)",
                                        "Z-Rate Change (Cross)", 
                                        "Z-Volume (Cross)", 
                                        "Z-Combined (Cross)"])

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

    # Z-Rate Change (Pair) heatmap
    fig.add_trace(go.Heatmap(z=[df['z_rate_change_pair']], x=df['open_time'], y=["Z-Rate Change (Pair)"],
                             colorscale='RdBu', zmin=-3, zmax=3, showscale=False),
                  row=3, col=1)

    # Z-Volume (Pair) heatmap
    fig.add_trace(go.Heatmap(z=[df['z_volume_pair']], x=df['open_time'], y=["Z-Volume (Pair)"],
                             colorscale='RdBu', zmin=-3, zmax=3, showscale=False), 
                  row=4, col=1)

    # Z-Combined (Pair) heatmap
    fig.add_trace(go.Heatmap(z=[df['z_combined_pair']], x=df['open_time'], y=["Z-Combined (Pair)"],
                             colorscale='RdBu', zmin=-3, zmax=3, showscale=False), 
                  row=5, col=1)

    # Z-Rate Change (Cross) heatmap (different Z-scale but same colors)
    fig.add_trace(go.Heatmap(z=[df['z_rate_change_all_pairs']], x=df['open_time'], y=["Z-Rate Change (Cross)"],
                             colorscale='RdBu', zmin=-2, zmax=2, showscale=False),
                  row=6, col=1)

    # Z-Volume (Cross) heatmap (different Z-scale but same colors)
    fig.add_trace(go.Heatmap(z=[df['z_volume_all_pairs']], x=df['open_time'], y=["Z-Volume (Cross)"],
                             colorscale='RdBu', zmin=-2, zmax=2, showscale=False), 
                  row=7, col=1)

    # Z-Combined (Cross) heatmap (different Z-scale but same colors)
    fig.add_trace(go.Heatmap(z=[df['z_combined_all_pairs']], x=df['open_time'], y=["Z-Combined (Cross)"],
                             colorscale='RdBu', zmin=-2, zmax=2, showscale=True, colorbar_x=1.02, colorbar_y=0.5, colorbar_len=0.7),
                  row=8, col=1)

    # Update layout for better visual clarity
    fig.update_layout(height=1500, title=f'{symbol} OHLCV Chart and Pair/Cross-Pair Z-Scores', 
                      xaxis_rangeslider_visible=False,
                      yaxis_title='Price', 
                      yaxis2_title='Volume')

    # Show the chart
    fig.show()

def main(symbol, num_units, grouping):
    # Connect to the SQLite database
    conn = sqlite3.connect('trading_data.db')

    # Fetch data for the specified trading pair and grouping (daily or weekly)
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
    parser.add_argument("grouping", type=str, choices=['d', 'w'], help="The grouping ('d' for daily, 'w' for weekly)")
    args = parser.parse_args()

    main(args.symbol, args.num_units, args.grouping)
