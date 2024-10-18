import sqlite3
import pandas as pd
import plotly.figure_factory as ff
import plotly.subplots as sp

# Function to fetch Z-scores from the database
def fetch_z_scores(conn, table, z_column):
    query = f'''
        SELECT {z_column} 
        FROM {table}
        WHERE {z_column} IS NOT NULL
    '''
    df = pd.read_sql_query(query, conn)
    return df[z_column]

# Function to create a distribution plot using Plotly
def create_distplot(z_scores, z_column, timeframe):
    # Create histogram and kernel density estimate (KDE) using Plotly
    fig = ff.create_distplot([z_scores], [f'{timeframe} {z_column}'], bin_size=0.1, show_rug=False, curve_type='normal')
    fig.update_layout(title=f"{timeframe} {z_column} Distribution", xaxis_title='Z-Score', yaxis_title='Density')
    return fig

def plot_all_distributions(conn):
    timeframes = ['4h', '1d', '1w']
    tables = {'4h': 'usdt_4h', '1d': 'usdt_d', '1w': 'usdt_w'}
    z_columns = ['z_rate_change_all_pairs', 'z_volume_all_pairs']
    z_column_names = {'z_rate_change_all_pairs': 'Rate Change Z-Score (Cross-Pair)',
                      'z_volume_all_pairs': 'Volume Z-Score (Cross-Pair)'}

    # Create subplots (6 rows: 3 timeframes, 2 Z-score types per timeframe)
    fig = sp.make_subplots(rows=6, cols=1, subplot_titles=[
        "4h Volume Z-Score", "4h Rate Change Z-Score",
        "1d Volume Z-Score", "1d Rate Change Z-Score",
        "1w Volume Z-Score", "1w Rate Change Z-Score"],
        shared_xaxes=False, shared_yaxes=False, vertical_spacing=0.1)

    for i, timeframe in enumerate(timeframes):
        table = tables[timeframe]
        
        # Fetch and plot Z-scores for rate change and volume
        for j, z_column in enumerate(z_columns):
            z_scores = fetch_z_scores(conn, table, z_column)
            
            # Create distribution plot for each
            distplot = create_distplot(z_scores, z_column_names[z_column], timeframe)

            # Add traces (histogram and kde) to the subplots
            for trace in distplot['data']:
                fig.add_trace(trace, row=i * 2 + j + 1, col=1)

    # Update layout for better visibility
    fig.update_layout(height=1800, width=1200, title_text="Z-Score Distributions for Volume and Rate Change Across Timeframes",
                      margin=dict(l=20, r=20, t=40, b=20), showlegend=True)
    
    # Show the figure
    fig.show()

def main():
    # Connect to the SQLite database
    conn = sqlite3.connect('trading_data.db')

    # Plot distributions for 4h, 1d, and 1w for rate change and volume Z-scores
    plot_all_distributions(conn)

    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()
