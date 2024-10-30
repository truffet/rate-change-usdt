import pandas as pd
import sqlite3

def fetch_zscore_data(conn, timeframe):
    """
    Fetches Z-score data from the database for the specified timeframe.

    Args:
        conn (sqlite3.Connection): Database connection object.
        timeframe (str): The timeframe for which data is to be fetched ('4h', 'd', 'w').

    Returns:
        pd.DataFrame: DataFrame containing six Z-score columns for the specified timeframe.
    """
    # Define the table name based on the timeframe
    table_name = f'usdt_{timeframe}'
    
    # Query to select the six Z-score columns
    query = f'''
        SELECT 
            z_rate_change_open_close, 
            z_rate_change_high_low, 
            z_volume_pair, 
            z_rate_change_open_close_all_pairs, 
            z_rate_change_high_low_all_pairs, 
            z_volume_all_pairs
        FROM {table_name}
    '''
    
    # Fetch the data into a DataFrame
    try:
        df = pd.read_sql_query(query, conn)
        print(f"Fetched data for {timeframe} timeframe with {len(df)} rows.")
    except Exception as e:
        print(f"Error fetching data: {e}")
        df = pd.DataFrame()  # Return an empty DataFrame in case of an error
    
    return df