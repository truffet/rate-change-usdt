import sqlite3
import pandas as pd

# Database file path
DB_FILE = 'trading_data.db'

# Define the tables and the threshold
tables = ['usdt_4h', 'usdt_d', 'usdt_w']
z_score_threshold = 2

def fetch_unique_symbols_with_high_z_volume(conn, table_name, threshold):
    """
    Fetch unique symbols from the specified table where the absolute value of z_volume_all_pairs exceeds the threshold.
    
    Args:
        conn: SQLite connection object.
        table_name (str): The name of the table to query.
        threshold (float): The z-score threshold.

    Returns:
        list: List of unique symbols that match the criteria.
    """
    query = f"""
    SELECT DISTINCT symbol
    FROM {table_name}
    WHERE ABS(z_volume_all_pairs) > ?
    """
    df = pd.read_sql_query(query, conn, params=(threshold,))
    return df['symbol'].tolist()

def main():
    # Connect to the database
    conn = sqlite3.connect(DB_FILE)

    # Loop through each table and fetch unique symbols with high z_volume_all_pairs
    results = {}
    for table in tables:
        symbols = fetch_unique_symbols_with_high_z_volume(conn, table, z_score_threshold)
        if symbols:
            results[table] = symbols
            print(f"Unique symbols for table {table}:\n", symbols)

    # Close the connection
    conn.close()

    # Optionally return the results if needed for further processing
    return results

if __name__ == "__main__":
    main()
