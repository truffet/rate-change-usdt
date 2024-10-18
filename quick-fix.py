import sqlite3

def delete_latest_timestamp_for_all_symbols(conn, timeframe):
    """
    Delete the latest record (based on open_time) for all symbols in a specific timeframe.

    Args:
        conn (sqlite3.Connection): The SQLite connection object.
        timeframe (str): The timeframe ('4h', 'd', 'w') to determine the appropriate table.
    """
    # Determine the appropriate table based on the timeframe
    table_name = f'usdt_{timeframe}'

    cursor = conn.cursor()

    # Fetch all unique symbols in the table
    cursor.execute(f"SELECT DISTINCT symbol FROM {table_name}")
    symbols = cursor.fetchall()

    if symbols:
        # Loop through each symbol and delete the latest record
        for symbol in symbols:
            symbol = symbol[0]  # Extract the symbol value from the tuple

            # Find the latest open_time (most recent timestamp) for the given symbol
            cursor.execute(f"SELECT MAX(open_time) FROM {table_name} WHERE symbol = ?", (symbol,))
            latest_timestamp = cursor.fetchone()[0]

            if latest_timestamp:
                # Delete the record with the latest open_time for the given symbol
                cursor.execute(f"DELETE FROM {table_name} WHERE symbol = ? AND open_time = ?", (symbol, latest_timestamp))
                conn.commit()
                print(f"Deleted the latest record with open_time {latest_timestamp} for symbol {symbol} in {table_name}.")
            else:
                print(f"No records found for symbol {symbol} in {table_name}.")
    else:
        print(f"No symbols found in {table_name}.")

# Example usage
if __name__ == "__main__":
    # Connect to the SQLite database
    conn = sqlite3.connect('trading_data.db')

    # Define the timeframe
    timeframe = '4h'  # Replace with the desired timeframe ('4h', 'd', or 'w')

    # Delete the latest timestamp for all symbols in the given timeframe
    delete_latest_timestamp_for_all_symbols(conn, timeframe)

    # Close the database connection
    conn.close()
