import sqlite3

def drop_normal_data_table(db_path, timeframe="4h"):
    """
    Drops the normal data table for the specified timeframe.

    Args:
        db_path (str): Path to the SQLite database.
        timeframe (str): The timeframe for which to drop the table (default is '4h').
    """
    table_name = f'normal_data_{timeframe}'
    conn = sqlite3.connect(db_path)
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        print(f"Table '{table_name}' dropped successfully.")
    except sqlite3.Error as e:
        print(f"Error dropping table '{table_name}': {e}")
    finally:
        conn.close()

# Run the function with the path to your database
drop_normal_data_table("../trading_data.db", timeframe="4h")
