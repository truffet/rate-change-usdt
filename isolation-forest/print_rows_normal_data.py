import sqlite3
import os

# Define the database path
DB_FILE = os.path.join("..", "trading_data.db")

def print_all_rows_in_normal_table():
    """
    Connect to the database and print every row in the 'normal_data_4h' table.
    """
    try:
        # Connect to the database
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # Specify the table name
        table_name = 'normal_data_4h'

        # Query all rows from the table
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)

        # Fetch all rows
        rows = cursor.fetchall()

        # Check if table is empty
        if not rows:
            print(f"No data found in table '{table_name}'.")
        else:
            # Print each row
            for row in rows:
                print(row)

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    print_all_rows_in_normal_table()
