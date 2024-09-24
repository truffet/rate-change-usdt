import sqlite3
import os

def check_database_exists(db_file="trading_data.db"):
    """Check if the database file exists."""
    if not os.path.exists(db_file):
        print(f"Database file '{db_file}' not found.")
        return False
    else:
        print(f"Database file '{db_file}' exists.")
        return True

def check_table_exists(db_file="trading_data.db", table_name="usdt_4h"):
    """Check if the table exists in the database."""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    try:
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        table_exists = cursor.fetchone()

        if table_exists:
            print(f"Table '{table_name}' exists.")
            return True
        else:
            print(f"Table '{table_name}' does not exist.")
            return False
    except sqlite3.Error as e:
        print(f"Error accessing the database: {e}")
        return False
    finally:
        conn.close()

def fetch_first_and_last_row(db_file="trading_data.db", table_name="usdt_4h"):
    """Fetch the first and last row of the table."""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    try:
        # Fetch the first row
        cursor.execute(f"SELECT * FROM {table_name} ORDER BY id ASC LIMIT 1")
        first_row = cursor.fetchone()

        # Fetch the last row
        cursor.execute(f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT 1")
        last_row = cursor.fetchone()

        if first_row:
            print("First Row:")
            print(first_row)
        else:
            print("No data found in the table.")

        if last_row:
            print("\nLast Row:")
            print(last_row)
        else:
            print("No data found in the table.")
    except sqlite3.Error as e:
        print(f"Error accessing the database: {e}")
    finally:
        conn.close()

def check_table_schema(db_file="trading_data.db", table_name="usdt_4h"):
    """Check the table schema to ensure columns are present."""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    try:
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        if columns:
            print(f"\nSchema for table '{table_name}':")
            for column in columns:
                print(f"Column Name: {column[1]}, Type: {column[2]}")
        else:
            print(f"No schema information found for table '{table_name}'.")
    except sqlite3.Error as e:
        print(f"Error accessing the database schema: {e}")
    finally:
        conn.close()

def main():
    db_file = "trading_data.db"
    table_name = "usdt_4h"

    # Check if the database exists
    if not check_database_exists(db_file):
        return

    # Check if the table exists
    if not check_table_exists(db_file, table_name):
        return

    # Check the table schema
    check_table_schema(db_file, table_name)

    # Fetch the first and last rows
    fetch_first_and_last_row(db_file, table_name)

if __name__ == "__main__":
    main()
