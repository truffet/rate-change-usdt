import os

def delete_database(db_file="trading_data.db"):
    """Delete the SQLite database file."""
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
            print(f"Database '{db_file}' has been deleted successfully.")
        except Exception as e:
            print(f"Error occurred while deleting the database: {e}")
    else:
        print(f"Database '{db_file}' does not exist.")

if __name__ == "__main__":
    delete_database()
