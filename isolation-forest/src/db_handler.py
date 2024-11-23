import sqlite3
import pandas as pd
import logging

class DatabaseHandler:
    def __init__(self, db_path, timeframe="4h"):
        self.conn = sqlite3.connect(db_path)
        self.timeframe = timeframe
        self.main_table = f'usdt_{self.timeframe}'
        self.normal_data_table = f'normal_data_{self.timeframe}'
        logging.info(f"Connected to the database at '{db_path}' for timeframe '{self.timeframe}'.")

    def create_normal_data_table(self):
        """
        Create the normal data table if it does not already exist.
        """
        create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {self.normal_data_table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            open_time INTEGER NOT NULL,
            close_time INTEGER NOT NULL,
            z_rate_change_open_close REAL,
            z_rate_change_high_low REAL,
            z_volume_pair REAL,
            z_rate_change_open_close_all_pairs REAL,
            z_rate_change_high_low_all_pairs REAL,
            z_volume_all_pairs REAL,
            UNIQUE(symbol, open_time)
        );
        '''
        try:
            cursor = self.conn.cursor()
            cursor.execute(create_table_query)
            self.conn.commit()
            logging.info(f"Table '{self.normal_data_table}' created successfully (or already exists).")
        except sqlite3.Error as e:
            logging.error(f"Error creating table '{self.normal_data_table}': {e}")

    def get_last_registered_open_time(self):
        """
        Retrieve the last recorded open_time from the normal data table.
        """
        query = f"SELECT MAX(open_time) FROM {self.normal_data_table}"
        cursor = self.conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]
        logging.info(f"Fetched last registered open_time: {result}")
        return result if result else 0

    def fetch_all_zscore_data(self):
        """
        Fetch all Z-score data from the main table.
        """
        query = f'''
            SELECT 
                symbol,
                open_time,
                close_time,
                z_rate_change_open_close, 
                z_rate_change_high_low, 
                z_volume_pair, 
                z_rate_change_open_close_all_pairs, 
                z_rate_change_high_low_all_pairs, 
                z_volume_all_pairs
            FROM {self.main_table}
        '''
        try:
            df = pd.read_sql_query(query, self.conn)
            logging.info(f"Fetched {len(df)} rows of Z-score data from {self.main_table}.")
            return df
        except sqlite3.Error as e:
            logging.error(f"Error fetching Z-score data from {self.main_table}: {e}")
            return pd.DataFrame()

    def insert_normal_data(self, df_normal):
        """
        Insert normal data into the normal data table.
        """
        try:
            df_normal.to_sql(self.normal_data_table, self.conn, if_exists='append', index=False)
            logging.info(f"Inserted {len(df_normal)} new rows into '{self.normal_data_table}'.")
        except sqlite3.Error as e:
            logging.error(f"Error inserting data into '{self.normal_data_table}': {e}")

    def close(self):
        """Close the database connection."""
        self.conn.close()
        logging.info("Closed the database connection.")
