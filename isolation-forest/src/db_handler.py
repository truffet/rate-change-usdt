import sqlite3
import pandas as pd

class DatabaseHandler:
    def __init__(self, db_file, timeframe):
        """
        Initialize DatabaseHandler with the path to the SQLite database file and the timeframe.
        
        Args:
            db_file (str): Path to the SQLite database file.
            timeframe (str): The timeframe for the data ('4h', 'd', 'w').
        """
        self.db_file = db_file
        self.timeframe = timeframe  # Store the timeframe as an instance variable

    def _connect(self):
        """Private method to create a new database connection."""
        return sqlite3.connect(self.db_file)

    def create_isolation_forest_table(self):
        """
        Create an isolation forest table for the instance's timeframe if it does not already exist.
        """
        table_name = f'isolation_forest_{self.timeframe}'
        
        create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            open_time INTEGER NOT NULL,
            error_score REAL NOT NULL,
            z_error_score REAL NOT NULL,
            is_anomaly BOOLEAN,
            FOREIGN KEY (symbol) REFERENCES usdt_{self.timeframe} (symbol),
            UNIQUE(symbol, open_time)  -- Ensure unique data per trading pair and open time
        );
        '''
        
        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(create_table_query)
                conn.commit()
                print(f"Table '{table_name}' created successfully (or already exists).")
            
        except sqlite3.Error as e:
            print(f"Error creating table '{table_name}': {e}")

    def fetch_zscore_data(self):
        """
        Fetches Z-score data and open_time from the database for the instance's timeframe.

        Returns:
            pd.DataFrame: DataFrame containing open_time and six Z-score columns for the specified timeframe.
        """
        table_name = f'usdt_{self.timeframe}'
        
        query = f'''
            SELECT 
                open_time,
                z_rate_change_open_close, 
                z_rate_change_high_low, 
                z_volume_pair, 
                z_rate_change_open_close_all_pairs, 
                z_rate_change_high_low_all_pairs, 
                z_volume_all_pairs
            FROM {table_name}
        '''
        
        try:
            with self._connect() as conn:
                df = pd.read_sql_query(query, conn)
                print(f"Fetched data for {self.timeframe} timeframe with {len(df)} rows, including open_time.")
        except Exception as e:
            print(f"Error fetching data: {e}")
            df = pd.DataFrame()
        
        return df

