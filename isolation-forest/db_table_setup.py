import sqlite3

def create_isolation_forest_table(conn, timeframe):
    """
    Create an isolation forest table for the specified timeframe if it does not already exist.
    
    Args:
        conn (sqlite3.Connection): SQLite database connection.
        timeframe (str): The timeframe ('4h', 'd', 'w') for which to create the table.
    """
    table_name = f'isolation_forest_{timeframe}'
    
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        open_time INTEGER NOT NULL,
        error_score REAL NOT NULL,
        z_error_score REAL NOT NULL,
        is_anomaly BOOLEAN,
        FOREIGN KEY (symbol) REFERENCES usdt_{timeframe} (symbol),
        UNIQUE(symbol, open_time)  -- Ensure unique data per trading pair and open time
    );
    '''
    
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print(f"Table '{table_name}' created successfully (or already exists).")
        
    except sqlite3.Error as e:
        print(f"Error creating table '{table_name}': {e}")
