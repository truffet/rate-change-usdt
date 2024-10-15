import sqlite3
import pandas as pd
from datetime import timedelta, datetime, timezone
import logging

class DatabaseHandler:
    def get_timestamp_cursor(self, conn, timeframe):
        """
        Fetch the most recent close_time from the database and return the next timestamp
        (incremented by 1 ms) as the starting point for backfilling data.
        If no data is found, return a timestamp 1 year back from the current time in UTC.
        """
        # Concatenate the timeframe to create the table name
        table = f'usdt_{timeframe}'

        # Determine the increment based on the timeframe
        if timeframe == '4h':
            increment = timedelta(hours=4)
        elif timeframe == 'd':
            increment = timedelta(days=1)
        elif timeframe == 'w':
            increment = timedelta(weeks=1)
        else:
            raise ValueError("Invalid timeframe specified.")

        # Fetch the most recent close_time from the specified table
        query = f"SELECT MAX(close_time) FROM {table}"
        cursor = conn.cursor()
        cursor.execute(query)
        last_close_time = cursor.fetchone()[0]

        # If no data, return max_backfill timestamp rounded to the timeframe in UTC
        if last_close_time is None:
            max_backfill = datetime.now(timezone.utc) - timedelta(days=365)
            logging.warning(f"No data found in {table}. Returning max backfill timestamp.")
            rounded_backfill = self._round_timestamp_to_timeframe(max_backfill, timeframe)
            return int(rounded_backfill.timestamp() * 1000)

        # Convert the last close_time to a datetime object and return the next timestamp (increment by 1 ms)
        last_close_time_datetime = pd.to_datetime(last_close_time, unit='ms')
        next_timestamp = last_close_time_datetime + timedelta(milliseconds=1)

        return int(next_timestamp.timestamp() * 1000)

    def _round_timestamp_to_timeframe(self, timestamp, timeframe):
        """
        Round the given timestamp (datetime) to the nearest lower interval (4h, d, or w).
        """
        timestamp = timestamp.astimezone(timezone.utc)  # Ensure UTC
        if timeframe == '4h':
            rounded_time = timestamp.replace(minute=0, second=0, microsecond=0)
            rounded_time -= timedelta(hours=timestamp.hour % 4)
        elif timeframe == 'd':
            rounded_time = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        elif timeframe == 'w':
            rounded_time = timestamp - timedelta(days=timestamp.weekday())
            rounded_time = rounded_time.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            raise ValueError("Invalid timeframe specified for rounding.")
        return rounded_time
