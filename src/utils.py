from datetime import datetime, timedelta, timezone

def get_latest_window(interval_str):
    """
    Get the start and end times for the latest time window based on the interval string (e.g., "4h").
    
    Args:
        interval_str (str): The interval string (e.g., "4h" for a 4-hour window).
    
    Returns:
        start_time (int): The start time in milliseconds (UTC) of the latest time window.
        end_time (int): The end time in milliseconds (UTC) of the latest time window.
    """
    # Extract the number of hours from the interval string (e.g., "4h" -> 4)
    interval_hours = int(interval_str[:-1])  # Remove the 'h' and convert to int

    # Get the current time in UTC
    now = datetime.now(timezone.utc)

    # Round down to the nearest interval boundary
    rounded_time = now - timedelta(hours=now.hour % interval_hours, minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
    start_time = rounded_time - timedelta(hours=interval_hours)

    start_timestamp = int(start_time.timestamp() * 1000)
    end_timestamp = int(rounded_time.timestamp() * 1000)

    return start_timestamp, end_timestamp
