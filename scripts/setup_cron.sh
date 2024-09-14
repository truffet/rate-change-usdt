#!/bin/bash

# Get the directory of the current script (setup_cron.sh)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Path to the Python script (absolute path)
PYTHON_SCRIPT_PATH="$SCRIPT_DIR/../main.py"

# Path to the logs directory
LOG_DIR="$SCRIPT_DIR/../logs"

# Create the logs directory if it doesn't exist
if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
fi

# Path to the log file (absolute path)
LOG_FILE="$LOG_DIR/debug_cron.log"

# Detect the Python path dynamically
PYTHON_PATH="$(which python3)"

if [ -z "$PYTHON_PATH" ]; then
    echo "Error: Python is not installed or not found in PATH."
    exit 1
fi

# Step 1: Detect the system timezone offset from UTC
# `date +%z` gives the offset from UTC in the format ±HHMM (e.g., +0200 for CEST)
timezone_offset=$(date +%z)

# Extract hours and minutes from the timezone offset
timezone_hours=${timezone_offset:0:3}
timezone_minutes=${timezone_offset:3:2}

# Calculate the adjusted cron schedule based on the timezone offset
# Convert to the nearest hour in system time to match the UTC schedule of 4:05, 8:05, etc.
# The goal is to shift the cron schedule to account for the system's offset from UTC
adjusted_hours="4,8,12,16,20,0"
cron_hours=""

for hour in $(echo $adjusted_hours | sed "s/,/ /g"); do
    adjusted_hour=$((hour + timezone_hours))  # Apply the hour offset
    if [ $adjusted_hour -lt 0 ]; then
        adjusted_hour=$((adjusted_hour + 24))  # Handle negative hour wraparound
    elif [ $adjusted_hour -ge 24 ]; then
        adjusted_hour=$((adjusted_hour - 24))  # Handle hours > 24 wraparound
    fi
    cron_hours="$cron_hours,$adjusted_hour"
done

# Remove the leading comma
cron_hours=$(echo $cron_hours | sed "s/^,//")

# Step 2: Set the cron job schedule (with adjusted hours)
CRON_SCHEDULE="5 $cron_hours * * *"

# Full cron job command to execute main.py at the specified times based on the adjusted system time
CRON_COMMAND="$CRON_SCHEDULE $PYTHON_PATH $PYTHON_SCRIPT_PATH >> $LOG_FILE 2>&1"

# Remove any existing cron jobs for the script to avoid duplicates
(crontab -l | grep -v "$PYTHON_SCRIPT_PATH") | crontab -

# Add the new cron job with the adjusted schedule
(crontab -l; echo "$CRON_COMMAND") | crontab -

# Success message
echo "Cron job set up successfully."
echo "main.py will now run according to the schedule: $CRON_SCHEDULE based on the system's timezone offset ($timezone_offset from UTC)."
echo "Logging output will be saved to $LOG_FILE."
