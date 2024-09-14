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

# Cron job schedule (for 4:05 AM, 8:05 AM, 12:05 PM, 4:05 PM, 8:05 PM, and 12:05 AM in system timezone)
CRON_SCHEDULE="${1:-"5 4,8,12,16,20,0 * * *"}"

# Full cron job command to execute main.py at the specified times based on system time
CRON_COMMAND="$CRON_SCHEDULE $PYTHON_PATH $PYTHON_SCRIPT_PATH >> $LOG_FILE 2>&1"

# Remove any existing cron jobs for the script to avoid duplicates
(crontab -l | grep -v "$PYTHON_SCRIPT_PATH") | crontab -

# Add the new cron job with system timezone
(crontab -l; echo "$CRON_COMMAND") | crontab -

# Success message
echo "Cron job set up successfully."
echo "main.py will now run according to the schedule: $CRON_SCHEDULE based on system timezone."
echo "Logging output will be saved to $LOG_FILE."
