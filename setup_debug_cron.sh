#!/bin/bash

# Path to the Python script
SCRIPT_PATH="$(pwd)/main.py"

# Path to the Python executable
PYTHON_PATH="$(which python3)"

# Path to the log file
LOG_FILE="$(pwd)/cronjob_debug.log"

# Debug cron job schedule (every 5 minutes)
DEBUG_CRON_SCHEDULE="*/5 * * * *"

# Full cron job command for debugging
DEBUG_CRON_COMMAND="$DEBUG_CRON_SCHEDULE $PYTHON_PATH $SCRIPT_PATH >> $LOG_FILE 2>&1"

# Check if the debug cron job already exists; if not, add it
(crontab -l | grep -F "$SCRIPT_PATH") || (crontab -l; echo "$DEBUG_CRON_COMMAND") | crontab -

echo "Debug cron job set up successfully to run every 5 minutes, logging output to $LOG_FILE."

# Run the script immediately for initial debugging
echo "Running the script immediately for debugging..."
$PYTHON_PATH $SCRIPT_PATH >> $LOG_FILE 2>&1

echo "Script executed for debugging. Check $LOG_FILE for output."
