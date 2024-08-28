#!/bin/bash

# Path to the Python script
SCRIPT_PATH="$(pwd)/main.py"

# Path to the Python executable
PYTHON_PATH="$(which python3)"

# Path to the log file
LOG_FILE="$(pwd)/cronjob.log"

# Cron job schedule for specific times
CRON_SCHEDULE="5 4,8,12,16,20,0 * * *"

# Full cron job command for scheduled times
CRON_COMMAND="$CRON_SCHEDULE $PYTHON_PATH $SCRIPT_PATH >> $LOG_FILE 2>&1"

# Check if the cron job already exists; if not, add it
(crontab -l | grep -F "$SCRIPT_PATH") || (crontab -l; echo "$CRON_COMMAND") | crontab -

echo "Cron job set up successfully to run at 4:05 am, 8:05 am, 12:05 pm, 4:05 pm, 8:05 pm, and 12:05 am, logging output to $LOG_FILE."

# Run the script immediately after setting up the cron jobs
echo "Running the script immediately..."
$PYTHON_PATH $SCRIPT_PATH >> $LOG_FILE 2>&1

echo "Script executed. Check $LOG_FILE for output."
