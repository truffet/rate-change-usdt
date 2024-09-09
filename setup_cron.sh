#!/bin/bash

# Path to the Python script
PYTHON_SCRIPT_PATH="$(pwd)/main.py"

# Path to the log file
LOG_FILE="$(pwd)/debug_cron.log"

# Full path to Python, update with your installed Python version path
PYTHON_PATH="/Library/Frameworks/Python.framework/Versions/3.12/bin/python3"

# Add any environment variables or PATH settings needed
export PATH="/Library/Frameworks/Python.framework/Versions/3.12/bin:/usr/bin:/bin:/usr/sbin:/sbin"

# Cron job schedule for 4:05 AM, 8:05 AM, 12:05 PM, 4:05 PM, 8:05 PM, and 12:05 AM
CRON_SCHEDULE="5 4,8,12,16,20,0 * * *"

# Full cron job command to execute main.py at the specified times
CRON_COMMAND="$CRON_SCHEDULE $PYTHON_PATH $PYTHON_SCRIPT_PATH >> $LOG_FILE 2>&1"

# Remove any existing cron jobs for the script to avoid duplicates
(crontab -l | grep -v "$PYTHON_SCRIPT_PATH") | crontab -

# Add the new cron job
(crontab -l; echo "$CRON_COMMAND") | crontab -

echo "Cron job set up successfully."
echo "main.py will now run at 4:05 AM, 8:05 AM, 12:05 PM, 4:05 PM, 8:05 PM, and 12:05 AM, logging output to $LOG_FILE."
