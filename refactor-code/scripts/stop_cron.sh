#!/bin/bash

# Description: This script removes any existing cron jobs for a specific Python script.

# Get the directory of the current script (stop_cron.sh)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Path to the Python script (default is main.py, but can be passed as an argument)
SCRIPT_NAME="${1:-main.py}"
PYTHON_SCRIPT_PATH="$SCRIPT_DIR/../$SCRIPT_NAME"

# Check if any cron jobs exist for the script
EXISTING_CRON_JOB=$(crontab -l | grep "$PYTHON_SCRIPT_PATH")

if [ -n "$EXISTING_CRON_JOB" ]; then
    # Remove the existing cron jobs for the script
    (crontab -l | grep -v "$PYTHON_SCRIPT_PATH") | crontab -

    echo "Cron job for $SCRIPT_NAME has been stopped successfully."
else
    echo "No existing cron job found for $SCRIPT_NAME."
fi
