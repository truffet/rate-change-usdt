#!/bin/bash

# Path to the log file
LOG_FILE="$(pwd)/cronjob.log"

# Check if the log file exists
if [ -f "$LOG_FILE" ]; then
    echo "Displaying the last 10 lines of the log file:"
    tail -n 10 "$LOG_FILE"
else
    echo "Log file not found. It seems the cron job has not generated any logs yet."
fi
