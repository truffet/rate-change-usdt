#!/bin/bash

# Path to the Python script
SCRIPT_PATH="$(pwd)/main.py"

# Remove the cron job that matches the script path
crontab -l | grep -v "$SCRIPT_PATH" | crontab -

echo "Cron job for $SCRIPT_PATH has been removed."
