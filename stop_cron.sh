#!/bin/bash

# Path to the Python script
PYTHON_SCRIPT_PATH="$(pwd)/main2.py"

# Remove any existing cron jobs for the script
(crontab -l | grep -v "$PYTHON_SCRIPT_PATH") | crontab -

echo "Cron job for main2.py has been stopped successfully."
