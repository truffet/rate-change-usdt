#!/bin/bash

# Step 1: Install Python3 (if not installed)
echo "Checking for Python3 installation..."
if ! command -v python3 &> /dev/null
then
    echo "Python3 not found. Installing Python3..."
    sudo apt-get update  # For Ubuntu/Linux (you can adapt for macOS or Windows as needed)
    sudo apt-get install python3 -y
else
    echo "Python3 is already installed."
fi

# Step 2: Install pip3 (if not installed)
echo "Checking for pip3 installation..."
if ! command -v pip3 &> /dev/null
then
    echo "pip3 not found. Installing pip3..."
    python3 -m ensurepip --upgrade
else
    echo "pip3 is already installed."
fi

# Step 3: Upgrade pip3 to the latest version
echo "Upgrading pip3 to the latest version..."
pip3 install --upgrade pip

# Step 4: Install necessary Python packages directly in the script
echo "Installing Python packages..."

# Install pandas
pip3 install pandas

# Install requests
pip3 install requests

# Install scipy
pip3 install scipy

# Install python-telegram-bot
pip3 install python-telegram-bot

# Install APScheduler (for cron-like scheduling)
pip3 install apscheduler

# Install Matplotlib
pip3 install matplotlib


# Final message
echo "Python and necessary Python packages installed successfully."
