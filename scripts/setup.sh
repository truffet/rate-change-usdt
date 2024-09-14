#!/bin/bash

# Step 1: Upgrade pip to the latest version
echo "Upgrading pip..."
pip install --upgrade pip

# Step 2: Install necessary Python packages directly in the script
echo "Installing Python packages..."

# Install pandas
pip install pandas

# Install requests
pip install requests

# Install scipy
pip install scipy

# Install python-telegram-bot
pip install python-telegram-bot

# Final message
echo "Python packages installed successfully."
