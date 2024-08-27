# Rate Change USDT Script

This repository contains a Python script that fetches cryptocurrency data from the Binance API, processes it to calculate rate changes and z-scores, and sends the results to a Telegram bot.

## Prerequisites

1. **Python 3.x**: Ensure Python 3 is installed on your system. You can download it from Python's official website.

2. **Required Python Packages**: Install the necessary Python packages by running:

pip install -r requirements.txt

3. **Set Up Telegram Bot**: Create a Telegram bot using BotFather and get the API token. Add your chat ID and bot token to the `config.json` file.

## Setup

### 1. Clone the Repository

Clone the repository to your local machine:

git clone https://github.com/yourusername/rate-change-usdt.git
cd rate-change-usdt

### 2. Configure the Script

Edit the `config.json` file to include your Telegram bot token and chat ID:

{
    "interval": "4h",
    "telegram": {
        "bot_token": "YOUR_BOT_TOKEN_HERE",
        "chat_id": "YOUR_CHAT_ID_HERE"
    }
}

Replace `YOUR_BOT_TOKEN_HERE` and `YOUR_CHAT_ID_HERE` with your actual Telegram bot token and chat ID.

### 3. Set Up Cron Job

To automatically run the script every 4 hours and log the output, you can use the `setup_cron.sh` script provided in the repository.

#### a. Make `setup_cron.sh` Executable

Run the following command to make the script executable:

chmod +x setup_cron.sh

#### b. Run the Setup Script

Execute the `setup_cron.sh` script to set up the cron job:

./setup_cron.sh

This will create a cron job that runs `main.py` every 4 hours and logs the output to `cronjob.log`.

### 4. Check Logs

To view the output logs of your cron job, use the `check_logs.sh` script:

#### a. Make `check_logs.sh` Executable

Run the following command to make the script executable:

chmod +x check_logs.sh

#### b. Run the Log Checking Script

Execute the `check_logs.sh` script to display the last 10 lines of the log file:

./check_logs.sh

## Usage

Once everything is set up, the script will automatically run every 4 hours and send the processed data to your Telegram bot. You can check the logs using the `check_logs.sh` script to verify the script's output.


## License

This project is licensed under the MIT License.
