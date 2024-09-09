# src/telegram_client.py

import requests
import logging

class TelegramBot:
    def __init__(self, bot_token, chat_id):
        """
        Initializes the Telegram bot with the token and chat ID.
        """
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

    def send_message(self, message):
        """
        Sends a message to the specified Telegram chat.

        Args:
            message (str): The message to send to Telegram.

        Raises:
            Exception: If the message fails to send, an exception is logged.
        """
        url = f"{self.base_url}/sendMessage"
        payload = {
            'chat_id': self.chat_id,
            'text': message
        }
        try:
            response = requests.post(url, data=payload)
            response.raise_for_status()  # Raise an exception for 4XX/5XX errors
            logging.info("Message sent successfully to Telegram.")
        except requests.RequestException as e:
            logging.error(f"Failed to send message to Telegram: {e}")
            raise Exception("Failed to send message to Telegram.") from e
