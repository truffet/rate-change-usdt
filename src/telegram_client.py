# src/telegram_client.py

import requests
import logging

class TelegramBot:
    def __init__(self, bot_token, chat_id):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def send_message(self, text):
        """Send a text message to the configured Telegram chat."""
        payload = {
            'chat_id': self.chat_id,
            'text': text,
            'parse_mode': 'Markdown'
        }
        response = requests.post(self.api_url, data=payload)
        if response.status_code != 200:
            raise Exception(f"Failed to send message: {response.text}")

    def send_candlestick_summary(self, df, open_time, close_time):
        """Prepare and send the candlestick data summary to Telegram."""
        full_message = f"📅 **Candlestick Data**\nOpen Time: {open_time} | Close Time: {close_time}\n\n"

        for _, row in df.iterrows():
            rate_change_icon = "🔺" if row['pct_change'] > 0 else "🔻"
            volume_icon = "🟩" if row['pct_change'] > 0 else "🟥"
            full_message += (
                f"💲 {row['pair']} {rate_change_icon}{row['pct_change']:.2f}% {volume_icon}{row['volume_usdt']:.0f} USDT "
                f"| R-Z: {row['z_pct_change']:.2f} | V-Z: {row['z_volume_usdt']:.2f} | C-Z: {row['combined_z_score']:.2f}\n"
            )

        # Send the entire message
        try:
            self.send_message(full_message)
        except Exception as e:
            logging.error(f"Failed to send message: {e}")

