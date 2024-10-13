import logging
from telegram import Bot
import asyncio

class TelegramBot:
    def __init__(self, bot_token, chat_id):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.bot = Bot(token=self.bot_token)

    async def send_candlestick_summary(self, df, open_time, close_time):
        """Send a summary of the filtered candlestick data to Telegram."""
        try:
            # Sort by absolute value of rate_change in descending order
            df = df.sort_values(by='rate_change', ascending=False)

            if df.empty:
                logging.info("No pairs meet the Z-score threshold, no message sent.")
                return

            # Format the open and close time as: 'YYYY-MM-DD: HH:MM:SS -> HH:MM:SS'
            date_str = open_time.strftime('%Y-%m-%d')
            open_time_str = open_time.strftime('%H:%M:%S')
            close_time_str = close_time.strftime('%H:%M:%S')
            full_message = f"Binance Spot USDT Market Recap\n{date_str}: {open_time_str} -> {close_time_str}\n\n"

            # Add each symbol and rate change to the message
            for _, row in df.iterrows():
                rate_change_icon = "🟩" if row['rate_change'] >= 0 else "🟥"
                full_message += f"{rate_change_icon} {row['symbol']} {row['rate_change']:.2f}%\n"

            # Send the entire message as a single Telegram message asynchronously
            await self.bot.send_message(chat_id=self.chat_id, text=f"```\n{full_message}\n```", parse_mode='Markdown')

        except Exception as e:
            logging.error(f"Failed to send message: {e}")
