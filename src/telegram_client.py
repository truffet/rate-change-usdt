import logging
from telegram import Bot
import asyncio

class TelegramBot:
    def __init__(self, bot_token, chat_id):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.bot = Bot(token=self.bot_token)

    async def send_candlestick_summary(self, df, timeframe):
        """
        Send a summary of the filtered candlestick data to Telegram for the specified timeframe.

        Args:
            df (pd.DataFrame): DataFrame containing the candlestick data with 'open_time' and 'close_time' columns.
            timeframe (str): The timeframe of the data (e.g., '4h', 'd', 'w').
        """
        try:
            if df.empty:
                logging.info("No pairs meet the Z-score threshold, no message sent.")
                return

            # Extract the open and close time from the DataFrame
            open_time = df['open_time'].min()  # Earliest open_time in the DataFrame
            close_time = df['close_time'].max()  # Latest close_time in the DataFrame

            # Sort by absolute value of rate_change in descending order
            df = df.sort_values(by='rate_change', ascending=False)

            # Format the open and close time as: 'YYYY-MM-DD: HH:MM:SS -> HH:MM:SS'
            date_str = open_time.strftime('%Y-%m-%d')
            open_time_str = open_time.strftime('%H:%M:%S')
            close_time_str = close_time.strftime('%H:%M:%S')

            # Add timeframe to the message
            full_message = f"Binance Spot USDT Market Recap ({timeframe.upper()} timeframe)\n{date_str}: {open_time_str} -> {close_time_str}\n\n"

            # Add each symbol and rate change to the message
            for _, row in df.iterrows():
                rate_change_icon = "🟩" if row['rate_change'] >= 0 else "🟥"
                full_message += f"{rate_change_icon} {row['symbol']} {row['rate_change']:.2f}%\n"

            # Send the entire message as a single Telegram message asynchronously
            await self.bot.send_message(chat_id=self.chat_id, text=f"```\n{full_message}\n```", parse_mode='Markdown')

        except Exception as e:
            logging.error(f"Failed to send message: {e}")
