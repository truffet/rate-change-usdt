import logging
from telegram import Bot
import asyncio
from datetime import datetime, timezone

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
            close_time = df['close_time'].max()  # Latest close_time in the DataFrame (this is currently in int timestamp)
            print(close_time)

            # Convert close_time from int (timestamp) to datetime
            close_time_dt = datetime.utcfromtimestamp(close_time / 1000)  # Convert milliseconds to seconds for datetime

            # Format the open and close time as: 'YYYY-MM-DD: HH:MM:SS -> HH:MM:SS'
            open_time_str = str(open_time)  # Keep open_time as raw timestamp (or format it if needed)
            close_time_str = close_time_dt.strftime('%Y-%m-%d %H:%M:%S')  # Convert close_time to datetime string

            # Add timeframe to the message
            full_message = f"Binance Spot USDT Market Recap ({timeframe.upper()} timeframe)\n"
            full_message += f"Open Time: {open_time_str}\nClose Time: {close_time_str}\n"
            full_message += "Symbol / Rate Change % Open-Close / % Volatility\n"
            # Sort DataFrame by 'rate_change_open_close' in descending order
            df = df.sort_values(by='rate_change_open_close', ascending=False)

            # Add each symbol and rate change to the message
            for _, row in df.iterrows():
                rate_change_icon = "🟩" if row['rate_change_open_close'] >= 0 else "🟥"
                full_message += (
                    f"{rate_change_icon} {row['symbol']} / "
                    f"{row['rate_change_open_close']:.2f}% / "
                    f"{row['rate_change_high_low']:.2f}%\n"
                )

            # Send the entire message as a single Telegram message asynchronously
            await self.bot.send_message(chat_id=self.chat_id, text=f"```\n{full_message}\n```", parse_mode='Markdown')

        except Exception as e:
            logging.error(f"Failed to send message: {e}")
