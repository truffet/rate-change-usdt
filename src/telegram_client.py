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
            # Sort by absolute value of z_combined_pair and separate into positives and negatives
            df['z_combined_pair_abs'] = df['z_combined_pair'].abs()
            positive_df = df[df['z_combined_pair'] >= 0].sort_values(by='z_combined_pair_abs', ascending=False)
            negative_df = df[df['z_combined_pair'] < 0].sort_values(by='z_combined_pair_abs', ascending=False)

            if positive_df.empty and negative_df.empty:
                logging.info("No pairs meet the Z-score threshold, no message sent.")
                return

            # Prepare message header with open and close times
            full_message = f"📅 **Binance Spot USDT Market Recap**\nOpen Time: {open_time} | Close Time: {close_time}\n\n"

            # Create two lists for positive and negative changes
            pos_lines = []
            neg_lines = []

            # Add positive values to pos_lines
            for _, row in positive_df.iterrows():
                rate_change_icon = "🟩"
                pos_lines.append(f"{rate_change_icon} {row['symbol']} {row['rate_change']:.2f}%")

            # Add negative values to neg_lines
            for _, row in negative_df.iterrows():
                rate_change_icon = "🟥"
                neg_lines.append(f"{rate_change_icon} {row['symbol']} {row['rate_change']:.2f}%")

            # Make sure both lists have the same length for side-by-side display
            max_len = max(len(pos_lines), len(neg_lines))
            pos_lines.extend([""] * (max_len - len(pos_lines)))  # Fill with empty strings
            neg_lines.extend([""] * (max_len - len(neg_lines)))

            # Combine the two columns into a side-by-side format
            for pos, neg in zip(pos_lines, neg_lines):
                full_message += f"{pos:<30} {neg}\n"

            # Send the entire message as a single Telegram message asynchronously
            await self.bot.send_message(chat_id=self.chat_id, text=f"```\n{full_message}\n```", parse_mode='Markdown')

        except Exception as e:
            logging.error(f"Failed to send message: {e}")

