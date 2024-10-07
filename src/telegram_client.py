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
            # Filter pairs where either combined Z-score has an absolute value >= 2
            filtered_df = df[
                (df['z_combined_pair'].abs() >= 2) | (df['z_combined_all_pairs'].abs() >= 2)
            ]

            if filtered_df.empty:
                logging.info("No pairs meet the Z-score threshold, no message sent.")
                return

            # Prepare message header with open and close times
            full_message = f"📅 **Candlestick Data (Filtered)**\nOpen Time: {open_time} | Close Time: {close_time}\n\n"
            
            # Loop through each row in the filtered DataFrame and add trading pair info
            for _, row in filtered_df.iterrows():
                rate_change_icon = "🔺" if row['rate_change'] > 0 else "🔻"
                volume_icon = "🟩" if row['rate_change'] > 0 else "🟥"
                
                full_message += (
                    f"💲 {row['symbol']} {rate_change_icon}{row['rate_change']:.2f}% {volume_icon}{row['quote_volume']:.0f} USDT\n"
                    f"| Pair-Specific Z-Scores: R-Z: {row['z_rate_change_pair']:.2f} | V-Z: {row['z_volume_pair']:.2f} | C-Z: {row['z_combined_pair']:.2f}\n"
                    f"| Cross-Pair Z-Scores: R-Z: {row['z_rate_change_all_pairs']:.2f} | V-Z: {row['z_volume_all_pairs']:.2f} | C-Z: {row['z_combined_all_pairs']:.2f}\n\n"
                )
            
            # Send the entire message as a single Telegram message asynchronously
            await self.bot.send_message(chat_id=self.chat_id, text=full_message, parse_mode='Markdown')

        except Exception as e:
            logging.error(f"Failed to send message: {e}")
