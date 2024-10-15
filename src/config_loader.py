# src/config_loader.py

import json
import os

class ConfigLoader:
    CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config.json')

    @staticmethod
    def load_config():
        """Loads and validates the Telegram configuration from the config.json file."""
        if not os.path.exists(ConfigLoader.CONFIG_PATH):
            raise FileNotFoundError(f"Configuration file {ConfigLoader.CONFIG_PATH} not found!")

        try:
            with open(ConfigLoader.CONFIG_PATH, 'r') as config_file:
                config = json.load(config_file)
            ConfigLoader._validate_telegram_config(config)
            return config
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            raise Exception(f"Error loading config: {e}")

    @staticmethod
    def _validate_telegram_config(config):
        """Validates that necessary Telegram fields are present in the config."""
        telegram_keys = ["bot_token", "chat_id"]

        if "telegram" not in config:
            raise ValueError("Missing 'telegram' key in config.")

        for key in telegram_keys:
            if key not in config['telegram']:
                raise ValueError(f"Missing Telegram configuration key: {key}")
