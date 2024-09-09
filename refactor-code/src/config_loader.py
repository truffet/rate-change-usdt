# src/config_loader.py

import json
import os

class ConfigLoader:
    CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config.json')

    @staticmethod
    def load_config():
        """Loads and validates the configuration file."""
        if not os.path.exists(ConfigLoader.CONFIG_PATH):
            raise FileNotFoundError(f"Configuration file {ConfigLoader.CONFIG_PATH} not found!")

        try:
            with open(ConfigLoader.CONFIG_PATH, 'r') as config_file:
                config = json.load(config_file)
            ConfigLoader._validate_config(config)
            return config
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            raise Exception(f"Error loading config: {e}")

    @staticmethod
    def _validate_config(config):
        """Validates that necessary fields are present in the config."""
        required_keys = ["telegram", "interval"]
        telegram_keys = ["bot_token", "chat_id"]

        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required configuration key: {key}")
        
        for key in telegram_keys:
            if key not in config['telegram']:
                raise ValueError(f"Missing Telegram configuration key: {key}")
