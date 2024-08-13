# Trend Analysis Simplified

This project fetches market data for USDT pairs from Binance and sorts them by percentage change between Open and Close prices.

## Setup

1. Create and activate a virtual environment:
    ```bash
    python -m venv env
    source env/bin/activate  # On Windows use `env\Scripts\activate`
    ```

2. Install the dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1. Fetch market data:
    ```bash
    python fetch_market_data.py
    ```

2. Sort and save the data:
    ```bash
    python sort_and_save.py
    ```
