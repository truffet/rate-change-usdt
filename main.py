from data_fetcher.get_usdt_pairs import get_usdt_pairs
from data_fetcher.fetch_latest_market_data import fetch_latest_market_data
from data_processor.process_market_data import process_market_data
from data_processor.sort_by_percentage_change import sort_by_percentage_change
from io_operations.save_to_csv import save_to_csv

def main():
    usdt_pairs = get_usdt_pairs()
    interval = '4h'  # 4-hour interval
    all_data = []

    for pair in usdt_pairs:
        print(f"Fetching latest data for {pair}")  # Console print for debugging
        data = fetch_latest_market_data(pair, interval)
        all_data.append(data)
        print(f"Latest data for {pair} fetched successfully")  # Console print for debugging

    # Process the market data
    processed_data = process_market_data(all_data)
    # Sort the processed data by percentage change
    sorted_data = sort_by_percentage_change(processed_data)
    # Save the sorted data to CSV
    save_to_csv(sorted_data, 'data/sorted_market_data.csv')

if __name__ == "__main__":
    main()
