import json
from data_fetcher.get_usdt_pairs import get_usdt_pairs
from data_fetcher.fetch_latest_market_data import fetch_latest_market_data
from data_processor.calculate_rate_change import calculate_rate_change
from data_processor.convert_volume_to_usdt import convert_volume_to_usdt
from data_processor.calculate_z_scores import calculate_z_scores, combine_z_scores
from data_processor.data_sort_by import data_sort_by
from io_operations.save_to_csv import save_to_csv

def load_config(config_file='config.json'):
    with open(config_file, 'r') as file:
        config = json.load(file)
    return config

def main():
    # Load configuration
    config = load_config()
    interval = config.get('interval', '4h')  # Default to '4h' if not found

    usdt_pairs = get_usdt_pairs()
    all_data = []
    timestamp = None  # Initialize timestamp

    for pair in usdt_pairs:
        print(f"Fetching latest data for {pair}")  # Console print for debugging
        data = fetch_latest_market_data(pair, interval)
        all_data.append(data)
        print(f"Latest data for {pair} fetched successfully")  # Console print for debugging
        if not timestamp:  # Set timestamp from the first pair data
            timestamp = data['timestamp']

    # Calculate percentage change
    processed_data = calculate_rate_change(all_data)
    
    # Convert volume to USDT
    processed_data = convert_volume_to_usdt(processed_data)

    # Calculate Z-scores for percentage change and volume in USDT
    processed_data = calculate_z_scores(processed_data)

    # Combine the Z-scores into a single metric
    processed_data = combine_z_scores(processed_data)

    # Sort the processed data by % change
    rate_sorted_data = data_sort_by(processed_data, 'pct_change')
    # Sort the processed data by combined Z score
    z_sorted_data = data_sort_by(processed_data, 'combined_z_score')

    # Filter data with combined Z score >= 2
    filtered_data = processed_data[processed_data['combined_z_score'] >= 2]

    # Save the sorted data to CSV with timestamp in filenames
    save_to_csv(rate_sorted_data, f'data/{timestamp}_rate_sorted_market_data.csv')
    save_to_csv(z_sorted_data, f'data/{timestamp}_z_sorted_market_data.csv')
    save_to_csv(filtered_data, f'data/{timestamp}_filtered_market_data.csv')

if __name__ == "__main__":
    main()
