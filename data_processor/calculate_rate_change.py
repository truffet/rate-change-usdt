def calculate_percentage_change(open_price, close_price):
    return ((close_price - open_price) / open_price) * 100

def calculate_rate_change(data):
    processed_data = []
    for entry in data:
        pct_change = calculate_percentage_change(entry['open'], entry['close'])
        entry['pct_change'] = pct_change
        processed_data.append(entry)
    return processed_data
