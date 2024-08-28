def convert_volume_to_usdt(data):
    #Convert volume from base currency to USDT using close price.
    for entry in data:
        entry['volume_usdt'] = entry['volume'] * entry['close']
    return data
