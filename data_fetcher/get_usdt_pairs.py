import requests

def get_usdt_pairs():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        if 'symbols' in data:
            return [
                symbol['symbol'] for symbol in data['symbols']
                if symbol['quoteAsset'] == 'USDT'
            ]
        else:
            print("API response does not contain 'symbols':", data)
            return []
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")
        return []
