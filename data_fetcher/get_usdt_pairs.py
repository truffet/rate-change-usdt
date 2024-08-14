import requests

def get_usdt_pairs():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)
    data = response.json()
    # Filter and return only the symbols with USDT as the quote asset that are spot and actively trading
    usdt_pairs = [
        symbol['symbol'] for symbol in data['symbols']
        if symbol['quoteAsset'] == 'USDT' and symbol['status'] == 'TRADING' and symbol['isSpotTradingAllowed']
    ]
    return usdt_pairs
