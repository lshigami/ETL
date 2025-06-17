import requests
from .config import get_app_config

def fetch_btcusdt_price():
    """Lấy giá BTCUSDT từ Binance API."""
    config = get_app_config()
    url = config['binance']['api_url']
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if 'symbol' in data and 'price' in data:
            return {
                'symbol': data['symbol'],
                'price': float(data['price'])
            }
        else:
            raise ValueError("Invalid response format from Binance API")
    except requests.RequestException as e:
        print(f"Error fetching price: {e}")
        return None