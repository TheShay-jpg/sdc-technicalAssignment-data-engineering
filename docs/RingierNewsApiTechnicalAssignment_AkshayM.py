import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_news(api_key, country='eu', from_date='2024-02-25', to_date='2024-02-26'):
    url = f'https://newsapi.org/v2/everything?q=Europe&from={from_date}&to={to_date}&apiKey={api_key}'
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        if data.get('status') == 'ok':
            return data['articles']
        else:
            print(f'Failed to fetch news articles: {data.get("message")}')
            return []
    except requests.exceptions.RequestException as e:
        print(f'Error fetching news articles: {e}')
        return []



