import yfinance as yf
import json
import os
import logging
from datetime import datetime
from typing import List, Dict

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.getcwd(), 'data')
NEWS_JSON_PATH = os.path.join(DATA_DIR, 'news.json')

def fetch_news_for_tickers(tickers: List[str]) -> Dict[str, List[Dict]]:
    """
    Fetches the latest 10 news items for each ticker from Yahoo Finance.
    Returns a dictionary keyed by ticker.
    """
    all_news = {}

    for ticker in tickers:
        logger.info(f"Fetching news for {ticker}...")
        try:
            t = yf.Ticker(ticker)
            news_items = t.news

            # yfinance returns a list of dicts. We want top 10.
            # We'll extract relevant fields to keep it clean.
            cleaned_news = []
            if news_items:
                for item in news_items[:10]:
                    content = item.get('content', {})

                    # Extract timestamp
                    # Try to parse 'pubDate' (ISO format) or 'providerPublishTime' if available
                    pub_date_str = "Unknown"
                    if 'pubDate' in content:
                        try:
                            # 2026-02-15T16:22:57Z
                            dt = datetime.strptime(content['pubDate'], '%Y-%m-%dT%H:%M:%SZ')
                            pub_date_str = dt.strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            pub_date_str = content['pubDate']

                    # Link logic: clickThroughUrl.url or canonicalUrl.url
                    link = "#"
                    if content.get('clickThroughUrl'):
                        link = content['clickThroughUrl'].get('url', '#')
                    elif content.get('canonicalUrl'):
                        link = content['canonicalUrl'].get('url', '#')

                    publisher = "Unknown"
                    if content.get('provider'):
                        publisher = content['provider'].get('displayName', 'Unknown')

                    cleaned_news.append({
                        "title": content.get('title', 'No Title'),
                        "link": link,
                        "publisher": publisher,
                        "published": pub_date_str
                    })

            all_news[ticker] = cleaned_news
            logger.info(f"Found {len(cleaned_news)} news items for {ticker}.")

        except Exception as e:
            logger.error(f"Failed to fetch news for {ticker}: {e}")
            all_news[ticker] = []

    return all_news

def save_news_to_json(news_data: Dict[str, List[Dict]]):
    """Saves the news data to data/news.json."""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(NEWS_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(news_data, f, indent=4, ensure_ascii=False)
        logger.info(f"News data saved to {NEWS_JSON_PATH}")
    except Exception as e:
        logger.error(f"Failed to save news JSON: {e}")

if __name__ == "__main__":
    tickers = ["SPY", "QQQ", "SOXX", "GLD"]
    news = fetch_news_for_tickers(tickers)
    save_news_to_json(news)
