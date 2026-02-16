import os
import json
import logging
import datetime
import yfinance as yf
from typing import List, Dict, Any
from backend.unified_data_manager import load_unified_data, save_unified_data

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
PROJECT_ROOT = os.getcwd()
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
NEWS_JSON_PATH = os.path.join(DATA_DIR, 'market_news.json')

def fetch_ticker_news(ticker_symbol: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Fetches the latest news for a given ticker using yfinance.
    """
    try:
        ticker = yf.Ticker(ticker_symbol)
        news = ticker.news
        if not news:
            logger.warning(f"No news found for {ticker_symbol}")
            return []

        # Format news items
        formatted_news = []
        for item in news[:limit]:
            # Handle potential nested structure in content
            content = item.get("content", {})
            title = content.get("title") if isinstance(content, dict) else item.get("title")
            pub_date = content.get("pubDate") if isinstance(content, dict) else item.get("providerPublishTime")
            link = content.get("clickThroughUrl", {}).get("url") if isinstance(content, dict) and content.get("clickThroughUrl") else item.get("link")

            # If still null, try direct access (yfinance structure varies)
            if not title and "title" in item: title = item["title"]
            if not link and "link" in item: link = item["link"]

            formatted_news.append({
                "title": title,
                "publisher": item.get("provider", {}).get("displayName") if isinstance(item.get("provider"), dict) else item.get("publisher"),
                "link": link,
                "providerPublishTime": pub_date,
                "type": item.get("type")
            })
        return formatted_news
    except Exception as e:
        logger.error(f"Error fetching news for {ticker_symbol}: {e}")
        return []

def save_news_to_file(news_data: Dict[str, List[Dict[str, Any]]]):
    """Saves the fetched news to a JSON file for debugging/reference."""
    try:
        # Ensure directory exists
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(NEWS_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(news_data, f, ensure_ascii=False, indent=4)
        logger.info(f"News data saved to {NEWS_JSON_PATH}")
    except Exception as e:
        logger.error(f"Failed to save news data: {e}")

def generate_gemini_analysis():
    """
    Orchestrates the Gemini analysis process:
    1. Fetches news for SPY, QQQ, SOXX, GLD.
    2. Loads existing market analysis data.
    3. Constructs a prompt for Gemini.
    4. Calls Gemini API.
    5. Updates the unified data with the analysis.
    """
    logger.info("Starting Gemini Analysis Process...")

    # Delayed import to avoid issues if module not present during initial load
    try:
        import google.generativeai as genai
        # Check for dotenv only if needed, usually loaded by main app
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
    except ImportError:
        logger.error("google-generativeai module not found. Skipping analysis.")
        return

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.error("GEMINI_API_KEY not found in environment variables.")
        return

    # 1. Fetch News
    target_tickers = ["SPY", "QQQ", "SOXX", "GLD"]
    news_data = {}
    for ticker in target_tickers:
        logger.info(f"Fetching news for {ticker}...")
        news_data[ticker] = fetch_ticker_news(ticker)

    save_news_to_file(news_data)

    # 2. Load Market Analysis Data
    unified_data = load_unified_data()

    # 3. Construct Prompt
    prompt = "あなたはプロの金融アナリストです。以下のデータ（ニュースとテクニカル分析データ）に基づき、SPY, QQQ, SOXX, GLD の現状と今後について、それぞれ1000文字程度で「AI解説」を作成してください。\n"
    prompt += "回答は必ず以下のJSONフォーマットで出力してください。Markdownのコードブロックなどは含めないでください。\n"
    prompt += '{\n  "SPY": "分析テキスト...",\n  "QQQ": "分析テキスト...",\n  "SOXX": "分析テキスト...",\n  "GLD": "分析テキスト..."\n}\n\n'

    prompt += "--- インジケータ解説 ---\n"
    prompt += "【短期（日足）】\n"
    prompt += "- Trend: Green=上昇トレンド, Red=下落トレンド\n"
    prompt += "- Bloodbath: 「暴落シグナル」。Safe=安全, Warning=警戒, Danger=危険\n"
    prompt += "- TSV (Time Segmented Volume): 資金流出入。Bull=買い優勢, Bear=売り優勢\n"
    prompt += "【長期（週足）】\n"
    prompt += "- ATR Trailing Stop: トレンドフォロー指標。Buy=買いシグナル, Sell=売りシグナル\n"
    prompt += "- RS Rating (Relative Strength): 市場全体に対する相対的な強さ（0-100）。80以上は強い。\n"
    prompt += "- Zone: RSとそのモメンタムに基づく状態区分。\n"
    prompt += "  - Leading: 先導（強い）\n"
    prompt += "  - Weakening: 弱含み（調整中）\n"
    prompt += "  - Lagging: 遅行（弱い）\n"
    prompt += "  - Improving: 改善（回復中）\n\n"

    prompt += "--- ニュース情報 ---\n"
    for ticker, items in news_data.items():
        prompt += f"【{ticker} ニュース】\n"
        for item in items:
            date_val = item.get('providerPublishTime')
            if isinstance(date_val, int):
                date_str = datetime.datetime.fromtimestamp(date_val).strftime('%Y-%m-%d %H:%M:%S')
            else:
                date_str = str(date_val)

            title = item.get('title', 'No Title')
            publisher = item.get('publisher', 'Unknown')
            prompt += f"- {date_str} : {title} ({publisher})\n"
        prompt += "\n"

    prompt += "--- テクニカル分析データ（参考） ---\n"
    for ticker in target_tickers:
        data = unified_data.get(ticker, {})
        short_term = data.get("short_term", {}).get("history", [])
        long_term = data.get("long_term", {}).get("data", [])

        prompt += f"【{ticker} データ概要】\n"
        if short_term:
            latest = short_term[-1]
            # Try to get TSV signal if available, otherwise default to N/A
            tsv_signal = latest.get('tsv_signal', 'N/A')
            prompt += f"短期（日足）: 日付={latest.get('date')}, 終値={latest.get('close')}, Volume={latest.get('volume')}, Bloodbath={latest.get('bloodbath_label')}, トレンド={latest.get('trend_color')}, TSV={tsv_signal}\n"

        if long_term:
             if isinstance(long_term, list) and len(long_term) > 0:
                 latest_lt = long_term[-1]
                 # Try to get ATR signal if available
                 atr_signal = latest_lt.get('atr_signal', 'N/A')
                 prompt += f"長期（週足）: 日付={latest_lt.get('date')}, ATR Signal={atr_signal}, RS Rating={latest_lt.get('rs_rating', 'N/A')}, Zone={latest_lt.get('zone_label', 'N/A')}\n"
        prompt += "\n"

    # 4. Call Gemini API
    try:
        genai.configure(api_key=api_key)
        model_name = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
        model = genai.GenerativeModel(model_name)

        logger.info(f"Calling Gemini API ({model_name})...")
        response = model.generate_content(prompt)

        response_text = response.text
        # Clean up if markdown code blocks are present
        response_text = response_text.replace("```json", "").replace("```", "").strip()

        try:
            analysis_result = json.loads(response_text)
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON from Gemini response: {response_text[:100]}...")
            return

        # 5. Update Unified Data
        for ticker, text in analysis_result.items():
            if ticker not in unified_data:
                unified_data[ticker] = {}

            unified_data[ticker]["gemini_analysis"] = {
                "content": text,
                "updated_at": datetime.datetime.now().isoformat()
            }
            logger.info(f"Updated Gemini analysis for {ticker}")

        save_unified_data(unified_data)
        logger.info("Gemini Analysis Process Completed Successfully.")

    except Exception as e:
        logger.error(f"Gemini API Error or Processing Failed: {e}")

if __name__ == "__main__":
    generate_gemini_analysis()
