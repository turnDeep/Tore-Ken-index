import os
import json
import logging
import google.generativeai as genai
from dotenv import load_dotenv
from backend.unified_data_manager import load_unified_data, save_unified_data

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    logger.warning("GEMINI_API_KEY not found in .env. Gemini analysis will be skipped.")

DATA_DIR = os.path.join(os.getcwd(), 'data')
NEWS_JSON_PATH = os.path.join(DATA_DIR, 'news.json')

def load_news_data():
    """Loads news data from JSON."""
    if not os.path.exists(NEWS_JSON_PATH):
        logger.warning(f"News data file not found at {NEWS_JSON_PATH}")
        return {}
    with open(NEWS_JSON_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

def generate_analysis_for_ticker(ticker, short_term_data, long_term_data, news_items):
    """
    Generates analysis using Gemini API.
    """
    if not GEMINI_API_KEY:
        return "Gemini API Key not configured."

    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-2.0-flash') # Use a capable model

        # Prepare Data Summary for Prompt
        st_summary = "No Short Term Data"
        if short_term_data and 'history' in short_term_data:
            # Get last 5 days
            history = short_term_data['history'][-5:]
            st_lines = []
            for h in history:
                st_lines.append(f"{h['date']}: Close {h['close']:.2f}, Status {h.get('status_text', 'N/A')}")
            st_summary = "\n".join(st_lines)

        lt_summary = "No Long Term Data"
        if long_term_data:
            lt_summary = f"Status: {long_term_data.get('status', 'Unknown')}, Last Updated: {long_term_data.get('last_updated', 'Unknown')}"

        news_summary = "No Recent News"
        if news_items:
            news_lines = []
            for item in news_items[:5]: # Top 5 news
                news_lines.append(f"- {item.get('title', 'No Title')} ({item.get('published', '')})")
            news_summary = "\n".join(news_lines)

        prompt = f"""
あなたはプロの市場アナリストです。以下のデータを基に、{ticker}の現状分析と今後の見通しを1000文字以内の日本語で記述してください。

【短期チャートデータ (直近5日)】
{st_summary}

【長期チャートステータス】
{lt_summary}

【直近のニュース】
{news_summary}

出力フォーマット:
1. 現状分析: (テクニカル・ファンダメンタル両面から)
2. 今後の見通し: (注目ポイントやシナリオ)
"""

        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        logger.error(f"Gemini API Error for {ticker}: {e}")
        return f"AI分析の生成に失敗しました: {e}"

def run_gemini_analysis():
    """
    Main function to run Gemini analysis for all tickers.
    """
    logger.info("Starting Gemini Analysis Process...")

    unified_data = load_unified_data()
    news_data = load_news_data()

    tickers = ["SPY", "QQQ", "SOXX", "GLD"]

    for ticker in tickers:
        logger.info(f"Analyzing {ticker}...")

        # Get Data
        st_data = unified_data.get(ticker, {}).get('short_term', {})
        lt_data = unified_data.get(ticker, {}).get('long_term', {})
        ticker_news = news_data.get(ticker, [])

        # Generate Analysis
        analysis_text = generate_analysis_for_ticker(ticker, st_data, lt_data, ticker_news)

        # Update Unified Data
        if ticker not in unified_data:
            unified_data[ticker] = {}

        unified_data[ticker]['gemini_analysis'] = {
            "text": analysis_text,
            "last_updated": datetime.now().isoformat()
        }

    save_unified_data(unified_data)
    logger.info("Gemini Analysis Complete.")

if __name__ == "__main__":
    from datetime import datetime
    run_gemini_analysis()
