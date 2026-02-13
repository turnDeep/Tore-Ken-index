import os
import json
import logging
import datetime
import pandas as pd
from backend.market_analysis_logic import get_market_analysis_data
from backend.market_chart_generator import generate_market_chart
from backend.market_bloodbath import calculate_market_bloodbath_data

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
PROJECT_ROOT = os.getcwd()
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

def run_short_term_process():
    """
    Generates Market Analysis Data (Short Term Charts) for tickers in short_term_ticker.csv.
    Returns the market data for the primary ticker (first in list).
    """
    logger.info("Starting Short Term Process...")

    try:
        csv_path = os.path.join(os.path.dirname(__file__), 'short_term_ticker.csv')
        st_df = pd.read_csv(csv_path)
        short_term_tickers = st_df['Ticker'].unique().tolist()
    except Exception as e:
        logger.error(f"Error reading short_term_ticker.csv: {e}")
        short_term_tickers = ["SPY"]

    logger.info(f"Processing Short Term Tickers: {short_term_tickers}")

    # Calculate Market Bloodbath ONCE
    try:
        logger.info("Calculating Market Bloodbath Data (once for all tickers)...")
        bloodbath_df = calculate_market_bloodbath_data()
    except Exception as e:
        logger.error(f"Failed to calculate bloodbath data: {e}")
        bloodbath_df = None

    primary_market_data = None # Will store the first ticker's data for legacy/notification support

    for i, ticker in enumerate(short_term_tickers):
        logger.info(f"Processing {ticker}...")
        market_data, spy_df = get_market_analysis_data(ticker=ticker, period="6mo", bloodbath_df=bloodbath_df)

        if market_data:
            # Generate Chart Image: {Ticker}_market_chart.png
            chart_path = os.path.join(DATA_DIR, f"{ticker}_market_chart.png")
            generate_market_chart(spy_df, chart_path)

            # Save Data: {Ticker}_market_analysis.json
            analysis_file = os.path.join(DATA_DIR, f"{ticker}_market_analysis.json")
            with open(analysis_file, "w") as f:
                json.dump({
                    "ticker": ticker,
                    "history": market_data,
                    "last_updated": datetime.datetime.now().isoformat()
                }, f)

            # If first ticker, save as legacy 'market_chart.png' and 'market_analysis.json' for backward compatibility
            if i == 0:
                primary_market_data = market_data

                # Legacy Image
                legacy_chart_path = os.path.join(DATA_DIR, "market_chart.png")
                generate_market_chart(spy_df, legacy_chart_path)

                # Legacy JSON
                legacy_analysis_file = os.path.join(DATA_DIR, "market_analysis.json")
                with open(legacy_analysis_file, "w") as f:
                    json.dump({
                        "history": market_data,
                        "last_updated": datetime.datetime.now().isoformat()
                    }, f)

    return primary_market_data

if __name__ == "__main__":
    run_short_term_process()
