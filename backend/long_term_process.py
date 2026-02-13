import os
import json
import logging
import pandas as pd
import numpy as np
import datetime
import subprocess
import sys
from backend.get_tickers import update_stock_csv_from_fmp
from backend.rdt_data_fetcher import get_unique_symbols, download_price_data, merge_price_data, save_price_data, load_existing_price_data
from backend.chart_generator_mx import RDTChartGenerator
# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
PROJECT_ROOT = os.getcwd()
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
STOCK_CSV_PATH = os.path.join(PROJECT_ROOT, 'stock.csv') # Save to root for rdt_data_fetcher
LATEST_JSON_PATH = os.path.join(DATA_DIR, 'latest.json')

def run_calculation_scripts():
    """Runs the calculation scripts as subprocesses."""
    scripts = [
        "backend/calculate_atr_trailing_stop.py",
        "backend/calculate_rs_percentile_histogram.py",
        "backend/calculate_zone_rs.py"
    ]

    for script in scripts:
        logger.info(f"Running {script}...")
        try:
            # Run using the same python interpreter
            subprocess.run([sys.executable, script], check=True, cwd=PROJECT_ROOT)
        except subprocess.CalledProcessError as e:
            logger.error(f"Error running {script}: {e}")
            pass

def generate_charts(stock_list=None, data_date=None):
    """Generates charts based on long_term_ticker.csv."""
    generator = RDTChartGenerator()

    # Read long_term_ticker.csv
    try:
        ticker_df = pd.read_csv("long_term_ticker.csv")
        long_term_tickers = ticker_df['Ticker'].unique().tolist()
    except Exception as e:
        logger.error(f"Error reading long_term_ticker.csv: {e}")
        long_term_tickers = ["QQQ"] # Fallback

    logger.info(f"Generating Long Term Charts for: {long_term_tickers}")

    for ticker in long_term_tickers:
        try:
            logger.info(f"Generating {ticker} Strong Stock Chart...")
            filename = os.path.join(DATA_DIR, f"{ticker}_strong_stock.png")
            generator.generate_chart(ticker, filename)
        except Exception as e:
            logger.error(f"Failed to generate {ticker} chart: {e}")

    # Individual stock chart generation removed as requested
    # if not stock_list:
    #     return

    # logger.info(f"Generating charts for {len(stock_list)} stocks...")

    # for stock in stock_list:
    #     ticker = stock['ticker']
    #     filename = os.path.join(DATA_DIR, f"{chart_date_str}-{ticker}.png")
    #     try:
    #         generator.generate_chart(ticker, filename)
    #     except Exception as e:
    #         logger.error(f"Failed to generate chart for {ticker}: {e}")

def run_long_term_process(force_weekend_mode=False):
    """Main Orchestrator for Long Term Charts."""
    logger.info("Starting Long Term Process...")

    # 1. Update Universe
    test_tickers = os.getenv("TEST_TICKERS")
    if test_tickers:
        logger.info(f"TEST_MODE: Using tickers {test_tickers}")
        with open(STOCK_CSV_PATH, 'w') as f:
            f.write("Symbol,Exchange\n")
            for t in test_tickers.split(','):
                f.write(f"{t.strip()},TEST\n")
    else:
        if not os.path.exists(STOCK_CSV_PATH):
            logger.info("Stock CSV not found. Fetching from FMP...")
        update_stock_csv_from_fmp(STOCK_CSV_PATH)

    # 2. Fetch Data
    existing_data, last_date = load_existing_price_data()
    symbols, start_date = get_unique_symbols()

    if not symbols:
        logger.error("No symbols found.")
        return {}

    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    data_date = None

    if existing_data is not None and last_date is not None:
         start_date_dl = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
         if last_date.date() < datetime.datetime.now().date():
             new_data = download_price_data(symbols, start_date_dl, end_date)
             final_data = merge_price_data(existing_data, new_data) if new_data is not None else existing_data
             save_price_data(final_data)
         else:
             logger.info("Data up to date.")
             final_data = existing_data
    else:
        final_data = download_price_data(symbols, start_date, end_date)
        if final_data is not None:
            save_price_data(final_data)

    if final_data is not None and not final_data.empty:
        data_date = final_data.index[-1]

    if data_date is None:
        logger.error("No data available to process.")
        return {}

    # 3. Run Calculations
    run_calculation_scripts()

    # 4. Generate Charts (Screening Logic Removed)
    generate_charts(None, data_date=data_date)

    # 5. Save JSON (Minimal for Notification compatibility)
    today_str = data_date.strftime('%Y%m%d')
    output_data = {
        "date": data_date.strftime('%Y-%m-%d'),
        "market_status": "Neutral",
        "status_text": "Charts Updated",
        "strong_stocks": [], # Empty list
        "last_updated": datetime.datetime.now().isoformat()
    }

    with open(os.path.join(DATA_DIR, f"{today_str}.json"), 'w') as f:
        json.dump(output_data, f)
    with open(LATEST_JSON_PATH, 'w') as f:
        json.dump(output_data, f)

    logger.info("Screener Process Complete.")
    return output_data

if __name__ == "__main__":
    run_long_term_process()
