import pandas as pd
import yfinance as yf
import logging
import os
import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STOCK_CSV = os.path.join(os.path.dirname(__file__), 'stock.csv')
HISTORY_PERIOD = "2y"  # 2 years to calculate 52-week low (1 year window) + 1 year history

# Climax Logic Constants
CLIMAX_THRESHOLD_DAILY = 20.0
ENTRY_LAG_DAYS = 22

def get_tickers():
    """Reads tickers from stock.csv"""
    try:
        if not os.path.exists(STOCK_CSV):
            logger.error(f"{STOCK_CSV} not found.")
            return []
        df = pd.read_csv(STOCK_CSV)
        # Assuming header exists and column 0 is Ticker
        return df.iloc[:, 0].tolist()
    except Exception as e:
        logger.error(f"Error reading {STOCK_CSV}: {e}")
        return []

def calculate_market_bloodbath_data():
    """
    Calculates Market Bloodbath (New Lows Ratio) for the last year.
    Returns a DataFrame with index 'Date' and column 'New_Lows_Ratio'.
    """
    tickers = get_tickers()
    if not tickers:
        logger.warning("No tickers found for bloodbath calculation.")
        return pd.DataFrame()

    logger.info(f"Found {len(tickers)} tickers. Starting data fetch for Bloodbath calculation...")

    try:
        # Fetch data for all tickers at once
        # We need 'Low' to determine New Lows
        # We fetch without group_by='ticker' to get (Price, Ticker) structure which is standard for 'Low' access
        data = yf.download(tickers, period=HISTORY_PERIOD, interval="1d", progress=False, threads=True)

        if data.empty:
            logger.error("No data fetched.")
            return pd.DataFrame()

        if isinstance(data.columns, pd.MultiIndex):
            # Try to get 'Low'
            try:
                lows = data['Low']
            except KeyError:
                logger.error("'Low' column not found in downloaded data.")
                return pd.DataFrame()
        else:
            # Single ticker?
            if len(tickers) == 1:
                lows = data[['Low']] # Keep as DataFrame
                lows.columns = tickers
            else:
                # Should be MultiIndex if multiple tickers
                lows = data['Low']

        # Now 'lows' is a DataFrame where columns are Tickers and Index is Date.

        # Rolling 52-week low (252 trading days)
        # We want to check if today's Low is the minimum of the last 252 days (inclusive).
        # rolling(252).min() includes the current day if we don't shift.
        # So: Is Low[t] == Min(Low[t-251:t])?
        # Yes, standard approach.

        rolling_min = lows.rolling(window=252, min_periods=50).min()

        # Check for New Lows
        # Condition: Low == RollingMin AND Low is not NaN
        # Also ensure we are not comparing NaN with NaN
        is_new_low = (lows <= rolling_min) & (lows.notna()) & (rolling_min.notna())

        # Count per date
        daily_new_lows = is_new_low.sum(axis=1)
        daily_total_issues = lows.notna().sum(axis=1)

        # Create Result DataFrame
        result = pd.DataFrame({
            'New_Lows': daily_new_lows,
            'Total_Issues': daily_total_issues
        })

        # Calculate Ratio
        # Avoid division by zero
        result['New_Lows_Ratio'] = 0.0
        mask = result['Total_Issues'] > 0
        result.loc[mask, 'New_Lows_Ratio'] = (result.loc[mask, 'New_Lows'] / result.loc[mask, 'Total_Issues']) * 100

        # Ensure index is DatetimeIndex and sorted
        result.sort_index(inplace=True)

        # Calculate Climax Signals (on full history to support lag)
        result['Is_Climax'] = result['New_Lows_Ratio'] >= CLIMAX_THRESHOLD_DAILY
        # Climax Entry is shifted by ENTRY_LAG_DAYS
        result['Climax_Entry'] = result['Is_Climax'].shift(ENTRY_LAG_DAYS).fillna(False)

        # Filter for the last 1 year (or return all, caller can slice)
        # User said "1 year of bloodbath". Let's return last 365 days.
        cutoff_date = result.index.max() - pd.Timedelta(days=365)
        result = result[result.index >= cutoff_date]

        logger.info(f"Calculated Bloodbath data for {len(result)} days.")
        return result[['New_Lows_Ratio', 'Climax_Entry']]

    except Exception as e:
        logger.error(f"Error calculating market bloodbath: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

if __name__ == "__main__":
    # Test run
    df = calculate_market_bloodbath_data()
    print(df.tail())
