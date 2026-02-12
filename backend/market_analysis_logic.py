import pandas as pd
import pandas_ta as ta
import numpy as np
import yfinance as yf
import datetime
import logging
from .market_bloodbath import calculate_market_bloodbath_data

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_wma(series, length):
    """Calculates Weighted Moving Average (WMA)."""
    weights = np.arange(1, length + 1)
    sum_weights = weights.sum()
    return series.rolling(window=length).apply(lambda x: np.dot(x, weights) / sum_weights, raw=True)

def calculate_tsv_approximation(df, length=13, ma_length=3, ma_type='EMA'):
    """
    Calculates Time Segmented Volume (TSV) approximation.
    Updated default ma_length to 3 to match Pine Script default.
    """
    price_change = df['Close'].diff()
    signed_volume = df['Volume'] * price_change
    tsv_raw = signed_volume.rolling(window=length).sum()

    if ma_type == 'EMA':
        tsv_smoothed = tsv_raw.ewm(span=ma_length, adjust=False).mean()
    elif ma_type == 'SMA':
        tsv_smoothed = tsv_raw.rolling(window=ma_length).mean()
    else:
        tsv_smoothed = tsv_raw.rolling(window=ma_length).mean()

    return tsv_smoothed

def detect_tsv_divergences(df, lbL=5, lbR=5, min_range=2, max_range=100):
    """
    Detects TSV Divergences (Regular Bullish/Bearish) using strict NumPy windowing logic.
    Mimics Pine Script's pivothigh/pivotlow behavior.
    """
    if 'TSV' not in df.columns or 'Close' not in df.columns:
        return [], []

    closes = df['Close'].values
    tsvs = df['TSV'].values
    n = len(df)

    bull_divs = []
    bear_divs = []

    is_pivot_low = np.zeros(n, dtype=bool)
    is_pivot_high = np.zeros(n, dtype=bool)

    # 1. Pivot Detection (Strict Window)
    for i in range(lbL, n - lbR):
        window = tsvs[i-lbL : i+lbR+1]
        # Check if max/min in window AND not flat (simple check)
        if tsvs[i] == np.min(window):
            is_pivot_low[i] = True
        if tsvs[i] == np.max(window):
            is_pivot_high[i] = True

    # 2. Divergence Check with Range Limits
    for i in range(lbL, n - lbR):
        if is_pivot_low[i]:
            prev_idx = None
            # Search backward for previous pivot
            for k in range(i - 1, -1, -1):
                if is_pivot_low[k]:
                    bars_diff = i - k
                    if min_range <= bars_diff <= max_range:
                        prev_idx = k
                        break
                    elif bars_diff > max_range:
                        break

            if prev_idx is not None:
                # Regular Bullish: Price LL, TSV HL
                if closes[i] < closes[prev_idx] and tsvs[i] > tsvs[prev_idx]:
                    bull_divs.append({'curr': i, 'prev': prev_idx})

        if is_pivot_high[i]:
            prev_idx = None
            for k in range(i - 1, -1, -1):
                if is_pivot_high[k]:
                    bars_diff = i - k
                    if min_range <= bars_diff <= max_range:
                        prev_idx = k
                        break
                    elif bars_diff > max_range:
                        break

            if prev_idx is not None:
                # Regular Bearish: Price HH, TSV LH
                if closes[i] > closes[prev_idx] and tsvs[i] < tsvs[prev_idx]:
                    bear_divs.append({'curr': i, 'prev': prev_idx})

    return bull_divs, bear_divs

def calculate_stochrsi_1op(df, rsi_length=14, stoch_length=14, k_smooth=5, d_smooth=5):
    """
    Calculates StochRSI with HEAVIER WMA smoothing (5, 5) to mimic 1OP cycles.
    """
    rsi = ta.rsi(df['Close'], length=rsi_length)
    rsi_low = rsi.rolling(window=stoch_length).min()
    rsi_high = rsi.rolling(window=stoch_length).max()

    denominator = rsi_high - rsi_low
    denominator = denominator.replace(0, np.nan)

    stoch_raw = ((rsi - rsi_low) / denominator) * 100
    stoch_raw = stoch_raw.fillna(50)

    k_line = calculate_wma(stoch_raw, k_smooth)
    d_line = calculate_wma(k_line, d_smooth)

    return k_line, d_line

def detect_cycle_phases(df):
    """
    Detects Bullish and Bearish Cycle Phases based on StochRSI Crosses.
    """
    if 'Fast_K' not in df.columns or 'Slow_D' not in df.columns:
        return None, None

    k = df['Fast_K'].values
    d = df['Slow_D'].values

    bullish_phase = np.zeros(len(df), dtype=bool)
    bearish_phase = np.zeros(len(df), dtype=bool)

    # State: 0 = Neutral/Unknown, 1 = Bullish, -1 = Bearish
    state = 0

    for i in range(1, len(df)):
        # Check Crosses
        cross_up = (k[i-1] <= d[i-1]) and (k[i] > d[i])
        cross_down = (k[i-1] >= d[i-1]) and (k[i] < d[i])

        if cross_up:
            state = 1
        elif cross_down:
            state = -1

        if state == 1:
            bullish_phase[i] = True
        elif state == -1:
            bearish_phase[i] = True

    return bullish_phase, bearish_phase

def get_market_analysis_data(ticker="SPY", period="6mo"):
    """
    Fetches data for the given ticker, calculates indicators, and returns a list of dictionaries.
    Returns: (list_of_dicts, dataframe)
    """
    try:
        # Use simple download.
        df = yf.download(ticker, period=period, interval="1d", progress=False)
        if df.empty:
            logger.error("Market data download failed")
            return [], pd.DataFrame()

        # Handle MultiIndex columns (Price, Ticker) -> Flatten to just Price
        if isinstance(df.columns, pd.MultiIndex):
            try:
                df.columns = df.columns.droplevel('Ticker')
            except KeyError:
                if df.columns.nlevels > 1:
                     df.columns = df.columns.droplevel(1)

        # Ensure Index is tz-naive
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        df.columns = [c.capitalize() for c in df.columns]

        # Indicators
        # UPDATED: Use length=13, ma_length=3 for TSV as per Pine Script
        df['TSV'] = calculate_tsv_approximation(df, length=13, ma_length=3, ma_type='EMA')
        df['Fast_K'], df['Slow_D'] = calculate_stochrsi_1op(df, rsi_length=14, stoch_length=14, k_smooth=5, d_smooth=5)

        # Detect TSV Divergences
        bull_divs, bear_divs = detect_tsv_divergences(df, lbL=5, lbR=5, min_range=2, max_range=100)

        # Add Divergence info to dataframe for easier result construction
        # We will store the 'prev' index for the 'curr' index
        df['Bullish_Divergence'] = None
        df['Bearish_Divergence'] = None

        for d in bull_divs:
            curr = d['curr']
            prev = d['prev']
            # Store dict or just prev index? Storing prev index allows us to reconstruct the line
            df.at[df.index[curr], 'Bullish_Divergence'] = prev

        for d in bear_divs:
            curr = d['curr']
            prev = d['prev']
            df.at[df.index[curr], 'Bearish_Divergence'] = prev

        # Phases
        bull_mask, bear_mask = detect_cycle_phases(df)
        df['Bullish_Phase'] = bull_mask
        df['Bearish_Phase'] = bear_mask

        # --- Integrate Market Bloodbath Data ---
        try:
            bloodbath_df = calculate_market_bloodbath_data()
            if not bloodbath_df.empty:
                # Merge with SPY df based on Date Index
                # Left join to keep SPY dates
                df = df.join(bloodbath_df, how='left')
                # Fill NaN with 0 or previous value? Fill 0 for safety if missing.
                df['New_Lows_Ratio'] = df['New_Lows_Ratio'].fillna(0.0)
                df['Climax_Entry'] = df['Climax_Entry'].fillna(False)
            else:
                df['New_Lows_Ratio'] = 0.0
                df['Climax_Entry'] = False
        except Exception as e:
            logger.error(f"Failed to integrate bloodbath data: {e}")
            df['New_Lows_Ratio'] = 0.0
            df['Climax_Entry'] = False

        # Generate Trend Signal for Chart Generator (1: Bull, -1: Bear, 0: Neutral)
        conditions = [df['Bullish_Phase'] == True, df['Bearish_Phase'] == True]
        choices = [1, -1]
        df['Trend_Signal'] = np.select(conditions, choices, default=0)

        results = []
        for i in range(len(df)):
            date = df.index[i]
            date_key = date.strftime('%Y%m%d')
            date_str = date.strftime('%Y/%-m/%-d')

            is_bull = bool(df['Bullish_Phase'].iloc[i])
            is_bear = bool(df['Bearish_Phase'].iloc[i])

            current_status = "Green" if is_bull else ("Red" if is_bear else "Neutral")

            if i > 0:
                prev_bull = bool(df['Bullish_Phase'].iloc[i-1])
                prev_bear = bool(df['Bearish_Phase'].iloc[i-1])
                prev_status = "Green" if prev_bull else ("Red" if prev_bear else "Neutral")
            else:
                prev_status = "Neutral"

            status_text = ""
            status_color = "Green" # Default?

            # Replicate legacy text logic
            if current_status == "Green":
                status_color = "Green"
                if prev_status == "Red":
                    status_text = "Red to Green"
                elif prev_status == "Green":
                    status_text = "still Green"
                else:
                    status_text = "Start Green"
            elif current_status == "Red":
                status_color = "Red"
                if prev_status == "Green":
                    status_text = "Green to Red"
                elif prev_status == "Red":
                    status_text = "still Red"
                else:
                    status_text = "Start Red"
            else:
                status_text = "Neutral"
                status_color = "Gray"

            results.append({
                "date_key": date_key,
                "date": date_str,
                "open": float(df['Open'].iloc[i]),
                "high": float(df['High'].iloc[i]),
                "low": float(df['Low'].iloc[i]),
                "close": float(df['Close'].iloc[i]),
                "tsv": float(df['TSV'].iloc[i]) if not pd.isna(df['TSV'].iloc[i]) else None,
                "fast_k": float(df['Fast_K'].iloc[i]) if not pd.isna(df['Fast_K'].iloc[i]) else None,
                "slow_d": float(df['Slow_D'].iloc[i]) if not pd.isna(df['Slow_D'].iloc[i]) else None,
                "market_status": status_color,
                "status_text": status_text,
                "new_lows_ratio": float(df['New_Lows_Ratio'].iloc[i]) if 'New_Lows_Ratio' in df.columns and not pd.isna(df['New_Lows_Ratio'].iloc[i]) else 0.0,
                "climax_entry": bool(df['Climax_Entry'].iloc[i]) if 'Climax_Entry' in df.columns else False,
                # Add Divergence Data
                "bull_div_prev": int(df['Bullish_Divergence'].iloc[i]) if pd.notna(df['Bullish_Divergence'].iloc[i]) else None,
                "bear_div_prev": int(df['Bearish_Divergence'].iloc[i]) if pd.notna(df['Bearish_Divergence'].iloc[i]) else None
            })

        return results, df

    except Exception as e:
        logger.error(f"Error in get_market_analysis_data: {e}")
        return [], pd.DataFrame()
