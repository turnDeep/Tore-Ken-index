import yfinance as yf
import pandas as pd
try:
    import pandas_datareader.data as web
except ImportError:
    pass
import datetime
import os

def download_market_data(start_date="2007-01-01", end_date=None, save_dir="data/market/"):
    """
    Download SPY, QQQ, IWM, VIX from yfinance.
    Download DGS10, BAMLH0A0HYM2 from FRED.
    Combine and forward fill missing data.
    """
    if end_date is None:
        end_date = datetime.date.today().strftime("%Y-%m-%d")

    os.makedirs(save_dir, exist_ok=True)

    # 1. Download YF Data
    tickers = ["SPY", "QQQ", "IWM", "^VIX"]
    print(f"Downloading YF market data for {tickers} from {start_date} to {end_date}...")
    try:
        yf_data = yf.download(tickers, start=start_date, end=end_date, progress=False)["Adj Close"]
    except Exception as e:
        print(f"Error downloading YF market data: {e}")
        yf_data = pd.DataFrame()

    # 2. Download FRED Data
    print("Downloading FRED data (DGS10, BAMLH0A0HYM2)...")
    try:
        fred_data = web.DataReader(["DGS10", "BAMLH0A0HYM2"], "fred", start_date, end_date)
    except Exception as e:
        print(f"Error downloading FRED data via pandas_datareader: {e}")
        fred_data = pd.DataFrame()

    # Combine
    if not yf_data.empty and not fred_data.empty:
        df = pd.concat([yf_data, fred_data], axis=1)
    elif not yf_data.empty:
        df = yf_data
    elif not fred_data.empty:
        df = fred_data
    else:
        df = pd.DataFrame()

    if not df.empty:
        # Forward fill (business days and FRED might have different holidays or missing dates)
        df.ffill(inplace=True)
        # Drop rows where all are NaN (usually weekends if merged poorly)
        df.dropna(how='all', inplace=True)

        save_path = os.path.join(save_dir, "market_data.csv")
        df.to_csv(save_path)
        print(f"Market data saved to {save_path}")
        return df
    else:
        print("No market data downloaded.")
        return None

def get_sp500_tickers():
    """
    A utility to get a rough proxy of S&P 500 or top 500 liquid stocks.
    For simplicity, we'll return a static list or fetch from wikipedia.
    """
    try:
        table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        df = table[0]
        tickers = df['Symbol'].tolist()
        # Clean some symbols (e.g. BRK.B -> BRK-B)
        tickers = [t.replace('.', '-') for t in tickers]
        return tickers
    except Exception as e:
        print(f"Error fetching SP500 tickers: {e}")
        # fallback
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "JPM", "JNJ", "V"]

def download_universe_data(tickers, start_date="2007-01-01", end_date=None, save_dir="data/universe/"):
    """
    Download OHLCV for all tickers.
    """
    if end_date is None:
        end_date = datetime.date.today().strftime("%Y-%m-%d")
    os.makedirs(save_dir, exist_ok=True)

    print(f"Downloading daily data for {len(tickers)} tickers...")
    try:
        data = yf.download(tickers, start=start_date, end=end_date, group_by="ticker", progress=True)
    except Exception as e:
        print(f"Error downloading universe data: {e}")
        return None

    # Save each ticker to a separate CSV or a combined HDF5/Parquet. We'll use individual CSVs for simplicity.
    if isinstance(data.columns, pd.MultiIndex):
        for ticker in tickers:
            if ticker in data.columns.levels[0]:
                df = data[ticker].dropna(how="all")
                if not df.empty:
                    df.to_csv(os.path.join(save_dir, f"{ticker}.csv"))
    elif len(tickers) == 1:
        # single ticker case
        data.dropna(how="all").to_csv(os.path.join(save_dir, f"{tickers[0]}.csv"))

    print(f"Universe data saved to {save_dir}")
    return data

def download_fundamentals(tickers, save_dir="data/fundamentals/"):
    """
    Download key financial metrics.
    Note: yfinance returns the most recent 4 periods usually.
    For a proper backtest going back to 2007, you need point-in-time fundamentals,
    which yfinance does NOT provide fully (only trailing).
    We will save what's available for demonstration.
    """
    os.makedirs(save_dir, exist_ok=True)
    print(f"Downloading fundamentals for {len(tickers)} tickers...")

    results = []

    for t in tickers:
        try:
            stock = yf.Ticker(t)
            info = stock.info

            # Get TTM data if available, or most recent annual
            # Since yfinance doesn't give deep historical point-in-time, we approximate
            # for the "current" simulation step.

            row = {
                "Ticker": t,
                "FreeCashflow": info.get("freeCashflow", None),
                "TotalDebt": info.get("totalDebt", None),
                "TotalCash": info.get("totalCash", None),
                "MarketCap": info.get("marketCap", None),
                "Revenue": info.get("totalRevenue", None),
                "OperatingMargins": info.get("operatingMargins", None),
                "EBITDA": info.get("ebitda", None),
            }
            results.append(row)
        except Exception as e:
            pass

    df = pd.DataFrame(results)
    save_path = os.path.join(save_dir, "fundamentals.csv")
    df.to_csv(save_path, index=False)
    print(f"Fundamentals saved to {save_path}")
    return df

if __name__ == "__main__":
    download_market_data(save_dir="../data/market/")
    tickers = get_sp500_tickers()[:10]  # Just 10 for quick test
    download_universe_data(tickers, save_dir="../data/universe/")
    download_fundamentals(tickers, save_dir="../data/fundamentals/")
