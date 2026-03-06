import os
import argparse
from src.download_data import download_market_data, get_sp500_tickers, download_universe_data, download_fundamentals
from src.backtest_engine import BacktestEngine
from src.reports import generate_report
import traceback

def main():
    parser = argparse.ArgumentParser(description="Run Market Regime & Stock Selection Backtest")
    parser.add_argument("--download", action="store_true", help="Download all necessary data before running backtest")
    parser.add_argument("--start", type=str, default="2007-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--capital", type=float, default=100000.0, help="Initial capital")
    args = parser.parse_args()

    # 1. Download Data
    if args.download:
        print("Starting data download...")
        try:
            download_market_data(start_date=args.start, end_date=args.end, save_dir="data/market/")

            # Here we might limit the tickers for faster testing
            tickers = get_sp500_tickers()
            print(f"Fetched {len(tickers)} tickers from S&P500 list.")

            download_universe_data(tickers, start_date=args.start, end_date=args.end, save_dir="data/universe/")
            download_fundamentals(tickers, save_dir="data/fundamentals/")
        except Exception as e:
            print(f"Error downloading data: {e}")
            traceback.print_exc()

    # 2. Run Backtest
    print("Starting backtest...")
    engine = BacktestEngine(
        market_data_path="data/market/market_data.csv",
        universe_dir="data/universe/",
        fundamentals_path="data/fundamentals/fundamentals.csv",
        start_date=args.start,
        end_date=args.end,
        initial_capital=args.capital
    )

    history = engine.run()
    trades_df = engine.trades_df if hasattr(engine, 'trades_df') else None

    # 3. Generate Report
    if history is not None and not history.empty:
        generate_report(history, trades_df=trades_df, output_path="backtest_report.txt")
        history.to_csv("backtest_history.csv")
        if trades_df is not None:
            trades_df.to_csv("backtest_trades.csv")
        print("Backtest finished. Check backtest_report.txt, backtest_history.csv, and backtest_trades.csv for results.")
    else:
        print("Backtest failed or no history generated.")

if __name__ == "__main__":
    main()
