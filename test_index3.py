import pandas as pd
from src.backtest_engine import BacktestEngine

engine = BacktestEngine(
    market_data_path="data/market/market_data.csv",
    universe_dir="data/universe/",
    fundamentals_path="data/fundamentals/fundamentals.csv",
    start_date="2007-01-01",
    end_date=None
)

print(engine.calendar)
