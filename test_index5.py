import pandas as pd
from src.backtest_engine import BacktestEngine

engine = BacktestEngine(
    market_data_path="data/market/market_data.csv",
    universe_dir="data/universe/",
    fundamentals_path="data/fundamentals/fundamentals.csv",
    start_date="2007-01-01",
    end_date="2026-03-01"
)

print(engine.start_date)
print(engine.end_date)
print(type(engine.start_date))

if not engine.market_features.empty:
    print("Market features dates:", engine.market_features.index)
    cal = engine.market_features.index
    cal = cal[(cal >= engine.start_date) & (cal <= engine.end_date)]
    print("Filtered cal:", cal)
