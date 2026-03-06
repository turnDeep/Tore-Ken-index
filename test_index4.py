import pandas as pd
market_data_path="data/market/market_data.csv"
market_df = pd.read_csv(market_data_path, index_col=0, parse_dates=True)
print(market_df.index)
print(type(market_df.index))

from src.feature_engineering import get_market_features
from src.regime_model import calc_fragility_score

mf = get_market_features("data/market/market_data.csv", "data/universe/")
print("mf index", mf.index)
