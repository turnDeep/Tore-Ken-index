import pandas as pd
from src.feature_engineering import get_market_features
from src.regime_model import calc_fragility_score

market_df = pd.read_csv("data/market/market_data.csv", index_col=0, parse_dates=True)
print(market_df.head())
print("Columns:", market_df.columns)

mf = get_market_features("data/market/market_data.csv", "data/universe/")
print("MF shape:", mf.shape)
print("MF columns:", mf.columns)
print("MF head:\n", mf.head())

mf = calc_fragility_score(mf)
print("Frag score in mf:", 'fragility_score' in mf.columns)
print("MF index:", mf.index)
