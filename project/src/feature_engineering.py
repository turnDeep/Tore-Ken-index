import pandas as pd
import numpy as np
import os
import glob

def calculate_zscore(series, window):
    rolling_mean = series.rolling(window=window, min_periods=1).mean()
    rolling_std = series.rolling(window=window, min_periods=1).std()
    # Avoid division by zero
    rolling_std = rolling_std.replace(0, np.nan)
    return (series - rolling_mean) / rolling_std

def build_vix_features(df_vix):
    """
    vix_level = VIX
    vix_z20 = (VIX - rolling_mean_20) / rolling_std_20
    vix_change_5 = VIX / VIX.shift(5) - 1
    """
    vix_features = pd.DataFrame(index=df_vix.index)
    vix_features['vix_level'] = df_vix
    vix_features['vix_z20'] = calculate_zscore(df_vix, 20)
    vix_features['vix_change_5'] = df_vix / df_vix.shift(5) - 1
    return vix_features

def build_rate_features(df_rate):
    """
    rate_10y = DGS10
    rate_change_5 = DGS10.diff(5)
    rate_z60 = zscore(DGS10.diff(1), 60)
    """
    rate_features = pd.DataFrame(index=df_rate.index)
    rate_features['rate_10y'] = df_rate
    rate_features['rate_change_5'] = df_rate.diff(5)
    rate_features['rate_z60'] = calculate_zscore(df_rate.diff(1), 60)
    return rate_features

def build_hy_features(df_hy):
    """
    hy_oas = BAMLH0A0HYM2
    hy_change_5 = hy_oas.diff(5)
    hy_z60 = zscore(hy_oas, 60)
    """
    hy_features = pd.DataFrame(index=df_hy.index)
    hy_features['hy_oas'] = df_hy
    hy_features['hy_change_5'] = df_hy.diff(5)
    hy_features['hy_z60'] = calculate_zscore(df_hy, 60)
    return hy_features

def build_breadth_features(universe_dir, spy_series):
    """
    pct_above_20dma: 20日移動平均上にある銘柄比率
    pct_above_50dma: 50日移動平均上にある銘柄比率
    pct_above_200dma: 200日移動平均上にある銘柄比率
    adv_dec_ratio: 当日上昇銘柄数 / 下落銘柄数
    new_high_ratio: 20日高値更新銘柄比率
    equal_weight_return: ユニバース等金額日次リターン
    cap_weight_return_proxy: SPY日次リターン
    breadth_spread = equal_weight_return - cap_weight_return_proxy
    """
    csv_files = glob.glob(os.path.join(universe_dir, "*.csv"))

    # Store daily indicators for each ticker
    all_prices = {}
    for f in csv_files:
        ticker = os.path.basename(f).replace('.csv', '')
        try:
            df = pd.read_csv(f, index_col=0, parse_dates=True)
            # Handle potential multi-level index issues if saved differently
            if 'Adj Close' in df.columns:
                close = df['Adj Close']
            elif 'Close' in df.columns:
                close = df['Close']
            else:
                continue
            all_prices[ticker] = close
        except Exception:
            continue

    if not all_prices:
        return pd.DataFrame()

    price_df = pd.DataFrame(all_prices)

    # Pre-calculate MAs, returns, new highs
    ma20 = price_df.rolling(20).mean()
    ma50 = price_df.rolling(50).mean()
    ma200 = price_df.rolling(200).mean()
    returns = price_df.pct_change()
    high20 = price_df.rolling(20).max()

    # Daily logic
    # Align dates
    valid_counts = price_df.notna().sum(axis=1)

    pct_above_20dma = (price_df > ma20).sum(axis=1) / valid_counts
    pct_above_50dma = (price_df > ma50).sum(axis=1) / valid_counts
    pct_above_200dma = (price_df > ma200).sum(axis=1) / valid_counts

    advancing = (returns > 0).sum(axis=1)
    declining = (returns < 0).sum(axis=1)

    # handle division by zero
    adv_dec_ratio = advancing / declining.replace(0, 1)

    new_high_ratio = (price_df >= high20).sum(axis=1) / valid_counts

    equal_weight_return = returns.mean(axis=1)

    # align SPY
    spy_ret = spy_series.pct_change()

    breadth_df = pd.DataFrame({
        'pct_above_20dma': pct_above_20dma,
        'pct_above_50dma': pct_above_50dma,
        'pct_above_200dma': pct_above_200dma,
        'adv_dec_ratio': adv_dec_ratio,
        'new_high_ratio': new_high_ratio,
        'equal_weight_return': equal_weight_return
    })

    # Merge with SPY return to calc spread
    breadth_df = breadth_df.join(spy_ret.rename('spy_return'))
    breadth_df['breadth_spread'] = breadth_df['equal_weight_return'] - breadth_df['spy_return']

    return breadth_df

def get_market_features(market_data_path, universe_dir=None):
    """
    Combines VIX, Rate, HY, and Breadth features.
    """
    if not os.path.exists(market_data_path):
        return pd.DataFrame()

    market_df = pd.read_csv(market_data_path, index_col=0, parse_dates=True)

    # Make sure columns exist
    if '^VIX' in market_df.columns:
        vix_f = build_vix_features(market_df['^VIX'])
    else:
        vix_f = pd.DataFrame()

    if 'DGS10' in market_df.columns:
        rate_f = build_rate_features(market_df['DGS10'])
    else:
        rate_f = pd.DataFrame()

    if 'BAMLH0A0HYM2' in market_df.columns:
        hy_f = build_hy_features(market_df['BAMLH0A0HYM2'])
    else:
        hy_f = pd.DataFrame()

    features = pd.concat([vix_f, rate_f, hy_f], axis=1)

    if universe_dir and os.path.exists(universe_dir) and 'SPY' in market_df.columns:
        breadth_f = build_breadth_features(universe_dir, market_df['SPY'])
        features = features.join(breadth_f)

    return features

if __name__ == "__main__":
    # Test
    # features = get_market_features("data/market/market_data.csv", "data/universe/")
    # print(features.tail())
    pass
