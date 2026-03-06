import pandas as pd
import numpy as np

def scale_0_to_100(series, window=252):
    """
    Min-Max scale a pandas Series to 0-100 using a rolling window to avoid lookahead bias.
    """
    rolling_min = series.rolling(window=window, min_periods=1).min()
    rolling_max = series.rolling(window=window, min_periods=1).max()

    # Avoid division by zero
    diff = rolling_max - rolling_min
    diff = diff.replace(0, np.nan)

    scaled = (series - rolling_min) / diff * 100
    return scaled.fillna(50) # default to 50 if diff is 0 or NaN

def calc_fragility_score(features_df, window=252):
    """
    Fragility Score combines:
    Volatility Stress:
      - VIX水準 (vix_level)
      - VIX急騰率 (vix_change_5)
    Rate Stress:
      - 10年金利5日変化 (rate_change_5)
      - 金利変化のz-score (rate_z60)
    Credit Stress:
      - HY OAS水準 (hy_oas)
      - HY OASの5日拡大幅 (hy_change_5)
    Breadth Stress:
      - 50DMA上銘柄比率 (pct_above_50dma)
      - 200DMA上銘柄比率 (pct_above_200dma)
      - equal-weight vs cap-weight差 (breadth_spread)

    fragility_score =
      0.30 * vol_stress +
      0.20 * rate_stress +
      0.25 * credit_stress +
      0.25 * breadth_stress
    """
    df = features_df.copy()

    # Fill missing columns if they don't exist
    expected_cols = ['vix_level', 'vix_change_5', 'rate_change_5', 'rate_z60', 'hy_oas', 'hy_change_5', 'pct_above_50dma', 'pct_above_200dma', 'breadth_spread']
    for col in expected_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Volatility Stress
    vix_lvl_scaled = scale_0_to_100(df['vix_level'], window)
    vix_chg_scaled = scale_0_to_100(df['vix_change_5'], window)
    vol_stress = (vix_lvl_scaled + vix_chg_scaled) / 2

    # Rate Stress
    rate_chg_scaled = scale_0_to_100(df['rate_change_5'], window)
    rate_z_scaled = scale_0_to_100(df['rate_z60'], window)
    rate_stress = (rate_chg_scaled + rate_z_scaled) / 2

    # Credit Stress
    hy_oas_scaled = scale_0_to_100(df['hy_oas'], window)
    hy_chg_scaled = scale_0_to_100(df['hy_change_5'], window)
    credit_stress = (hy_oas_scaled + hy_chg_scaled) / 2

    # Breadth Stress (lower breadth -> higher stress)
    pct_50dma_scaled = 100 - scale_0_to_100(df['pct_above_50dma'], window)
    pct_200dma_scaled = 100 - scale_0_to_100(df['pct_above_200dma'], window)
    breadth_sprd_scaled = 100 - scale_0_to_100(df['breadth_spread'], window)
    breadth_stress = (pct_50dma_scaled + pct_200dma_scaled + breadth_sprd_scaled) / 3

    # Handle missing inputs by keeping neutral 50
    vol_stress = vol_stress.fillna(50)
    rate_stress = rate_stress.fillna(50)
    credit_stress = credit_stress.fillna(50)
    breadth_stress = breadth_stress.fillna(50)

    fragility_score = (
        0.30 * vol_stress +
        0.20 * rate_stress +
        0.25 * credit_stress +
        0.25 * breadth_stress
    )

    df['fragility_score'] = fragility_score
    return df

def classify_regime(fragility_score):
    """
    0 - 30 = Risk-On
    30 - 55 = Neutral
    55 - 75 = Risk-Off
    75 - 100 = Crisis
    """
    if pd.isna(fragility_score):
        return 'Neutral' # default

    if fragility_score < 30:
        return 'Risk-On'
    elif fragility_score < 55:
        return 'Neutral'
    elif fragility_score < 75:
        return 'Risk-Off'
    else:
        return 'Crisis'

def add_regime_classification(df):
    """
    Adds a 'regime' column based on 'fragility_score'
    """
    if 'fragility_score' in df.columns:
        df['regime'] = df['fragility_score'].apply(classify_regime)
    return df

if __name__ == "__main__":
    pass
