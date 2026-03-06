import pandas as pd
import numpy as np
import os

def load_fundamentals(filepath="data/fundamentals/fundamentals.csv"):
    if not os.path.exists(filepath):
        return pd.DataFrame()
    return pd.read_csv(filepath)

def build_quality_scores(fundamentals_df):
    """
    fcf_margin = FCF / Revenue
    de_ratio = TotalDebt / Equity (Here we approximate using what we have, e.g. TotalDebt/MarketCap or EBITDA proxy)
    net_cash_ratio = (Cash - TotalDebt) / MarketCap
    interest_coverage = EBIT / InterestExpense (We might only have EBITDA)
    op_margin = OperatingIncome / Revenue
    roic_proxy = EBIT / (Debt + Equity - Cash)

    quality_score =
        0.25 * rank(fcf_margin) +
        0.20 * rank(op_margin) +
        0.20 * rank(interest_coverage) +
        0.20 * rank(-de_ratio) +
        0.15 * rank(net_cash_ratio)
    """
    if fundamentals_df.empty:
        return pd.DataFrame()

    df = fundamentals_df.copy()

    # Fill missing values with neutral values or handle via rank ignoring NaNs
    # Note: For real implementation, you'd use exact columns pulled from yfinance financials

    # Try calculating FCF margin
    if 'FreeCashflow' in df.columns and 'Revenue' in df.columns:
        df['fcf_margin'] = df['FreeCashflow'] / df['Revenue'].replace(0, np.nan)
    else:
        df['fcf_margin'] = np.nan

    # OP margin
    if 'OperatingMargins' in df.columns:
        df['op_margin'] = df['OperatingMargins']
    else:
        df['op_margin'] = np.nan

    # Interest Coverage proxy (we might not have interest expense directly from info easily)
    # Just putting a placeholder column or ranking based on EBITDA
    if 'EBITDA' in df.columns:
        df['interest_coverage_proxy'] = df['EBITDA'] # the higher the better
    else:
        df['interest_coverage_proxy'] = np.nan

    # DE Ratio proxy
    if 'TotalDebt' in df.columns and 'MarketCap' in df.columns:
        df['de_ratio_proxy'] = df['TotalDebt'] / df['MarketCap'].replace(0, np.nan)
    else:
        df['de_ratio_proxy'] = np.nan

    # Net cash ratio
    if 'TotalCash' in df.columns and 'TotalDebt' in df.columns and 'MarketCap' in df.columns:
        df['net_cash_ratio'] = (df['TotalCash'] - df['TotalDebt']) / df['MarketCap'].replace(0, np.nan)
    else:
        df['net_cash_ratio'] = np.nan

    # Ranking functions (0 to 1 scale)
    def rank_col(series, ascending=True):
        return series.rank(pct=True, ascending=ascending)

    rank_fcf = rank_col(df['fcf_margin'])
    rank_op = rank_col(df['op_margin'])
    rank_ic = rank_col(df['interest_coverage_proxy'])
    rank_de = rank_col(df['de_ratio_proxy'], ascending=False) # Lower DE is better
    rank_nc = rank_col(df['net_cash_ratio'])

    # Fill NaNs in ranks with 0.5 (median) for robust scoring
    rank_fcf = rank_fcf.fillna(0.5)
    rank_op = rank_op.fillna(0.5)
    rank_ic = rank_ic.fillna(0.5)
    rank_de = rank_de.fillna(0.5)
    rank_nc = rank_nc.fillna(0.5)

    # quality_score (0 to 100)
    df['quality_score'] = (
        0.25 * rank_fcf +
        0.20 * rank_op +
        0.20 * rank_ic +
        0.20 * rank_de +
        0.15 * rank_nc
    ) * 100

    return df[['Ticker', 'quality_score', 'fcf_margin', 'op_margin', 'de_ratio_proxy', 'net_cash_ratio']]

if __name__ == "__main__":
    df = load_fundamentals("../data/fundamentals/fundamentals.csv")
    if not df.empty:
        scores = build_quality_scores(df)
        print(scores.head())
