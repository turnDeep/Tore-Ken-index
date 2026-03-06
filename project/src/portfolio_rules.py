import pandas as pd
import numpy as np

def regime_to_exposure(regime):
    if regime == 'Risk-On':
        return 0.90
    elif regime == 'Neutral':
        return 0.60
    elif regime == 'Risk-Off':
        return 0.30
    elif regime == 'Crisis':
        return 0.10
    return 0.0

def regime_to_level(regime):
    """Convert regime to integer level for comparison (higher = worse)"""
    mapping = {'Risk-On': 0, 'Neutral': 1, 'Risk-Off': 2, 'Crisis': 3}
    return mapping.get(regime, 1)

def select_candidates(date, regime, scores_df, precalc_features, max_names=25):
    """
    Selects top candidates for a given regime using precalculated features to avoid bottlenecks.
    """
    candidates = []

    for t in scores_df['Ticker']:
        if t not in precalc_features:
            continue

        feat_df = precalc_features[t]

        if date not in feat_df.index:
            continue

        row = feat_df.loc[date]
        current_price = row.get('close', np.nan)
        ma50 = row.get('ma50', np.nan)
        ma200 = row.get('ma200', np.nan)
        rs_126d = row.get('rs_126d', np.nan)

        if pd.isna(current_price) or pd.isna(ma50) or pd.isna(ma200) or pd.isna(rs_126d):
            continue

        q_score = scores_df[scores_df['Ticker'] == t]['quality_score'].values
        if len(q_score) == 0:
            continue
        q_score = q_score[0]

        candidates.append({
            'Ticker': t,
            'quality_score': q_score,
            'price': current_price,
            'ma50': ma50,
            'ma200': ma200,
            'rs_126d': rs_126d
        })

    df = pd.DataFrame(candidates)
    if df.empty:
        return []

    # Rank RS
    df['rs_rank'] = df['rs_126d'].rank(pct=True)

    # 総合順位: entry_score = 0.45 * quality_score + 0.35 * rs_rank + 0.20 * liq_rank
    df['entry_score'] = 0.45 * df['quality_score'] + 0.55 * (df['rs_rank']*100)

    # Base filters that apply to ALL entries unless overridden by regime (Sec 7)
    # quality_score >= 70, close > ma50, ma50 > ma200, rs_126d >= top 30%
    base_mask = (
        (df['quality_score'] >= 70) &
        (df['price'] > df['ma50']) &
        (df['ma50'] > df['ma200']) &
        (df['rs_rank'] >= 0.70)
    )

    # Regime specific logic (Sec 6.2) combines with base rules where appropriate
    if regime == 'Risk-On':
        # Risk-On allows lower quality (top 70% -> score >= 30)
        mask = (df['quality_score'] >= 30) & (df['price'] > df['ma50']) & (df['ma50'] > df['ma200']) & (df['rs_rank'] >= 0.70)
        filtered = df[mask].copy()
    elif regime == 'Neutral':
        # Neutral allows top 50% quality, must be above 50DMA, below 200DMA excluded
        mask = (df['quality_score'] >= 50) & (df['price'] > df['ma50']) & (df['price'] > df['ma200'])
        filtered = df[mask].copy()
    elif regime == 'Risk-Off':
        # Risk-Off requires top 30% quality (score >= 70), above 200DMA
        mask = (df['quality_score'] >= 70) & (df['price'] > df['ma200'])
        filtered = df[mask].copy()
    elif regime == 'Crisis':
        return []

    if filtered.empty:
        return []

    filtered = filtered.sort_values(by='entry_score', ascending=False)

    return filtered.head(max_names)['Ticker'].tolist()

def apply_risk_rules(current_holdings, precalc_features, date, stop_loss=-0.08):
    """
    Applies daily stop loss, +20% half-profit taking, and 20DMA trailing stop.
    Returns:
      kept_holdings: list of holdings to keep (can have modified shares)
      sell_orders: list of dicts for what needs to be sold: {'Ticker': str, 'shares': float, 'reason': str}
    """
    to_keep = []
    sell_orders = []

    for holding in current_holdings:
        ticker = holding['Ticker']
        entry_price = holding['entry_price']
        shares = holding['shares']
        half_sold = holding.get('half_sold', False)

        if ticker not in precalc_features:
            to_keep.append(holding) # keep if missing data
            continue

        feat_df = precalc_features[ticker]
        if date not in feat_df.index:
            to_keep.append(holding) # keep if missing data
            continue

        row = feat_df.loc[date]
        current_price = row.get('close', np.nan)
        ma50 = row.get('ma50', np.nan)
        ma20 = row.get('ma20', np.nan) # Needed for trailing stop

        if pd.isna(current_price):
            to_keep.append(holding) # keep if no current price
            continue

        # 1. Stop loss (-8%)
        if current_price < entry_price * (1 + stop_loss):
            sell_orders.append({'Ticker': ticker, 'shares': shares, 'reason': 'StopLoss'})
            continue # Full sell

        # 2. 50DMA cut (Explicit drop)
        if not pd.isna(ma50) and current_price < ma50:
            sell_orders.append({'Ticker': ticker, 'shares': shares, 'reason': '50DMACut'})
            continue # Full sell

        # 3. Take half profit at +20%
        if not half_sold and current_price >= entry_price * 1.20:
            sell_shares = shares * 0.5
            sell_orders.append({'Ticker': ticker, 'shares': sell_shares, 'reason': 'TakeHalfProfit'})

            # Update holding
            holding['shares'] -= sell_shares
            holding['half_sold'] = True

        # 4. If half sold, trail remainder with 20DMA
        if holding.get('half_sold', False) and not pd.isna(ma20) and current_price < ma20:
            sell_orders.append({'Ticker': ticker, 'shares': holding['shares'], 'reason': '20DMATrailingStop'})
            continue # Sell remainder

        to_keep.append(holding)

    return to_keep, sell_orders

if __name__ == "__main__":
    pass
