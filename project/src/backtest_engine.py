import pandas as pd
import numpy as np
from src.feature_engineering import get_market_features
from src.regime_model import calc_fragility_score, classify_regime
from src.portfolio_rules import regime_to_exposure, select_candidates, apply_risk_rules, regime_to_level
from src.stock_scoring import load_fundamentals, build_quality_scores
import os
import glob
from tqdm import tqdm

class BacktestEngine:
    def __init__(self, market_data_path, universe_dir, fundamentals_path, start_date="2007-01-01", end_date="2026-03-01", initial_capital=100000.0):
        self.market_data_path = market_data_path
        self.universe_dir = universe_dir
        self.fundamentals_path = fundamentals_path
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date) if end_date else pd.to_datetime('today')
        self.initial_capital = initial_capital

        self.portfolio = [] # List of dicts: {'Ticker': str, 'entry_price': float, 'shares': float, 'half_sold': bool}
        self.cash = initial_capital
        self.history = [] # Logs daily equity, regime, etc.
        self.trades = [] # Log trades for reporting

        self._load_data()

    def _precalc_features(self):
        """
        Precalculate moving averages and relative strength to avoid daily recalculation.
        """
        print("Precalculating individual stock features...")
        self.precalc = {}
        csv_files = glob.glob(os.path.join(self.universe_dir, "*.csv"))
        for f in csv_files:
            ticker = os.path.basename(f).replace('.csv', '')
            try:
                df = pd.read_csv(f, index_col=0, parse_dates=True)
                if 'Adj Close' in df.columns:
                    close = df['Adj Close']
                elif 'Close' in df.columns:
                    close = df['Close']
                else:
                    continue

                feat_df = pd.DataFrame(index=close.index)
                feat_df['close'] = close
                feat_df['ma20'] = close.rolling(20).mean()
                feat_df['ma50'] = close.rolling(50).mean()
                feat_df['ma200'] = close.rolling(200).mean()
                feat_df['rs_126d'] = close / close.shift(126) - 1
                self.precalc[ticker] = feat_df
            except Exception:
                pass

    def _load_data(self):
        print("Loading market features...")
        self.market_features = get_market_features(self.market_data_path, self.universe_dir)
        if not self.market_features.empty:
            self.market_features = calc_fragility_score(self.market_features)

        print("Loading fundamentals...")
        fund_df = load_fundamentals(self.fundamentals_path)
        self.quality_scores = build_quality_scores(fund_df)

        self._precalc_features()

        if not self.market_features.empty:
            self.calendar = self.market_features.index
        else:
            self.calendar = pd.DatetimeIndex([])

        self.calendar = self.calendar[(self.calendar >= self.start_date) & (self.calendar <= self.end_date)]

    def _check_crisis_reentry(self, current_date):
        idx = self.market_features.index.get_loc(current_date)
        if idx < 5:
            return False

        mf = self.market_features
        recent = mf.iloc[idx-5:idx+1]

        conditions_met = 0

        # 1. Fragility score fell 5 consecutive days
        frag_scores = recent['fragility_score'].values
        if all(frag_scores[i] < frag_scores[i-1] for i in range(1, 6)):
            conditions_met += 1

        # 2. VIX drops >= 10% from 20d high
        if idx >= 20 and 'vix_level' in mf.columns:
            vix_20d_high = mf['vix_level'].iloc[idx-20:idx+1].max()
            current_vix = mf['vix_level'].iloc[idx]
            if current_vix <= vix_20d_high * 0.90:
                conditions_met += 1

        # 3. HY OAS 5d change < 0
        if 'hy_change_5' in mf.columns and not pd.isna(mf['hy_change_5'].iloc[idx]):
            if mf['hy_change_5'].iloc[idx] < 0:
                conditions_met += 1

        # 4. pct_above_20dma WoW improvement
        if 'pct_above_20dma' in mf.columns:
            current_20dma = mf['pct_above_20dma'].iloc[idx]
            prev_20dma = mf['pct_above_20dma'].iloc[idx-5]
            if current_20dma > prev_20dma:
                conditions_met += 1

        # 5. adv_dec_ratio > 1.2
        if 'adv_dec_ratio' in mf.columns:
            if mf['adv_dec_ratio'].iloc[idx] > 1.2:
                conditions_met += 1

        return conditions_met >= 3

    def run(self):
        if self.calendar.empty:
            print("No valid calendar dates. Check data.")
            return

        print(f"Running backtest from {self.calendar[0].date()} to {self.calendar[-1].date()}...")

        last_rebalance_week = -1
        last_regime = None

        # Re-entry tracking
        reentry_active = False
        reentry_day_count = 0
        reentry_phase = 0
        reentry_candidates = []

        for i, date in enumerate(tqdm(self.calendar)):
            # 1. Update daily values
            holdings_value = 0.0
            current_prices = {}
            for t, feat_df in self.precalc.items():
                if date in feat_df.index:
                    row = feat_df.loc[date]
                    if not pd.isna(row.get('close')):
                        current_prices[t] = row['close']

            for holding in self.portfolio:
                if holding['Ticker'] in current_prices:
                    holdings_value += holding['shares'] * current_prices[holding['Ticker']]
                else:
                    holdings_value += holding['shares'] * holding['entry_price']

            total_equity = self.cash + holdings_value

            # 2. Daily Regime
            if date in self.market_features.index:
                frag_score = self.market_features.loc[date, 'fragility_score']
            else:
                frag_score = 50

            regime = classify_regime(frag_score)

            # Check for regime worsening
            # If regime worsens by 1 level, reduce all holdings by 20%
            if last_regime is not None:
                current_level = regime_to_level(regime)
                last_level = regime_to_level(last_regime)
                if current_level > last_level:
                    # Regime worsened
                    reduction_fraction = 0.20
                    # If worsened by 2 levels (e.g. Risk-On -> Risk-Off, or Neutral -> Crisis)
                    if current_level >= last_level + 2:
                        reduction_fraction = 0.50 # "弱い順に半分売却" logic requested (simplified to 50% across board)

                    for holding in self.portfolio:
                        t = holding['Ticker']
                        shares_to_sell = holding['shares'] * reduction_fraction
                        holding['shares'] -= shares_to_sell
                        if shares_to_sell > 0 and t in current_prices:
                            sell_price = current_prices[t]
                            sell_value = shares_to_sell * sell_price * (1 - 0.001)
                            self.cash += sell_value
                            self.trades.append({'Date': date, 'Type': 'SELL', 'Ticker': t, 'Price': sell_price, 'Reason': f'RegimeWorsened_{reduction_fraction*100}%'})

            last_regime = regime

            self.history.append({
                'Date': date,
                'Equity': total_equity,
                'Cash': self.cash,
                'HoldingsValue': holdings_value,
                'Regime': regime,
                'Fragility': frag_score
            })

            # 3. Daily Risk Control (Includes half profit and trailing 20DMA)
            kept_portfolio, sell_orders = apply_risk_rules(self.portfolio, self.precalc, date, stop_loss=-0.08)

            # Process returned sell orders
            for order in sell_orders:
                t = order['Ticker']
                shares = order['shares']
                reason = order['reason']
                if t in current_prices and shares > 0:
                    sell_price = current_prices[t]
                    sell_value = shares * sell_price * (1 - 0.001)
                    self.cash += sell_value
                    self.trades.append({'Date': date, 'Type': 'SELL', 'Ticker': t, 'Price': sell_price, 'Reason': reason})

            # kept_portfolio is already updated (minus full sells, and with partial reductions applied inline)
            self.portfolio = [p for p in kept_portfolio if p['shares'] > 1e-5] # clear dust

            # 4. Crisis Re-entry Logic
            if regime == 'Crisis':
                if not reentry_active and self._check_crisis_reentry(date):
                    reentry_active = True
                    reentry_day_count = 0
                    reentry_phase = 1
                    reentry_candidates = select_candidates(date, 'Risk-On', self.quality_scores, self.precalc, max_names=10)

                if reentry_active:
                    reentry_day_count += 1
                    invest_fraction = 0.0

                    if reentry_phase == 1 and reentry_day_count == 1:
                        invest_fraction = 0.30
                        reentry_phase = 2
                    elif reentry_phase == 2 and reentry_day_count == 3:
                        if self._check_crisis_reentry(date):
                            invest_fraction = 0.30
                            reentry_phase = 3
                        else:
                            reentry_active = False
                    elif reentry_phase == 3 and reentry_day_count == 7:
                        if self._check_crisis_reentry(date):
                            invest_fraction = 0.40
                            reentry_active = False
                        else:
                            reentry_active = False

                    if invest_fraction > 0 and reentry_candidates:
                        buy_power = total_equity * 0.90 * invest_fraction
                        alloc_per_stock = buy_power / len(reentry_candidates)
                        alloc_per_stock = min(alloc_per_stock, total_equity * 0.07)

                        for c in reentry_candidates:
                            if c in current_prices and self.cash >= alloc_per_stock:
                                price = current_prices[c]
                                shares = (alloc_per_stock * (1 - 0.001)) / price
                                self.cash -= alloc_per_stock
                                self.portfolio.append({
                                    'Ticker': c,
                                    'entry_price': price,
                                    'shares': shares,
                                    'half_sold': False
                                })
                                self.trades.append({'Date': date, 'Type': 'BUY', 'Ticker': c, 'Price': price, 'Reason': 'CrisisReentry'})
            else:
                reentry_active = False

                # 5. Weekly Rebalance (Standard)
                current_week = date.isocalendar().week
                if current_week != last_rebalance_week and i > 200:
                    last_rebalance_week = current_week

                    target_exposure = regime_to_exposure(regime)
                    current_exposure = sum([p['shares'] * current_prices.get(p['Ticker'], p['entry_price']) for p in self.portfolio]) / total_equity

                    if current_exposure < target_exposure - 0.05:
                        buy_power = (target_exposure - current_exposure) * total_equity
                        candidates = select_candidates(date, regime, self.quality_scores, self.precalc, max_names=10)

                        owned_tickers = [p['Ticker'] for p in self.portfolio]
                        candidates = [c for c in candidates if c not in owned_tickers]

                        if candidates:
                            alloc_per_stock = buy_power / len(candidates)
                            alloc_per_stock = min(alloc_per_stock, total_equity * 0.07)

                            for c in candidates:
                                if c in current_prices and self.cash >= alloc_per_stock:
                                    price = current_prices[c]
                                    shares = (alloc_per_stock * (1 - 0.001)) / price
                                    self.cash -= alloc_per_stock
                                    self.portfolio.append({
                                        'Ticker': c,
                                        'entry_price': price,
                                        'shares': shares,
                                        'half_sold': False
                                    })
                                    self.trades.append({'Date': date, 'Type': 'BUY', 'Ticker': c, 'Price': price, 'Reason': 'WeeklyRebalance'})

        self.trades_df = pd.DataFrame(self.trades)
        return pd.DataFrame(self.history).set_index('Date')

if __name__ == "__main__":
    pass
