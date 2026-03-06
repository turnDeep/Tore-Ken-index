# Market Regime & Stock Selection Backtest

This project implements a stock trading strategy and backtesting engine based on the following concepts:
1. Macro-level market regime detection (VIX, Interest Rates, HY Spread, Breadth).
2. Fundamental quality-based stock selection (FCF Margin, OP Margin, Interest Coverage, etc.).
3. Post-crisis reentry rules.

## Data Sources
- `yfinance` for market indices (^VIX, SPY, QQQ, IWM), individual stock daily OHLCV, and company financials.
- `FRED` (Federal Reserve Economic Data) for DGS10 (10-Year Treasury Yield) and BAMLH0A0HYM2 (US High Yield Index Option-Adjusted Spread).
