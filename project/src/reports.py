import pandas as pd
import numpy as np

def calculate_metrics(history_df, trades_df=None):
    """
    Computes CAGR, Volatility, Sharpe, Max Drawdown, Monthly Win Rate,
    Average Holding Period, Crisis Period Return, and Total Trades.
    history_df: DataFrame with daily 'Equity', 'Regime', etc.
    trades_df: DataFrame with 'Date', 'Ticker', 'Type'.
    """
    if history_df.empty or len(history_df) < 2:
        return {}

    equity_series = history_df['Equity']
    daily_returns = equity_series.pct_change().dropna()

    # CAGR
    total_return = equity_series.iloc[-1] / equity_series.iloc[0]
    years = (equity_series.index[-1] - equity_series.index[0]).days / 365.25
    cagr = (total_return ** (1 / years)) - 1 if years > 0 else 0.0

    # Volatility (Annualized)
    annual_vol = daily_returns.std() * np.sqrt(252)

    # Sharpe Ratio
    sharpe = (cagr) / annual_vol if annual_vol > 0 else 0.0

    # Sortino Ratio
    downside_returns = daily_returns[daily_returns < 0]
    downside_vol = downside_returns.std() * np.sqrt(252) if not downside_returns.empty else 0.0
    sortino = (cagr) / downside_vol if downside_vol > 0 else 0.0

    # Max Drawdown
    cumulative_max = equity_series.cummax()
    drawdown = (equity_series - cumulative_max) / cumulative_max
    max_dd = drawdown.min()

    # Calmar Ratio
    calmar = cagr / abs(max_dd) if max_dd < 0 else 0.0

    # Win Rate (Days up vs Days down)
    daily_win_rate = len(daily_returns[daily_returns > 0]) / len(daily_returns) if len(daily_returns) > 0 else 0.0

    # Monthly Win Rate
    monthly_returns = equity_series.resample('M').last().pct_change().dropna()
    monthly_win_rate = len(monthly_returns[monthly_returns > 0]) / len(monthly_returns) if len(monthly_returns) > 0 else 0.0

    # Crisis Period Return
    crisis_periods = history_df[history_df['Regime'] == 'Crisis']
    if not crisis_periods.empty:
        # Sum of daily returns during crisis days as a proxy
        crisis_return = crisis_periods['Equity'].pct_change().sum()
    else:
        crisis_return = 0.0

    # Trades Metrics
    total_trades = 0
    avg_holding_days = 0.0

    if trades_df is not None and not trades_df.empty:
        total_trades = len(trades_df)

        # Calculate Average Holding Period
        buys = trades_df[trades_df['Type'] == 'BUY']
        sells = trades_df[trades_df['Type'] == 'SELL']

        holding_times = []
        for ticker in buys['Ticker'].unique():
            t_buys = buys[buys['Ticker'] == ticker].sort_values('Date')
            t_sells = sells[sells['Ticker'] == ticker].sort_values('Date')

            # Simple matching: pop first sell for first buy
            for b_idx, buy_row in t_buys.iterrows():
                b_date = buy_row['Date']
                # find first sell after this buy
                valid_sells = t_sells[t_sells['Date'] > b_date]
                if not valid_sells.empty:
                    s_date = valid_sells.iloc[0]['Date']
                    holding_times.append((pd.to_datetime(s_date) - pd.to_datetime(b_date)).days)
                    # Remove used sell to avoid double counting (in a crude way)
                    t_sells = t_sells.drop(valid_sells.index[0])

        if holding_times:
            avg_holding_days = sum(holding_times) / len(holding_times)

    return {
        "CAGR": cagr,
        "Annual_Volatility": annual_vol,
        "Sharpe_Ratio": sharpe,
        "Sortino_Ratio": sortino,
        "Max_Drawdown": max_dd,
        "Calmar_Ratio": calmar,
        "Daily_Win_Rate": daily_win_rate,
        "Monthly_Win_Rate": monthly_win_rate,
        "Crisis_Period_Return_Proxy": crisis_return,
        "Total_Trades": total_trades,
        "Avg_Holding_Days": avg_holding_days,
        "Total_Return": total_return - 1
    }

def regime_performance(history_df):
    """
    Calculates average daily return under each market regime.
    """
    if history_df.empty or 'Regime' not in history_df.columns:
        return pd.Series()

    history_df['Daily_Return'] = history_df['Equity'].pct_change()
    regime_stats = history_df.groupby('Regime')['Daily_Return'].mean() * 252 # Annualized
    return regime_stats

def generate_report(history_df, trades_df=None, output_path=None):
    if history_df is None or history_df.empty:
        print("No history data to report.")
        return

    metrics = calculate_metrics(history_df, trades_df)
    regimes = regime_performance(history_df)

    print("\n--- Backtest Performance Report ---")
    for k, v in metrics.items():
        if "Ratio" in k or k == "Total_Return" or "Trades" in k or "Days" in k:
            if "Trades" in k or "Days" in k:
                print(f"{k}: {v:.1f}")
            else:
                print(f"{k}: {v:.4f}")
        else:
            print(f"{k}: {v:.2%}")

    print("\n--- Regime Annualized Returns ---")
    for regime, ret in regimes.items():
        print(f"{regime}: {ret:.2%}")

    if output_path:
        with open(output_path, "w") as f:
            f.write("Backtest Performance Report\n")
            for k, v in metrics.items():
                f.write(f"{k}: {v}\n")
            f.write("\nRegime Annualized Returns\n")
            for regime, ret in regimes.items():
                f.write(f"{regime}: {ret}\n")
        print(f"Report saved to {output_path}")

    return metrics, regimes

if __name__ == "__main__":
    pass
