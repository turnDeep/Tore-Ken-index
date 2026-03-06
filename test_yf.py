import yfinance as yf
tickers = ["SPY", "QQQ", "IWM", "^VIX"]
try:
    data = yf.download(tickers, start="2007-01-01", end="2026-03-06", progress=False)
    print(data.columns)
except Exception as e:
    print("Error:", e)
