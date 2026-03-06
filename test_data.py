import yfinance as yf
try:
    import pandas_datareader.data as web
except ImportError:
    import os
    os.system("pip install pandas_datareader")
    import pandas_datareader.data as web
import datetime

start = datetime.datetime(2023, 1, 1)
end = datetime.datetime(2023, 12, 31)

print("Checking VIX...")
vix = yf.download("^VIX", start=start, end=end, progress=False)
print("VIX data points:", len(vix))

print("Checking SPY...")
spy = yf.download("SPY", start=start, end=end, progress=False)
print("SPY data points:", len(spy))

print("Checking DGS10 from FRED...")
try:
    dgs10 = web.DataReader('DGS10', 'fred', start, end)
    print("DGS10 data points:", len(dgs10))
except Exception as e:
    print("Error fetching DGS10:", e)

print("Checking BAMLH0A0HYM2 from FRED...")
try:
    hy = web.DataReader('BAMLH0A0HYM2', 'fred', start, end)
    print("HY data points:", len(hy))
except Exception as e:
    print("Error fetching HY:", e)

print("Checking AAPL Financials...")
aapl = yf.Ticker("AAPL")
try:
    fin = aapl.financials
    bs = aapl.balance_sheet
    cf = aapl.cashflow
    print("Financials available:", fin is not None and not fin.empty)
    print("Balance Sheet available:", bs is not None and not bs.empty)
    print("Cash Flow available:", cf is not None and not cf.empty)
except Exception as e:
    print("Error fetching financials:", e)
