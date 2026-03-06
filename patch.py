with open("project/src/backtest_engine.py", "r") as f:
    code = f.read()

code = code.replace("self.end_date = pd.to_datetime(end_date)", "self.end_date = pd.to_datetime(end_date) if end_date else pd.to_datetime('today')")

with open("project/src/backtest_engine.py", "w") as f:
    f.write(code)
