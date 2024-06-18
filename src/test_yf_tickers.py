import yfinance as yf
import pandas as pd
import numpy as np
import os

df_data = pd.DataFrame()
tickers = ['SPY', 'BND', 'GLD', 'QQQ', 'VTI', 'CMI']
for t in tickers:
    data = yf.download(t)
    df_data [t] = data['Adj Close']

print(df_data)
