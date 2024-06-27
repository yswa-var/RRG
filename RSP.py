index_array = ['^NSEBANK', 'NIFTY_FIN_SERVICE.NS', '^CNXENERGY', '^CNXFMCG', '^CNXAUTO', '^CNXMETAL', '^CNXIT',
               '^CNXREALTY', '^CNXPHARMA', '^CNXCONSUM', ]

index = '^CNX100'
import yfinance as yf
import pandas as pd

def normalized_mean(df, on='Close'):
    nz = (df[on] - df[on].mean()) / df[on].std()
    return nz


def normalized_min_max(df, on='Close'):
    nz = (df[on] - df[on].min()) / (df[on].max() - df.min())
    return nz


df = yf.download(tickers=index_array, period='1y', group_by='ticker',keepna=True)

df = df.loc[:, (slice(None), 'Close')]
df.columns = df.columns.get_level_values(0)
df = df.dropna(axis=1, how='all')


bench_mark = yf.download(index, period='1y')

