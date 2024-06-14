import pandas as pd
import yfinance as yf

def get_nifty200_data(stocks):
    for i in stocks['Symbol']:
        df = yf.download(f'{i}.NS', period='1y')
        df.to_csv(f"tickers/data/{i}.csv")

def normalized_mean(df, on='Close'):
    nz = (df[on] - df[on].mean())/df[on].std()
    return nz

def normalized_min_max(df, on='Close'):
    nz = (df[on]-df[on].min())/(df[on].max() - df.min())
    return nz

def get_rs(df, bench):
    df = df.copy()
    bench = bench.copy()
    df['Date'] = pd.to_datetime(df['Date'])
    bench.index = pd.to_datetime(bench.index)
    df['nz'] = normalized_mean(df)
    bench['nz'] = normalized_mean(bench)
    merged_df = pd.merge(df, bench, left_on='Date', right_index=True, suffixes=('_stk', '_bm'))
    merged_df['RS'] = merged_df['nz_stk'] / merged_df['nz_bm']
    merged_df['RSM'] = merged_df['RS'].pct_change(fill_method=None) * 100
    return merged_df['RS'], merged_df['RSM']



