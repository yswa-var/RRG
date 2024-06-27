import pandas as pd
import yfinance as yf
import time

def normalized_mean(df, on='Close'):
    nz = (df[on] - df[on].mean()) / df[on].std()
    return nz


def normalized_min_max(df, on='Close'):
    nz = (df[on] - df[on].min()) / (df[on].max() - df.min())
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
    return merged_df[['RS', 'RSM']][-1:].values


def get_nifty_data(stocks):
    for i in stocks['symbol']:
        df = yf.download(f'{i}.NS', period='1mo')
        df.to_csv(f"tickers/data/{i}.csv")


def process_data(stocks, bentch='^CNX100'):
    bm = yf.download(bentch, period='1mo')
    bm['nz'] = normalized_mean(bm)
    no_data_count = 0
    for i in stocks['symbol']:
        try:
            df = pd.read_csv(f'tickers/data/{i}.csv')
            df['RS'] = 0.000
            df['RSM'] = 0.000
            df['nz'] = normalized_mean(df)
            rs_rm = get_rs(df, bm)
            stocks.loc[stocks['symbol'] == i, 'RS'] = rs_rm[0][0]
            stocks.loc[stocks['symbol'] == i, 'RSM'] = rs_rm[0][1]
            # df.to_csv(f"tickers/data/{i}.csv")
        except:
            no_data_count += 1

    stocks = stocks.dropna(subset=['RS'])
    return stocks
    # stocks.to_csv('rmsm.csv')


if __name__ == '__main__':
    stocks = pd.read_csv('tickers/NSE.csv')
    stocks.rename(columns={
        '20MICRONS': 'symbol',
        '20 MICRONS LTD': 'name',
        'Mining': 'industry',
        'Basic Materials': 'macro'
    }, inplace=True)
    stocks.drop(columns=['Unnamed: 4'], inplace=True)

    # get_nifty_data(stocks)
    stocks = process_data(stocks)

    df = stocks
    avg_rs = df['RS'].mean()
    avg_rsm = df['RSM'].mean()

    industry_groups = df.groupby('industry')
    avg_rs_industry = industry_groups['RS'].mean()
    avg_rsm_industry = industry_groups['RSM'].mean()
    macro_groups = df.groupby('macro')
    avg_rs_macro = macro_groups['RS'].mean()
    avg_rsm_macro = macro_groups['RSM'].mean()

    results_industry = pd.DataFrame({
        'avg_RS': [avg_rs] + list(avg_rs_industry),
        'avg_RSM': [avg_rsm] + list(avg_rsm_industry)
    }, index=['Industry'] + list(avg_rs_industry.index))

    results_macro = pd.DataFrame({
        'avg_RS': [avg_rs] + list(avg_rs_macro),
        'avg_RSM': [avg_rsm] + list(avg_rsm_macro)
    }, index=['Macro'] + list(avg_rs_macro.index))

    writer = pd.ExcelWriter('results.xlsx')

    results_industry.to_excel(writer, sheet_name='Industry')
    results_macro.to_excel(writer, sheet_name='Macro')

    writer.close()